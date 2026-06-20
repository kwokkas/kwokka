//! Post-poll reaping of settled scope children.
//!
//! A [`scope`](crate::task::scope) awaits its children to settlement, but a
//! settled child keeps its slab slot until the worker slab drops. When a scope
//! settles inside a poll it records its parent here through the poll frame; the
//! run-loop drains the queue after the tick -- outside any poll borrow -- and
//! frees each settled child's slot. Reclaiming inside the poll is unsound:
//! [`Slab::remove`](kwokka_core::slab::Slab::remove) needs `&mut Slab`, but a
//! poll reaches the slab through a raw pointer the frame still reads, so the free
//! runs only once that borrow has ended.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::task::{
    TaskRef,
    children::{iter_children, remove_child},
    slot::TaskSlot,
    state::TaskState,
};
use kwokka_core::slab::{Slab, SlabKey};

/// Per-worker capacity of settled-parent records awaiting a reap pass.
///
/// A power of two. A full queue drops the record, leaving that scope's children
/// to the slab drop -- a bounded leak under a reap storm, never a soundness
/// fault.
pub(crate) const REAP_QUEUE_CAPACITY: usize = 64;

/// Fixed-capacity ring of parents whose scope settled this tick.
///
/// Carries [`TaskRef`] (a `Copy` handle), so a full ring drops the record rather
/// than handing it back. `N` must be a power of two. No allocation after
/// construction.
pub(crate) struct ReapQueue<const N: usize> {
    slots: [Option<TaskRef>; N],
    head: usize,
    tail: usize,
}

impl<const N: usize> ReapQueue<N> {
    /// Creates an empty queue.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is not a power of two or is zero.
    pub(crate) const fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            tail: 0,
        }
    }

    /// Records `parent` for the next reap pass.
    ///
    /// Returns `false` when the ring is full -- the record is dropped and that
    /// scope's settled children wait for the slab drop instead.
    pub(crate) const fn push(&mut self, parent: TaskRef) -> bool {
        if self.tail.wrapping_sub(self.head) >= N {
            return false;
        }
        self.slots[self.tail & (N - 1)] = Some(parent);
        self.tail = self.tail.wrapping_add(1);
        true
    }

    /// Pops the oldest recorded parent, or `None` when the ring is empty.
    pub(crate) const fn pop(&mut self) -> Option<TaskRef> {
        if self.head == self.tail {
            return None;
        }
        let parent = self.slots[self.head & (N - 1)].take();
        self.head = self.head.wrapping_add(1);
        parent
    }
}

/// Reclaims the settled children of every parent recorded since the last pass.
///
/// Drains the queue; for each parent, frees every child that has settled
/// (terminal or [`Done`](TaskState::Done)) and unlinks it from the parent's
/// children list. Unlinking precedes the free: freeing rolls the slot
/// generation, so a sibling link must be repaired first or a later walk chases a
/// stale reference.
pub(crate) fn reap_settled(tasks: &mut Slab<TaskSlot>, reap: &mut ReapQueue<REAP_QUEUE_CAPACITY>) {
    while let Some(parent) = reap.pop() {
        reap_children(tasks, parent);
    }
}

/// Frees `parent`'s settled children one at a time until none remain.
///
/// Re-walks from the list head each step because [`remove_child`] mutates the
/// list and would invalidate a held iterator. Children lists are short, so the
/// repeated walk is cheap. A non-settled child is left linked.
fn reap_children(tasks: &mut Slab<TaskSlot>, parent: TaskRef) {
    while let Some(child) = first_settled_child(tasks, parent) {
        // IGNORE: remove_child fails only if parent/child are gone; both were just located.
        let _ = remove_child(tasks, parent, child);
        let key = SlabKey::new(child.index(), child.generation());
        if is_husk(tasks, key) {
            // The body moved to another slab and settles under its new
            // owner; release the bookkeeping without running drop glue.
            tasks.retire_slot(key);
        } else {
            tasks.remove(key);
        }
    }
}

/// Whether `key` resolves to a `Retired` husk left behind by a relocation.
fn is_husk(tasks: &Slab<TaskSlot>, key: SlabKey) -> bool {
    tasks
        .get(key)
        .is_some_and(|slot| slot.header().state.load() == TaskState::Retired)
}

/// The first settled child in `parent`'s list, or `None` when none has settled.
fn first_settled_child(tasks: &Slab<TaskSlot>, parent: TaskRef) -> Option<TaskRef> {
    iter_children(tasks, parent).find(|&child| is_settled(tasks, child))
}

/// Whether `child` has reached a terminal state or [`Done`](TaskState::Done).
///
/// A `Retired` husk settles only once its settled note landed
/// (`is_remote_settled`): the body moved to another slab, and until the
/// relocated task settles there, the husk must stay linked -- it is what
/// keeps the sibling chain walkable. Its release then runs through
/// `retire_slot`, never `remove`, so no drop glue touches bytes the new
/// owner dropped.
fn is_settled(tasks: &Slab<TaskSlot>, child: TaskRef) -> bool {
    let key = SlabKey::new(child.index(), child.generation());
    tasks.get(key).is_some_and(|slot| {
        let header = slot.header();
        let state = header.state.load();
        if state == TaskState::Retired {
            return header.is_remote_settled;
        }
        state.is_terminal() || state == TaskState::Done
    })
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use kwokka_core::{id::Pip, namespace::Namespace};

    use super::*;
    use crate::task::{children::push_child, header::Slot, state::TaskState};

    /// A child future that never completes; tests drive its state directly.
    struct Inert;
    impl Future for Inert {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    fn insert_task(slab: &mut Slab<TaskSlot>, worker: u8) -> TaskRef {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Inert).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        TaskRef::from_slab(worker, key)
    }

    fn drive_to_done(slab: &Slab<TaskSlot>, task: TaskRef) {
        let key = SlabKey::new(task.index(), task.generation());
        let Some(slot) = slab.get(key) else {
            panic!("task must resolve");
        };
        let state = &slot.header().state;
        let Ok(()) = state.transition(TaskState::Sleeping, TaskState::Running) else {
            panic!("Sleeping -> Running must succeed");
        };
        let Ok(()) = state.complete() else {
            panic!("Running -> Done must succeed");
        };
    }

    fn force_cancel(slab: &Slab<TaskSlot>, task: TaskRef) {
        let key = SlabKey::new(task.index(), task.generation());
        let Some(slot) = slab.get(key) else {
            panic!("task must resolve");
        };
        assert!(slot.header().state.cancel(), "a sleeping task must cancel");
    }

    fn first_child(slab: &Slab<TaskSlot>, parent: TaskRef) -> Option<TaskRef> {
        let key = SlabKey::new(parent.index(), parent.generation());
        slab.get(key)
            .unwrap_or_else(|| panic!("parent must resolve"))
            .header()
            .first_child
    }

    fn is_live(slab: &Slab<TaskSlot>, task: TaskRef) -> bool {
        slab.get(SlabKey::new(task.index(), task.generation()))
            .is_some()
    }

    #[test]
    fn a_remote_settled_husk_is_released_without_drop() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        struct Bomb<'a>(&'a AtomicUsize);
        impl Future for Bomb<'_> {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for Bomb<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = AtomicUsize::new(0);
        let mut slab = Slab::<TaskSlot>::new(2);
        let parent = insert_task(&mut slab, 0);
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Bomb(&drops)).into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert into a fresh slab must succeed");
        };
        let child = TaskRef::from_slab(0, key);
        let Ok(()) = push_child(&mut slab, parent, child) else {
            panic!("linking a child must succeed");
        };
        {
            let Some(slot) = slab.get(key) else {
                panic!("child must resolve");
            };
            let Ok(()) = slot.header().state.try_retire() else {
                panic!("Sleeping -> Retired must succeed");
            };
        }
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);
        assert!(
            is_live(&slab, child),
            "an unmarked husk stays linked: its task is alive elsewhere",
        );
        let Some(slot) = slab.get_mut(key) else {
            panic!("husk must resolve");
        };
        slot.header_mut().is_remote_settled = true;
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);
        assert!(
            first_child(&slab, parent).is_none(),
            "the marked husk is unlinked",
        );
        assert!(!is_live(&slab, child), "the husk slot is released");
        assert_eq!(
            drops.load(Ordering::Relaxed),
            0,
            "the release must run no drop glue; the new owner dropped the bytes",
        );
    }

    #[test]
    fn an_unmarked_husk_is_not_reaped() {
        let mut slab = Slab::<TaskSlot>::new(2);
        let parent = insert_task(&mut slab, 0);
        let child = insert_task(&mut slab, 0);
        let Ok(()) = push_child(&mut slab, parent, child) else {
            panic!("linking a child must succeed");
        };
        {
            let Some(slot) = slab.get(SlabKey::new(child.index(), child.generation())) else {
                panic!("child must resolve");
            };
            let Ok(()) = slot.header().state.try_retire() else {
                panic!("Sleeping -> Retired must succeed");
            };
        }
        let mut queue = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(queue.push(parent));
        reap_settled(&mut slab, &mut queue);
        assert!(
            is_live(&slab, child),
            "a retired husk is the steal protocol's to release, not the reaper's",
        );
        assert_eq!(first_child(&slab, parent), Some(child));
    }

    #[test]
    fn queue_push_pop_is_fifo() {
        let mut queue = ReapQueue::<4>::new();
        assert!(queue.push(TaskRef::from_raw(1)));
        assert!(queue.push(TaskRef::from_raw(2)));
        let Some(first) = queue.pop() else {
            panic!("pop must yield the first record");
        };
        assert_eq!(first.raw(), TaskRef::from_raw(1).raw());
        let Some(second) = queue.pop() else {
            panic!("pop must yield the second record");
        };
        assert_eq!(second.raw(), TaskRef::from_raw(2).raw());
        assert!(queue.pop().is_none());
    }

    #[test]
    fn queue_full_drops_record() {
        let mut queue = ReapQueue::<2>::new();
        assert!(queue.push(TaskRef::from_raw(1)));
        assert!(queue.push(TaskRef::from_raw(2)));
        assert!(
            !queue.push(TaskRef::from_raw(3)),
            "a full queue rejects the record",
        );
    }

    #[test]
    fn reap_frees_a_done_child() {
        let mut slab = Slab::<TaskSlot>::new(4);
        let parent = insert_task(&mut slab, 0);
        let child = insert_task(&mut slab, 0);
        let Ok(()) = push_child(&mut slab, parent, child) else {
            panic!("link must succeed");
        };
        drive_to_done(&slab, child);
        let before = slab.len();

        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);

        assert_eq!(slab.len(), before - 1, "the done child slot was freed");
        assert!(!is_live(&slab, child), "the child slot is reclaimed");
        assert!(
            first_child(&slab, parent).is_none(),
            "the child is unlinked from the parent",
        );
    }

    #[test]
    fn reap_frees_a_cancelled_child() {
        let mut slab = Slab::<TaskSlot>::new(4);
        let parent = insert_task(&mut slab, 0);
        let child = insert_task(&mut slab, 0);
        let Ok(()) = push_child(&mut slab, parent, child) else {
            panic!("link must succeed");
        };
        force_cancel(&slab, child);

        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);

        assert!(!is_live(&slab, child), "the cancelled child is reclaimed");
    }

    #[test]
    fn reap_keeps_a_sleeping_child() {
        let mut slab = Slab::<TaskSlot>::new(4);
        let parent = insert_task(&mut slab, 0);
        let child = insert_task(&mut slab, 0); // stays Sleeping
        let Ok(()) = push_child(&mut slab, parent, child) else {
            panic!("link must succeed");
        };

        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);

        assert!(is_live(&slab, child), "a sleeping child is kept");
        assert_eq!(
            first_child(&slab, parent),
            Some(child),
            "the sleeping child stays linked",
        );
    }

    #[test]
    fn reap_handles_a_mixed_list() {
        let mut slab = Slab::<TaskSlot>::new(8);
        let parent = insert_task(&mut slab, 0);
        let done = insert_task(&mut slab, 0);
        let sleeping = insert_task(&mut slab, 0);
        let cancelled = insert_task(&mut slab, 0);
        for child in [done, sleeping, cancelled] {
            let Ok(()) = push_child(&mut slab, parent, child) else {
                panic!("link must succeed");
            };
        }
        drive_to_done(&slab, done);
        force_cancel(&slab, cancelled);

        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        assert!(reap.push(parent));
        reap_settled(&mut slab, &mut reap);

        assert!(!is_live(&slab, done), "the done child is freed");
        assert!(!is_live(&slab, cancelled), "the cancelled child is freed");
        assert!(is_live(&slab, sleeping), "the sleeping child survives");
        assert_eq!(
            first_child(&slab, parent),
            Some(sleeping),
            "only the sleeping child remains linked",
        );
    }
}
