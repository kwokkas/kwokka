//! Worker-local wake: move a task onto its worker's run queue.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::slab::{Slab, SlabKey};
#[cfg(feature = "steal")]
use kwokka_io::DriverType;

use crate::{
    scheduler::runnable::queue::LocalRunQueue,
    task::{TaskRef, cell::slot::TaskSlot},
};
#[cfg(feature = "steal")]
use crate::{
    scheduler::stealing::relocate::ForwardTable, task::cell::state::TaskState, worker::registry,
};

/// Wakes `task_ref`: transitions it to `Woken` and, on success, pushes it
/// onto the local run queue.
///
/// The single wake entry the timer, completion drain, and `IndexWaker`
/// paths funnel through. A failed transition -- the task is no longer
/// `Sleeping` (already woken, running, or terminal) -- is a no-op, so a
/// duplicate wake never double-enqueues. A stale `task_ref` whose slot was
/// recycled resolves to nothing and is dropped.
pub(crate) fn wake_local(
    tasks: &mut Slab<TaskSlot>,
    run_queue: &mut LocalRunQueue,
    task_ref: TaskRef,
) {
    let key = SlabKey::new(task_ref.index(), task_ref.generation());
    let woke = match tasks.get(key) {
        Some(slot) => slot.header().state.wake().is_ok(),
        None => return,
    };
    if woke {
        run_queue.push(task_ref, tasks);
    }
}

/// Wakes `task_ref` like [`wake_local`], re-routing through the forward
/// table when the slot turns out relocated.
///
/// Three shapes. A live slot wakes locally. A `Retired` husk re-routes to
/// the recorded new home -- the serve step's contract is to record the
/// route at ship time, before the husk is observable, so a husk always
/// has one; a missing route means the slot was reused and relocated
/// again, and the stale key's wake drops, the documented
/// generation-collision degradation. A key that no longer resolves
/// re-routes if the table still carries its route, and otherwise drops
/// like the [`wake_local`] stale no-op.
#[cfg(feature = "steal")]
pub(crate) fn wake_or_forward(
    tasks: &mut Slab<TaskSlot>,
    run_queue: &mut LocalRunQueue,
    forward: &ForwardTable,
    source: Option<&DriverType>,
    task_ref: TaskRef,
) {
    let key = SlabKey::new(task_ref.index(), task_ref.generation());
    let is_relocated = tasks
        .get(key)
        .is_none_or(|slot| slot.header().state.load() == TaskState::Retired);
    if !is_relocated {
        wake_local(tasks, run_queue, task_ref);
        return;
    }
    let Some(new_ref) = forward.lookup(key) else {
        return;
    };
    if registry::enqueue(new_ref).is_err() {
        // A full target inbox drops the wake, the same policy as the
        // direct waker path.
        return;
    }
    registry::signal(source, new_ref.worker_id());
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use kwokka_core::{
        id::{Namespace, Pip},
        slab::{Slab, SlabKey},
    };

    use super::wake_local;
    use crate::{
        scheduler::runnable::queue::LocalRunQueue,
        task::cell::{lifecycle::spawn_insert, slot::TaskSlot, state::TaskState},
    };

    struct Pending;
    impl Future for Pending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    #[test]
    fn wake_enqueues_sleeping_task_and_marks_woken() {
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(task_ref) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Pending)
        else {
            panic!("spawn into a fresh slab must succeed");
        };

        wake_local(&mut tasks, &mut run_queue, task_ref);

        assert_eq!(run_queue.len(), 1);
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get(key) else {
            panic!("task must resolve");
        };
        assert_eq!(slot.header().state.load(), TaskState::Woken);
    }

    #[test]
    fn duplicate_wake_does_not_double_enqueue() {
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(task_ref) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Pending)
        else {
            panic!("spawn must succeed");
        };

        wake_local(&mut tasks, &mut run_queue, task_ref);
        wake_local(&mut tasks, &mut run_queue, task_ref);

        assert_eq!(
            run_queue.len(),
            1,
            "the second wake observes Woken and must not re-enqueue",
        );
    }

    #[cfg(feature = "steal")]
    #[test]
    fn a_husk_wake_re_routes_to_the_new_worker() {
        use kwokka_core::Generation;

        use crate::{
            scheduler::stealing::relocate::ForwardTable,
            task::TaskRef,
            worker::{WorkerId, registry},
        };
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(stale) = spawn_insert(&mut tasks, 17, Pip::detached(), Namespace::ROOT, Pending)
        else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(stale.index(), stale.generation());
        let Some(slot) = tasks.get(key) else {
            panic!("task must resolve");
        };
        let Ok(()) = slot.header().state.try_retire() else {
            panic!("Sleeping -> Retired must succeed");
        };
        let mut forward = ForwardTable::new(1);
        let new_home = TaskRef::from_slab(18, SlabKey::new(0, Generation::from_raw(1)));
        forward.record(key, new_home);
        super::wake_or_forward(&mut tasks, &mut run_queue, &forward, None, stale);
        assert!(run_queue.is_empty(), "a husk never enters the local queue");
        let Ok(target) = WorkerId::new(18) else {
            panic!("worker id within range");
        };
        assert_eq!(
            registry::pop(target),
            Some(new_home),
            "the wake lands in the new worker's inbox",
        );
    }

    #[cfg(feature = "steal")]
    #[test]
    fn a_husk_wake_without_route_drops() {
        use crate::scheduler::stealing::relocate::ForwardTable;
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(stale) = spawn_insert(&mut tasks, 19, Pip::detached(), Namespace::ROOT, Pending)
        else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(stale.index(), stale.generation());
        let Some(slot) = tasks.get(key) else {
            panic!("task must resolve");
        };
        let Ok(()) = slot.header().state.try_retire() else {
            panic!("Sleeping -> Retired must succeed");
        };
        let forward = ForwardTable::new(1);
        super::wake_or_forward(&mut tasks, &mut run_queue, &forward, None, stale);
        assert!(
            run_queue.is_empty(),
            "a reused-slot collision drops rather than misroutes",
        );
    }

    #[test]
    fn wake_of_stale_ref_is_a_noop() {
        let mut tasks = Slab::<TaskSlot>::new(1);
        let mut run_queue = LocalRunQueue::new();
        let Ok(task_ref) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Pending)
        else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        assert!(tasks.remove(key).is_some());

        wake_local(&mut tasks, &mut run_queue, task_ref);

        assert!(run_queue.is_empty());
    }
}
