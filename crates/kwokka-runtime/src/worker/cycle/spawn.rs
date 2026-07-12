//! Materializing the tasks a poll asked for: the worker spawn-inbox drain.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::{id::Pip, slab::Slab};

use crate::{
    scheduler::runnable::queue::LocalRunQueue,
    task::{TaskRef, cell::slot::TaskSlot, join::children::push_child},
    worker::{
        WorkerId,
        park::wake::wake_local,
        queue::inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
    },
};

/// Drains the worker spawn inbox: each pending child is stamped with a freshly
/// issued id, inserted into the slab, linked under its parent, and woken onto
/// the run queue.
///
/// Driver-free, like [`tick`](crate::worker::cycle::pass::tick): the blocking
/// run-loop composes it around the
/// tick, after the poll borrow on the inbox has ended. The child cell was built
/// with a detached id at spawn-request time; the issuing worker stamps the real
/// id here from `pip_seq`, the same per-worker counter `WorkerShard::issue_pip`
/// advances, so the issuing worker id stays embedded across a later steal.
pub(crate) fn drain_spawns(
    tasks: &mut Slab<TaskSlot>,
    run_queue: &mut LocalRunQueue,
    spawn_inbox: &mut SpawnInbox<SPAWN_INBOX_CAPACITY>,
    worker_id: WorkerId,
    pip_seq: &mut u64,
) {
    while let Some(mut pending) = spawn_inbox.pop() {
        let pip = Pip::issue(u64::from(worker_id.raw()), *pip_seq);
        *pip_seq += 1;
        pending.cell.header_mut().set_pip(pip);
        let Ok(key) = tasks.insert(pending.cell) else {
            // TODO(pablo): replace this panic with bounded backpressure in 0.2.0 (#116).
            // 0.1.0 treats a full worker task slab as a configuration error and
            // aborts rather than silently dropping the child (a lost-task hazard,
            // not benign backpressure). Unbounded dynamic fan-out is the roadmap, so
            // bounded backpressure -- surfacing to the parent or capping the spawn
            // rate -- is the replacement.
            panic!(
                "worker task slab full: a child spawn exceeded capacity; \
                 increase it via RuntimeBuilder::task_capacity"
            );
        };
        let child = TaskRef::from_slab(worker_id.raw(), key);
        // IGNORE: push_child only fails when the parent or child no longer
        // resolves; both were just established (the parent polled this tick, the
        // child was just inserted), so the link cannot fail.
        let _ = push_child(tasks, pending.parent, child);
        wake_local(tasks, run_queue, child);
    }
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

    use super::drain_spawns;
    use crate::{
        scheduler::runnable::queue::LocalRunQueue,
        task::{
            cell::{header::Slot, lifecycle::spawn_insert, slot::TaskSlot},
            join::children::iter_children,
        },
        worker::{
            WorkerId,
            queue::inbox::{PendingSpawn, SPAWN_INBOX_CAPACITY, SpawnInbox},
        },
    };

    /// Builds a [`WorkerId`] for tests, panicking outside the routable range.
    fn worker(id: u8) -> WorkerId {
        let Ok(worker_id) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker_id
    }

    struct Ready;
    impl Future for Ready {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    #[test]
    fn drain_links_and_wakes_a_seeded_child() {
        let mut tasks = Slab::<TaskSlot>::new(4);
        let mut run_queue = LocalRunQueue::new();
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut pip_seq = 1_u64;

        // A parent task plus a pending child spawn that names it as parent.
        let Ok(parent) = spawn_insert(&mut tasks, 0, Pip::detached(), Namespace::ROOT, Ready)
        else {
            panic!("parent spawn must succeed");
        };
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready).into_erased();
        assert!(
            spawn_inbox.push(PendingSpawn { parent, cell }).is_none(),
            "seed push must enqueue",
        );

        drain_spawns(
            &mut tasks,
            &mut run_queue,
            &mut spawn_inbox,
            worker(0),
            &mut pip_seq,
        );

        assert!(spawn_inbox.is_empty(), "the drain consumes the inbox");
        assert_eq!(run_queue.len(), 1, "the child is woken onto the run queue");
        assert_eq!(
            pip_seq, 2,
            "issuing the child advanced the per-worker counter"
        );

        // The child is linked as the parent's first child and stamped with the
        // issued id, not the detached placeholder it was built with.
        let Some(parent_slot) = tasks.get(SlabKey::new(parent.index(), parent.generation())) else {
            panic!("parent must resolve");
        };
        let Some(child_ref) = parent_slot.header().first_child else {
            panic!("the child must be linked under the parent");
        };
        let Some(child_slot) = tasks.get(SlabKey::new(child_ref.index(), child_ref.generation()))
        else {
            panic!("the child must resolve");
        };
        assert_eq!(
            child_slot.header().pip,
            Pip::issue(0, 1),
            "the child is stamped with the issued id, not the detached placeholder",
        );
    }

    /// Pip issuance and the children tree are two independent axes, asserted
    /// apart here. `drain_spawns` mints a flat, per-worker-unique id for each
    /// child off the shared counter; it does not derive a hierarchical id from
    /// the parent (there is no `Pip::child` on this path). The parent-child
    /// structure lives only in the `first_child` / `next_sibling` list. So pip
    /// is checked for uniqueness, never for relation, and the children list is
    /// checked for structure. There is deliberately no `is_parent_of` claim: a
    /// flat id encodes nothing about its parent.
    #[test]
    fn drain_issues_distinct_child_pips() {
        let mut tasks = Slab::<TaskSlot>::new(8);
        let mut run_queue = LocalRunQueue::new();
        let mut spawn_inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();

        // The parent already carries an issued id; the child counter starts past
        // it, mirroring a live shard that shares one `pip_seq` between the root
        // (issue 1) and the post-poll drain.
        let parent_pip = Pip::issue(0, 1);
        let Ok(parent) = spawn_insert(&mut tasks, 0, parent_pip, Namespace::ROOT, Ready) else {
            panic!("parent spawn must succeed");
        };
        for _ in 0..2 {
            let cell = Slot::new(Pip::detached(), Namespace::ROOT, Ready).into_erased();
            assert!(
                spawn_inbox.push(PendingSpawn { parent, cell }).is_none(),
                "each seed push must enqueue",
            );
        }

        let mut pip_seq = 2_u64;
        drain_spawns(
            &mut tasks,
            &mut run_queue,
            &mut spawn_inbox,
            worker(0),
            &mut pip_seq,
        );

        // Axis 2 -- the children list is the tree. Both children link under the
        // parent, and reaching each through the slab also confirms core's `Slab`
        // backs the runtime's live task headers, not dead storage.
        let (first, second) = {
            let mut children = iter_children(&tasks, parent);
            let Some(first) = children.next() else {
                panic!("the first child must be linked under the parent");
            };
            let Some(second) = children.next() else {
                panic!("the second child must be linked under the parent");
            };
            assert!(
                children.next().is_none(),
                "exactly the two spawned children are linked",
            );
            (first, second)
        };

        // Axis 1 -- pip is unique issuance, not relation. The drain stamps each
        // child with `Pip::issue(worker, seq)` off the shared counter, so the two
        // carry exactly issue(0, 2) and issue(0, 3); the children list, not the
        // ids, sets which is which. The check is positive on the issued ids
        // rather than a `Pip::detached()` comparison: detached is a fresh
        // process-global root on every call over `GLOBAL_SEQ`, not a stable
        // sentinel, and it shares the seq field with `issue`, so a re-read
        // detached is both non-deterministic and prone to a seq collision. There
        // is deliberately no `is_parent_of` claim: a flat id encodes nothing
        // about its parent.
        let Some(first_slot) = tasks.get(SlabKey::new(first.index(), first.generation())) else {
            panic!("the first child must resolve in the slab");
        };
        let Some(second_slot) = tasks.get(SlabKey::new(second.index(), second.generation())) else {
            panic!("the second child must resolve in the slab");
        };
        let first_pip = first_slot.header().pip.as_u128();
        let second_pip = second_slot.header().pip.as_u128();
        assert_ne!(first_pip, second_pip, "each child gets a distinct id");
        let issued_low = Pip::issue(0, 2).as_u128();
        let issued_high = Pip::issue(0, 3).as_u128();
        assert!(
            (first_pip == issued_high && second_pip == issued_low)
                || (first_pip == issued_low && second_pip == issued_high),
            "both children carry the two ids the per-worker counter issued",
        );
        assert_eq!(
            first_slot.header().pip.worker_id(),
            0,
            "the issuing worker is stamped in",
        );
        assert_eq!(
            second_slot.header().pip.worker_id(),
            0,
            "the issuing worker is stamped in",
        );
        assert_eq!(
            pip_seq, 4,
            "two children advanced the per-worker counter by two",
        );
    }
}
