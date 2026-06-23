//! Task spawn-insert and poll dispatch over the per-worker slab.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{
    future::Future,
    task::{Context, Poll},
};

use kwokka_core::{
    id::Pip,
    namespace::Namespace,
    slab::{Slab, SlabError},
};

use crate::task::{TaskRef, header::Slot, slot::TaskSlot, state::TaskState};

/// Result of a single [`poll_task`] attempt.
///
/// A real enum, not a unit-error result: the worker loop branches on the
/// outcome to decide whether to re-enqueue, drop, or move on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    /// The future returned `Ready`; state advanced `Running -> Done`. The
    /// output sits in the cell awaiting a join handle.
    Completed,
    /// The future returned `Pending`; state advanced `Running -> Sleeping`.
    Suspended,
    /// Reserved: a wake landing mid-poll would surface here (`suspend`
    /// observes `Woken`), but the current state machine has no
    /// `Running -> Woken` path, so this is not produced yet.
    Rescheduled,
    /// `try_start_poll` failed: the task was not `Woken` (already running
    /// elsewhere, terminal, or still sleeping). No poll happened.
    Skipped(TaskState),
}

/// Builds a task for `future` and inserts it into the per-worker slab.
///
/// The `pip` is supplied by the caller; minting it (the worker counter and
/// issuing policy) is wired separately. The returned [`TaskRef`] is a
/// slab-path handle routed to `worker_id`. The freshly inserted task is
/// `Sleeping` and is not enqueued -- initial schedulability is the caller's
/// decision.
///
/// # Errors
///
/// Returns [`SlabError::Full`] when the worker's slab is at capacity; the
/// `future` is dropped on that path.
pub(crate) fn spawn_insert<F>(
    tasks: &mut Slab<TaskSlot>,
    worker_id: u8,
    pip: Pip,
    namespace: Namespace,
    future: F,
) -> Result<TaskRef, SlabError>
where
    F: Future,
{
    let cell = Slot::new(pip, namespace, future).into_erased();
    let key = tasks.insert(cell)?;
    Ok(TaskRef::from_slab(worker_id, key))
}

/// Polls the task in `slot` once, driving the lifecycle state machine.
///
/// Transitions `Woken -> Running` before the poll, then `Running -> Done` on
/// `Ready` or `Running -> Sleeping` on `Pending`. [`PollOutcome::Rescheduled`]
/// is reserved for a wake landing mid-poll (`suspend` observing `Woken`); the
/// current state machine has no `Running -> Woken` path, so it is unreachable
/// until that protocol is wired.
pub(crate) fn poll_task(slot: &mut TaskSlot, cx: &mut Context<'_>) -> PollOutcome {
    if let Err(observed) = slot.header().state.try_start_poll() {
        return PollOutcome::Skipped(observed);
    }
    // The runnable steal path offers only polled tasks, so record poll entry
    // here: a task becomes stealable as a runnable only after its owning
    // worker has run it at least once, which keeps a task's first poll on the
    // worker that spawned it.
    slot.header_mut().has_polled = true;
    match slot.poll_via_vtable(cx) {
        Poll::Ready(()) => {
            // The future is consumed and its output written; advance
            // Running -> Done so a join handle can take it. `complete()` only
            // fails if a terminal transition won first: `cancel()`
            // (Running -> Cancelled) exists today, but nothing drives it
            // concurrently with a poll yet. When the cancellation lifecycle
            // lands it must reconcile this window -- a Cancelled slot left
            // after a Ready poll has its output written and its future
            // already dropped, so a later vtable drop would double-drop the
            // future. The poll produced a value, so report Completed either way.
            match slot.header().state.complete() {
                Ok(()) | Err(_) => PollOutcome::Completed,
            }
        }
        Poll::Pending => match slot.header().state.suspend() {
            Err(TaskState::Woken) => PollOutcome::Rescheduled,
            // `Ok` means the suspend took; any other `Err` needs a concurrent
            // terminal transition (cancellation), unwired in this change.
            // Both leave the task off the run queue, so treat them alike.
            Ok(()) | Err(_) => PollOutcome::Suspended,
        },
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };

    use kwokka_core::{
        id::Pip,
        namespace::Namespace,
        slab::{Slab, SlabError, SlabKey},
    };

    use super::{PollOutcome, poll_task, spawn_insert};
    use crate::task::{slot::TaskSlot, state::TaskState, waker::waker_from_task_ref};

    /// Future that records its own drop, optionally completing immediately.
    struct CountingFuture {
        ready: bool,
        drops: &'static AtomicUsize,
    }

    impl Future for CountingFuture {
        type Output = u32;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
            if self.ready {
                Poll::Ready(7)
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for CountingFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn spawn_insert_returns_resolvable_slab_ref() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(2);
        let Ok(task_ref) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("spawn into a fresh slab must succeed");
        };
        assert!(!task_ref.is_arena());
        assert_eq!(task_ref.worker_id(), 0);
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        assert!(tasks.get(key).is_some());
    }

    #[test]
    fn poll_pending_returns_suspended() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let Ok(task_ref) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get_mut(key) else {
            panic!("just-spawned key must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let waker = waker_from_task_ref(task_ref);
        let mut cx = Context::from_waker(&waker);
        assert_eq!(poll_task(slot, &mut cx), PollOutcome::Suspended);
        assert_eq!(slot.header().state.load(), TaskState::Sleeping);
    }

    #[test]
    fn poll_ready_completes_and_drops_future() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let Ok(task_ref) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: true,
                drops: &DROPS,
            },
        ) else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get_mut(key) else {
            panic!("just-spawned key must resolve");
        };
        let Ok(()) = slot.header().state.wake() else {
            panic!("Sleeping -> Woken must succeed");
        };
        let waker = waker_from_task_ref(task_ref);
        let mut cx = Context::from_waker(&waker);
        assert_eq!(poll_task(slot, &mut cx), PollOutcome::Completed);
        assert_eq!(slot.header().state.load(), TaskState::Done);
        assert_eq!(
            DROPS.load(Ordering::Relaxed),
            1,
            "the future is dropped in place when poll returns Ready",
        );
    }

    #[test]
    fn poll_without_wake_is_skipped() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let Ok(task_ref) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: true,
                drops: &DROPS,
            },
        ) else {
            panic!("spawn must succeed");
        };
        let key = SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get_mut(key) else {
            panic!("just-spawned key must resolve");
        };
        let waker = waker_from_task_ref(task_ref);
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            poll_task(slot, &mut cx),
            PollOutcome::Skipped(TaskState::Sleeping),
            "a freshly spawned task is Sleeping, so try_start_poll fails",
        );
    }

    #[test]
    fn spawn_insert_into_full_slab_errors() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let Ok(_first) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("first spawn must succeed");
        };
        let Err(SlabError::Full) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("second spawn must report Full");
        };
    }

    #[test]
    fn stale_ref_after_remove_does_not_resolve() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut tasks = Slab::<TaskSlot>::new(1);
        let Ok(first) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("spawn must succeed");
        };
        let old_key = SlabKey::new(first.index(), first.generation());
        assert!(tasks.remove(old_key).is_some());
        let Ok(second) = spawn_insert(
            &mut tasks,
            0,
            Pip::detached(),
            Namespace::ROOT,
            CountingFuture {
                ready: false,
                drops: &DROPS,
            },
        ) else {
            panic!("respawn must succeed");
        };
        assert_eq!(first.index(), second.index());
        assert!(tasks.get(old_key).is_none());
    }

    #[test]
    fn slab_drop_releases_occupied_futures() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        {
            let mut tasks = Slab::<TaskSlot>::new(2);
            let Ok(_) = spawn_insert(
                &mut tasks,
                0,
                Pip::detached(),
                Namespace::ROOT,
                CountingFuture {
                    ready: false,
                    drops: &DROPS,
                },
            ) else {
                panic!("spawn must succeed");
            };
            let Ok(_) = spawn_insert(
                &mut tasks,
                0,
                Pip::detached(),
                Namespace::ROOT,
                CountingFuture {
                    ready: false,
                    drops: &DROPS,
                },
            ) else {
                panic!("spawn must succeed");
            };
        }
        assert_eq!(
            DROPS.load(Ordering::Relaxed),
            2,
            "dropping the slab drops every occupied task's future via the vtable",
        );
    }
}
