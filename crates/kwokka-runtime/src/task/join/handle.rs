//! Phantom-typed handle to a spawned task.
//!
//! This module locks the public type, layout, and trait bounds. `pip` is an
//! inline field read, `cancel` routes through the owning worker's poll frame,
//! and `Future::poll` resolves the join through that frame. The scope spawn
//! entry links children through the Pip-tree rather than returning handles, so
//! no production caller constructs a handle until the immediate-handle path
//! lands (0.3.0+); these paths are exercised only by tests today.
#![allow(
    clippy::redundant_pub_crate,
    reason = "satisfies the workspace unreachable_pub lint on a private module"
)]
#![allow(
    dead_code,
    reason = "TaskHandle awaits the immediate-handle spawn path; join/cancel exercised by tests until then"
)]

use core::{
    error::Error,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use kwokka_core::id::Pip;

use crate::{
    task::{TaskRef, marker::Mode},
    worker::{WorkerId, poll::polling},
};

/// Owning handle to a spawned task.
///
/// The phantom parameters carry the task's `Output` (so the join future
/// returns `T`) and its scheduler [`Mode`] (so cross-mode misuse is a
/// compile error: `TaskHandle<T, Affine>` is `!Send`, `TaskHandle<T,
/// Stealing>` is `Send + Sync`). The output phantom is `fn() -> T`
/// rather than `T` so the auto-trait propagation does not carry `T`'s
/// own `Send`/`Sync` bounds - a function pointer is `Send + Sync`
/// unconditionally, which matches the handle's behavior: the handle is
/// transferable whenever the mode permits, regardless of `T`.
///
/// The [`Pip`] is held inline so `pip()` is a pure field read; storage
/// indirection is reserved for `cancel` and `Future::poll`, both of
/// which the runtime's worker layer wires later.
#[must_use = "TaskHandle dropped without await detaches the task"]
pub struct TaskHandle<T, M: Mode> {
    task_ref: TaskRef,
    pip: Pip,
    _output: PhantomData<fn() -> T>,
    _mode: PhantomData<M>,
}

impl<T, M: Mode> TaskHandle<T, M> {
    /// Constructs a handle over an existing slab/arena entry.
    ///
    /// Crate-internal: only spawn entry points construct handles.
    pub(crate) const fn new(task_ref: TaskRef, pip: Pip) -> Self {
        Self {
            task_ref,
            pip,
            _output: PhantomData,
            _mode: PhantomData,
        }
    }

    /// Returns the task's [`Pip`] for tracing and observability.
    ///
    /// Pure field read held inline at construction.
    pub const fn pip(&self) -> Pip {
        self.pip
    }

    /// Cancels the task.
    ///
    /// Routes to the owning worker's installed poll frame and flips the task's
    /// atomic state to `Cancelled` through a disjoint slab read. A no-op when
    /// called outside a poll on the owning worker (no frame is installed), when
    /// the task is already terminal, or when its slot has been reclaimed. The
    /// flip is the cancellation signal; reclaiming the slot is driven by the
    /// join handle or the owning scope's exit, not here.
    pub fn cancel(&self) {
        let Ok(worker) = WorkerId::new(self.task_ref.worker_id()) else {
            return;
        };
        polling::with_current(worker, |frame| frame.cancel_child(self.task_ref));
    }

    /// Crate-internal accessor for the underlying slab/arena reference.
    pub(crate) const fn task_ref(&self) -> TaskRef {
        self.task_ref
    }
}

/// Reason a [`TaskHandle`] resolved without a value.
///
/// `#[non_exhaustive]` so future variants can be added without a major
/// version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JoinError {
    /// The task was cancelled before it produced a value.
    Cancelled,
    /// The task panicked or otherwise failed during execution.
    Failed,
    /// The task's output was already consumed by an earlier join.
    AlreadyJoined,
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cancelled => "task was cancelled",
            Self::Failed => "task panicked or failed",
            Self::AlreadyJoined => "task output was already joined",
        })
    }
}

impl Error for JoinError {}

impl<T, M: Mode> Future for TaskHandle<T, M> {
    type Output = Result<T, JoinError>;

    /// Resolves the join by reading the task's slot through the owning worker's
    /// poll frame and branching on its state. `Done` moves the output out and
    /// returns `Ready(Ok(T))`; a cancelled or failed task returns
    /// `Ready(Err(..))`; a non-terminal task returns `Pending`.
    ///
    /// No 0.1.0 production caller awaits a handle (the scope joins children
    /// through the Pip-tree); when the immediate-handle path lands, the re-poll
    /// that observes a later completion will be driven by the owning scope, not a
    /// stored waker -- `cx` is not registered here, so a handle awaited outside a
    /// scope does not self-wake. A handle whose worker has no installed frame
    /// likewise parks.
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(worker) = WorkerId::new(self.task_ref.worker_id()) else {
            return Poll::Pending;
        };
        polling::with_current(worker, |frame| frame.join_child::<T>(self.task_ref))
            .unwrap_or(Poll::Pending)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::task::marker::{Affine, Stealing};

    const _: fn() = || {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TaskHandle<i32, Stealing>>();
        assert_send_sync::<TaskHandle<*const u8, Stealing>>();
    };

    const _: fn() = || {
        const fn assert_future<F: Future<Output = Result<i32, JoinError>>>() {}
        assert_future::<TaskHandle<i32, Stealing>>();
    };

    const fn task_ref_fixture() -> TaskRef {
        TaskRef::from_raw(0)
    }

    #[test]
    fn pip_returns_stored_value() {
        let pip = Pip::detached();
        let handle: TaskHandle<u32, Stealing> = TaskHandle::new(task_ref_fixture(), pip);
        assert_eq!(handle.pip(), pip);
    }

    #[test]
    fn task_ref_round_trips_through_handle() {
        let task_ref = task_ref_fixture();
        let handle: TaskHandle<u32, Stealing> = TaskHandle::new(task_ref, Pip::detached());
        assert_eq!(handle.task_ref().raw(), task_ref.raw());
    }

    #[test]
    fn cancel_outside_a_poll_is_a_noop() {
        // Worker 96 has no installed poll frame (no other test routes to it), so
        // `cancel` resolves `with_current` to `None` and returns without
        // reaching a slab -- a benign no-op rather than a panic.
        let task_ref = TaskRef::from_raw(96 << 56);
        let handle: TaskHandle<(), Stealing> = TaskHandle::new(task_ref, Pip::detached());
        handle.cancel();
    }

    #[test]
    fn poll_without_a_frame_is_pending() {
        use core::task::Waker;
        // Worker 96 has no installed frame, so the join parks instead of
        // reaching a slab.
        let task_ref = TaskRef::from_raw(96 << 56);
        let mut handle: TaskHandle<u32, Stealing> = TaskHandle::new(task_ref, Pip::detached());
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(Pin::new(&mut handle).poll(&mut context), Poll::Pending);
    }

    #[test]
    fn join_error_display_for_cancelled() {
        assert_eq!(JoinError::Cancelled.to_string(), "task was cancelled");
    }

    #[test]
    fn join_error_display_for_failed() {
        assert_eq!(JoinError::Failed.to_string(), "task panicked or failed");
    }

    #[test]
    fn join_error_implements_error_trait() {
        let err = JoinError::Cancelled;
        let as_err: &dyn Error = &err;
        assert!(as_err.source().is_none());
    }

    #[test]
    fn affine_handle_constructs() {
        let _handle: TaskHandle<i32, Affine> = TaskHandle::new(task_ref_fixture(), Pip::detached());
    }
}
