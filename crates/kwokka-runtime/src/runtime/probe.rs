//! Internal-op submit probe -- the completion-path keystone.
//!
//! [`SubmitProbe`] submits one internal timeout op through the poll frame and
//! resolves with the kernel result when the completion drain delivers it. It
//! proves the submit -> CQE -> wake -> result path end to end against the
//! smallest op that carries no buffer; real file and socket futures build on the
//! same seam.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on the module-private SubmitProbe"
)]
#![allow(
    dead_code,
    reason = "SubmitProbe is the internal-op submit-path proof, exercised end to end under cfg(test)"
)]

use core::{
    future::Future,
    pin::Pin,
    ptr,
    task::{Context, Poll},
};

use kwokka_io::operation::{IoRequest, SubmitResult};

use crate::{
    task::{cell::header::WakeData, reference::waker},
    worker::{WorkerId, poll::polling},
};

/// A future that submits one internal timeout op and resolves with its result.
///
/// Two-state: the first poll submits the op -- decoding the polling task's
/// [`TaskRef`](crate::task::TaskRef) from the waker for the `user_data`
/// round-trip -- and yields `Pending`; a later poll, woken by the completion
/// drain, reads the result the drain cached on the frame at poll entry.
pub(crate) struct SubmitProbe {
    /// Timeout in nanoseconds, submitted on the first poll.
    duration_ns: u64,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl SubmitProbe {
    /// Constructs a timer future for `duration_ns` nanoseconds.
    pub(crate) const fn new(duration_ns: u64) -> Self {
        Self {
            duration_ns,
            is_submitted: false,
        }
    }
}

impl Future for SubmitProbe {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's TaskRef is encoded in its waker; a combinator that
        // wrapped the waker would decode to a garbage ref, so reject it -- the
        // same contract scope() holds.
        if !ptr::eq(
            ptr::from_ref(cx.waker().vtable()),
            ptr::from_ref(&waker::VTABLE),
        ) {
            panic!("SubmitProbe requires the runtime task waker; await it directly");
        }
        let task_ref = waker::data_to_task_ref(cx.waker().data());
        let Ok(worker) = WorkerId::new(task_ref.worker_id()) else {
            panic!("SubmitProbe decoded a non-routable worker id from the waker");
        };
        let this = self.get_mut();
        if this.is_submitted {
            // Woken by the completion drain. `wake_data` stays `EMPTY` until the
            // result is stored, so a spurious early poll stays Pending; a real
            // completion carries a non-empty result (a timeout is -ETIME, never
            // the zeroed EMPTY).
            return match polling::with_current(worker, |frame| frame.wake_data) {
                Some(data) if data != WakeData::EMPTY => Poll::Ready(data.result),
                _ => Poll::Pending,
            };
        }
        let request = IoRequest::<()>::timeout(this.duration_ns).with_user_data(task_ref.raw());
        match polling::with_current(worker, |frame| frame.submit_internal(request)) {
            Some(Some(SubmitResult::Submitted(_))) => {
                this.is_submitted = true;
                Poll::Pending
            }
            // No frame, no driver, or the backend rejected the op. The keystone
            // runs on a real driver, so this is the test-frame / unsupported
            // path; resolve with -EINVAL rather than hang.
            _ => Poll::Ready(-22),
        }
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
// The e2e test needs a real io_uring ring: Miri cannot run the syscalls, and the
// loom build drives loom atomics outside a model. The submit_internal unsafe
// deref is covered by the static SAFETY argument instead.
#[cfg(not(any(miri, loom)))]
mod tests {
    use super::*;
    use crate::runtime::Runtime;

    /// Self-waking timer wrapper: every poll re-queues the task, so the
    /// worker never sees an idle tick and never parks -- the deferred
    /// task-work starvation shape on a `DEFER_TASKRUN` ring.
    struct BusyTimer {
        timer: SubmitProbe,
    }

    impl Future for BusyTimer {
        type Output = i32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
            cx.waker().wake_by_ref();
            Pin::new(&mut self.timer).poll(cx)
        }
    }

    #[test]
    fn a_busy_worker_still_reaps_deferred_completions() {
        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        // The root self-wakes on every poll, so the run loop never parks
        // and the park-side GETEVENTS enter never runs. The timer CQE must
        // still post: the completion drain flushes deferred task work
        // itself, or this test hangs.
        let result = runtime.block_on(BusyTimer {
            timer: SubmitProbe::new(1_000_000),
        });
        assert!(
            result < 0,
            "the timer CQE must post while the worker never parks, got {result}",
        );
    }

    #[test]
    fn timer_future_resolves_with_a_negative_etime() {
        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        let result = runtime.block_on(SubmitProbe::new(1_000));
        // An io_uring timeout completes with -ETIME (errno 62), a negative
        // -errno -- never 0. Proves submit -> CQE -> drain -> wake -> result.
        assert!(
            result < 0,
            "a completed timeout returns a negative -errno, got {result}",
        );
    }

    #[test]
    fn wake_signal_drains_as_the_sentinel_not_a_task() {
        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        // The eventfd counter persists, so a signal sent before the run
        // lands on the armed read during the first drain pass; the run-loop
        // must treat it as the wake sentinel (re-arm and continue), not as
        // a task completion, and the timer must still resolve.
        let Ok(()) = kwokka_io::wake::signal_wake_fd(runtime.wake_fd) else {
            panic!("signaling the runtime wake fd must succeed");
        };
        let result = runtime.block_on(SubmitProbe::new(1_000));
        assert!(
            result < 0,
            "the timer resolves past the wake sentinel, got {result}",
        );
    }
}
