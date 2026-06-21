//! Accepting one connection from a listener's backlog.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kwokka_io::{
    boundary::{self, IoSeam},
    operation::{IoRequest, SubmitResult},
};

/// A future that accepts one connection on listening socket `fd`.
///
/// The first poll submits an accept op through the io seam -- addressed by
/// the polling task's identity token for the `user_data` round trip -- and
/// yields `Pending`. A later poll, woken by the completion drain, returns
/// the kernel result: the accepted connection's file descriptor, or a
/// negative `-errno`. The op carries no buffer and no socket address, so
/// the future owns no storage the kernel could dangle on -- the caller
/// resolves the peer address later if needed.
///
/// The returned descriptor is owned by the caller, who is responsible for
/// closing it.
///
/// The `io_uring` accept op completes in the kernel regardless of the
/// listener fd's blocking mode. The readiness-based epoll / kqueue
/// fallback requires the fd switched to non-blocking before submission;
/// that switch lands with the fallback driver, per the listener's
/// blocking-mode contract.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the
/// `user_data` round trip decodes the polling task from the waker, so
/// await it directly.
#[must_use = "futures do nothing unless polled"]
pub struct AcceptFuture {
    /// Listening socket file descriptor.
    fd: i32,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl AcceptFuture {
    /// Constructs an accept future for listening socket `fd`.
    pub const fn new(fd: i32) -> Self {
        Self {
            fd,
            is_submitted: false,
        }
    }
}

impl Future for AcceptFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's identity is encoded in its waker; the seam
        // decoder rejects a waker the runtime did not build, the same
        // contract the buffered futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("AcceptFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => Poll::Ready(slot.result),
                _ => Poll::Pending,
            };
        }
        let request = IoRequest::<()>::accept(this.fd).with_user_data(binding.token);
        match IoSeam::with_current(binding.worker_id, |seam| seam.submit_internal(request)) {
            Some(Some(SubmitResult::Submitted(_))) => {
                this.is_submitted = true;
                Poll::Pending
            }
            // No seam, no driver, or the backend rejected the op. The
            // production path runs on a real driver, so this is the
            // test-seam / unsupported path; resolve with -EINVAL rather
            // than hang.
            _ => Poll::Ready(-22),
        }
    }
}
