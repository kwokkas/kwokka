//! Connecting a socket to a peer address.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kwokka_io::{
    addr::SockAddr,
    boundary::{self, IoSeam},
    operation::{IoRequest, SubmitResult},
};

/// A future that connects socket `fd` to a peer address.
///
/// The connect counterpart of [`AcceptFuture`](super::AcceptFuture): the
/// first poll moves the address into a connect op submitted through the io
/// seam -- addressed by the polling task's identity token for the
/// `user_data` round trip -- and yields `Pending`. A later poll, woken by
/// the completion drain, returns the kernel result: `0` on success, or a
/// negative `-errno`. The address is moved out on submit, so the future
/// owns no storage the kernel could dangle on.
///
/// At most one connect may be in flight per worker. The driver packs the
/// address into its single submission scratch buffer, so a second connect
/// submitted while one is in flight overwrites the first address in place.
/// This 0.1.0 limit lifts when per-op address storage lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the
/// `user_data` round trip decodes the polling task from the waker, so
/// await it directly.
#[must_use = "futures do nothing unless polled"]
pub struct ConnectFuture {
    /// Socket file descriptor to connect.
    fd: i32,
    /// Peer address; taken on submit, so `None` marks the submitted state.
    addr: Option<SockAddr>,
}

impl ConnectFuture {
    /// Constructs a connect future for socket `fd` toward `addr`.
    pub const fn new(fd: i32, addr: SockAddr) -> Self {
        Self {
            fd,
            addr: Some(addr),
        }
    }
}

impl Future for ConnectFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's identity is encoded in its waker; the seam
        // decoder rejects a waker the runtime did not build, the same
        // contract the accept future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("ConnectFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        // The address doubles as the submit-once gate: taking it marks the
        // op submitted, so later polls fall through to the result read.
        let Some(addr) = this.addr.take() else {
            return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => Poll::Ready(slot.result),
                _ => Poll::Pending,
            };
        };
        let request = IoRequest::<()>::connect(this.fd, addr).with_user_data(binding.token);
        match IoSeam::with_current(binding.worker_id, |seam| seam.submit_internal(request)) {
            Some(Some(SubmitResult::Submitted(_))) => Poll::Pending,
            // No seam, no driver, or the backend rejected the op. The
            // production path runs on a real driver, so this is the
            // test-seam / unsupported path; resolve with -EINVAL rather
            // than hang.
            _ => Poll::Ready(-22),
        }
    }
}
