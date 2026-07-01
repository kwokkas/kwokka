//! Accepting one connection from a listener's backlog.

use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use kwokka_io::{
    boundary::{self, IoSeam, MultishotNext, WakerBinding},
    buffer::multishot::MultishotSlotKey,
    operation::{IoRequest, SubmitResult},
};

use crate::tcp::TcpStream;

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

/// A stream of accepted connections from one multishot accept.
///
/// [`TcpListener::accept_multi`](crate::tcp::TcpListener::accept_multi) returns
/// this. On a kernel with multishot accept, one submitted SQE posts a completion
/// per incoming connection, which the stream drains from the worker's registry;
/// on a backend without it, the stream degrades to one single-shot accept per
/// item. Either way `next().await` yields `Some(Ok(conn))` for a connection,
/// `Some(Err(_))` for a per-accept error, or `None` once the op ends. Dropping
/// the stream cancels the in-flight op.
#[must_use = "streams do nothing unless polled"]
pub struct AcceptStream<'listener> {
    /// Listening socket file descriptor.
    fd: i32,
    /// Where the stream is in its lifecycle.
    state: AcceptState,
    /// Binds the stream to the listener's lifetime so it cannot outlive the fd
    /// and observe a closed or reused descriptor.
    listener: PhantomData<&'listener ()>,
}

/// The accept stream's progress.
enum AcceptState {
    /// Nothing submitted yet.
    Idle,
    /// A multishot op is in flight; its key drains completions.
    Multishot(MultishotSlotKey),
    /// The backend lacks multishot; a fresh single-shot accept drives each item.
    Fallback(AcceptFuture),
    /// The stream ended.
    Done,
}

impl<'listener> AcceptStream<'listener> {
    /// Builds an accept stream for listening socket `fd`.
    pub(crate) const fn new(fd: i32) -> Self {
        Self {
            fd,
            state: AcceptState::Idle,
            listener: PhantomData,
        }
    }

    /// Awaits the next accepted connection, or `None` once the stream ends.
    ///
    /// Written for the ordinary `while let Some(conn) = stream.next().await`
    /// loop.
    pub const fn next(&mut self) -> AcceptNext<'_, 'listener> {
        AcceptNext { stream: self }
    }
}

impl Drop for AcceptStream<'_> {
    fn drop(&mut self) {
        if let AcceptState::Multishot(key) = &self.state {
            // A live multishot op is `io_bound`, so this drop runs on the owning
            // worker; the cancel reaches the inbox single-writer.
            boundary::push_multishot_cancel_for_worker(*key);
        }
    }
}

/// The future returned by [`AcceptStream::next`].
#[must_use = "futures do nothing unless polled"]
pub struct AcceptNext<'stream, 'listener> {
    stream: &'stream mut AcceptStream<'listener>,
}

impl Future for AcceptNext<'_, '_> {
    type Output = Option<io::Result<TcpStream>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("AcceptStream requires the runtime task waker; await it directly");
        };
        let stream = &mut *self.get_mut().stream;
        if matches!(stream.state, AcceptState::Idle) {
            stream.state = start_multishot(stream.fd, binding);
        }
        let multishot_key = match &stream.state {
            AcceptState::Multishot(key) => Some(*key),
            _ => None,
        };
        if let Some(key) = multishot_key {
            return match multishot_next(binding, key) {
                MultishotNext::Item(result) => Poll::Ready(Some(adopt(result))),
                MultishotNext::Pending => Poll::Pending,
                MultishotNext::Ended => {
                    stream.state = AcceptState::Done;
                    Poll::Ready(None)
                }
            };
        }
        if matches!(stream.state, AcceptState::Fallback(_)) {
            return poll_fallback(stream, cx);
        }
        // `Idle` is left non-`Idle` by `start_multishot`; `Done` yields `None`.
        Poll::Ready(None)
    }
}

/// Allocates a slot and submits the multishot accept, or picks a fallback.
///
/// Returns [`AcceptState::Done`] when no multishot registry is reachable (a test
/// seam or a full registry), [`AcceptState::Fallback`] when the backend rejects
/// the multishot op, or [`AcceptState::Multishot`] once the op is in flight.
fn start_multishot(fd: i32, binding: WakerBinding) -> AcceptState {
    let allocated = IoSeam::with_current(binding.worker_id, |seam| {
        seam.allocate_multishot_slot(binding.token)
    });
    let Some(Some((key, sentinel))) = allocated else {
        return AcceptState::Done;
    };
    let request = IoRequest::<()>::accept_multishot(fd).with_user_data(sentinel);
    let submitted = IoSeam::with_current(binding.worker_id, |seam| seam.submit_internal(request));
    if let Some(Some(SubmitResult::Submitted(_))) = submitted {
        return AcceptState::Multishot(key);
    }
    IoSeam::with_current(binding.worker_id, |seam| seam.multishot_free(key));
    AcceptState::Fallback(AcceptFuture::new(fd))
}

/// Reads the next completion for `key` from the worker's multishot registry.
fn multishot_next(binding: WakerBinding, key: MultishotSlotKey) -> MultishotNext {
    IoSeam::with_current(binding.worker_id, |seam| seam.multishot_next(key))
        .unwrap_or(MultishotNext::Ended)
}

/// Drives one single-shot accept, re-arming a fresh one after each item so a
/// stale completion never repeats.
fn poll_fallback(
    stream: &mut AcceptStream<'_>,
    cx: &mut Context<'_>,
) -> Poll<Option<io::Result<TcpStream>>> {
    let AcceptState::Fallback(mut accept) =
        core::mem::replace(&mut stream.state, AcceptState::Done)
    else {
        return Poll::Ready(None);
    };
    match Pin::new(&mut accept).poll(cx) {
        Poll::Pending => {
            stream.state = AcceptState::Fallback(accept);
            Poll::Pending
        }
        Poll::Ready(result) => {
            stream.state = AcceptState::Fallback(AcceptFuture::new(stream.fd));
            Poll::Ready(Some(adopt(result)))
        }
    }
}

/// Adopts a completion result as a connection or a per-accept error.
fn adopt(result: i32) -> io::Result<TcpStream> {
    boundary::adopt_accepted_fd(result).map_or_else(
        || Err(io::Error::from_raw_os_error(-result)),
        |fd| Ok(TcpStream::from(fd)),
    )
}
