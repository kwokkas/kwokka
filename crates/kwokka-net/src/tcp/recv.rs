//! Streaming receives from a connected socket into kernel-selected buffers.

use core::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use kwokka_io::{
    boundary::{self, IoSeam, RecvMultishotAlloc, RecvMultishotNext, WakerBinding},
    buffer::multishot::RecvMultishotSlotKey,
    operation::{ProvidedBuf, ProvidedRecvFuture, SubmitResult},
};

/// A stream of received buffers from one multishot recv.
///
/// [`TcpStream::recv_multishot`](crate::tcp::TcpStream::recv_multishot) returns
/// this. On a kernel with multishot recv, one submitted SQE posts a completion
/// per received chunk, each naming a buffer the kernel picked from the worker's
/// provided-buffer ring; the stream drains them from the worker's registry. On a
/// backend without multishot recv, or when the registry is full, the stream
/// degrades to one single-shot provided recv per item. Either way `next().await`
/// yields `Some(Ok(buf))` for a received chunk, `Some(Err(_))` for a per-recv
/// error, or `None` once a multishot op ends.
///
/// Each `Ok` item is a [`ProvidedBuf`] borrowing the worker pool's bytes; reading
/// it and dropping it recycles the buffer to the ring, so the caller owns the
/// recycle by holding the view no longer than needed. An empty view marks end of
/// stream (a zero-length read), the ordinary signal to stop the receive loop.
///
/// A backend with no provided-buffer group at all resolves the first item as
/// [`io::ErrorKind::Unsupported`]: the caller's signal to abandon the stream and
/// receive through the inline-buffer [`recv`](crate::tcp::TcpStream::recv) path.
///
/// Dropping the stream cancels the in-flight op.
#[must_use = "streams do nothing unless polled"]
pub struct RecvStream<'stream> {
    /// Source socket file descriptor.
    fd: i32,
    /// Where the stream is in its lifecycle.
    state: RecvState,
    /// Binds the stream to the connection's lifetime so it cannot outlive the fd
    /// and receive from a closed or reused descriptor.
    stream: PhantomData<&'stream ()>,
}

/// The recv stream's progress.
enum RecvState {
    /// Nothing submitted yet.
    Idle,
    /// A multishot op is in flight; its key drains completions.
    Multishot(RecvMultishotSlotKey),
    /// The backend lacks multishot recv, or the registry is full; a fresh
    /// single-shot provided recv drives each item.
    Fallback(ProvidedRecvFuture),
    /// The stream ended.
    Done,
}

impl RecvStream<'_> {
    /// Builds a recv stream for connected socket `fd`.
    pub(crate) const fn new(fd: i32) -> Self {
        Self {
            fd,
            state: RecvState::Idle,
            stream: PhantomData,
        }
    }

    /// Awaits the next received buffer, or `None` once the stream ends.
    ///
    /// Written for the ordinary `while let Some(buf) = stream.next().await` loop.
    /// Await it directly on a runtime task: the returned future panics when
    /// polled through a waker the runtime did not build.
    pub async fn next(&mut self) -> Option<io::Result<ProvidedBuf>> {
        core::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Advances the stream by one completion.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<ProvidedBuf>>> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("RecvStream requires the runtime task waker; await it directly");
        };
        if matches!(self.state, RecvState::Idle) {
            self.state = start_multishot(self.fd, binding);
        }
        let multishot_key = match &self.state {
            RecvState::Multishot(key) => Some(*key),
            _ => None,
        };
        if let Some(key) = multishot_key {
            return match multishot_next(binding, key) {
                RecvMultishotNext::Item { result, buf_id } => Poll::Ready(Some(
                    boundary::resolve_provided_recv(binding.worker_id, result, buf_id),
                )),
                RecvMultishotNext::Pending => Poll::Pending,
                RecvMultishotNext::Ended => {
                    self.state = RecvState::Done;
                    Poll::Ready(None)
                }
            };
        }
        if matches!(self.state, RecvState::Fallback(_)) {
            return poll_fallback(self, cx);
        }
        // `Idle` is left non-`Idle` by `start_multishot`; `Done` yields `None`.
        Poll::Ready(None)
    }
}

impl Drop for RecvStream<'_> {
    fn drop(&mut self) {
        if let RecvState::Multishot(key) = &self.state {
            // A live multishot op is `io_bound`, so this drop runs on the owning
            // worker; the cancel reaches the inbox single-writer. A `Fallback`
            // future self-cancels through its own drop, so only the multishot
            // state cancels here.
            boundary::push_recv_multishot_cancel_for_worker(*key);
        }
    }
}

/// Allocates a slot and submits the multishot recv, or picks a fallback.
///
/// Returns [`RecvState::Multishot`] once the op is in flight,
/// [`RecvState::Fallback`] when the registry is full or the backend rejects the
/// multishot op (degrading to single-shot provided recv), or [`RecvState::Done`]
/// when no seam or registry is reachable (a test seam).
fn start_multishot(fd: i32, binding: WakerBinding) -> RecvState {
    let alloc = IoSeam::with_current(binding.worker_id, |seam| {
        seam.allocate_recv_multishot_slot(binding.token)
    });
    let (key, sentinel) = match alloc {
        Some(RecvMultishotAlloc::Allocated { key, sentinel }) => (key, sentinel),
        // A full registry still has bytes to receive; degrade to a single-shot
        // provided recv per item rather than ending the stream.
        Some(RecvMultishotAlloc::Full) => return RecvState::Fallback(ProvidedRecvFuture::new(fd)),
        // No seam or no registry (a test seam) can drive the stream.
        _ => return RecvState::Done,
    };
    let submitted = IoSeam::with_current(binding.worker_id, |seam| {
        seam.submit_recv_multishot_provided(fd, sentinel)
    });
    if let Some(Some(SubmitResult::Submitted(_))) = submitted {
        return RecvState::Multishot(key);
    }
    // The backend lacks multishot recv (a kernel below 6.0), or the op was
    // refused. Free the slot and degrade to the single-shot provided recv, which
    // in turn signals `Unsupported` if no provided-buffer group exists at all.
    IoSeam::with_current(binding.worker_id, |seam| seam.recv_multishot_free(key));
    RecvState::Fallback(ProvidedRecvFuture::new(fd))
}

/// Reads the next completion for `key` from the worker's recv registry.
fn multishot_next(binding: WakerBinding, key: RecvMultishotSlotKey) -> RecvMultishotNext {
    IoSeam::with_current(binding.worker_id, |seam| seam.recv_multishot_next(key))
        .unwrap_or(RecvMultishotNext::Ended)
}

/// Drives one single-shot provided recv, re-arming a fresh one after each item
/// so a stale completion never repeats.
fn poll_fallback(
    stream: &mut RecvStream<'_>,
    cx: &mut Context<'_>,
) -> Poll<Option<io::Result<ProvidedBuf>>> {
    let RecvState::Fallback(mut recv) = core::mem::replace(&mut stream.state, RecvState::Done)
    else {
        return Poll::Ready(None);
    };
    match Pin::new(&mut recv).poll(cx) {
        Poll::Pending => {
            stream.state = RecvState::Fallback(recv);
            Poll::Pending
        }
        Poll::Ready(result) => {
            stream.state = RecvState::Fallback(ProvidedRecvFuture::new(stream.fd));
            Poll::Ready(Some(result))
        }
    }
}
