//! Pinned-storage completion futures for socket operations.
//!
//! [`RecvFuture`] and [`SendFuture`] carry their byte storage inline and
//! hand the kernel a borrowed pointer into it through [`InlineBuf`]. Submits
//! and completion reads travel the poll boundary, the same path the no-buffer
//! socket futures use.

use core::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    boundary::{self, IoSeam},
    operation::{InlineBuf, IoBufMut, IoRequest, SubmitResult},
};

/// A future that receives from socket `fd` into an inline `CAP`-byte buffer.
///
/// The first poll submits a recv op, handing the kernel an [`InlineBuf`]
/// over this future's own `buf` -- addressed by the polling task's identity
/// token for the `user_data` round trip -- and yields `Pending`. A later
/// poll, woken by the completion drain, returns the kernel result paired
/// with the buffer: the bytes received (a short count on a partial read, or
/// 0 when the peer closed), or a negative `-errno`.
///
/// The buffer lives inline in this future, which the runtime pins in its
/// slab slot, so the kernel writes into stable memory with no heap
/// allocation. `CAP` must leave the future small enough for the fixed task
/// slot.
///
/// Await it to completion. While the recv is in flight the kernel holds a
/// pointer into `buf`, so dropping the future before the completion arrives
/// frees that storage under an in-flight write -- undefined behavior. Do
/// not place it in a branch that may be dropped before it resolves. This
/// 0.1.0 limit lifts when the per-op cancel-and-await-CQE path lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the `user_data`
/// round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct RecvFuture<const CAP: usize> {
    /// Source socket file descriptor.
    fd: i32,
    /// Inline destination; the kernel writes here, pinned with the future.
    buf: [u8; CAP],
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<const CAP: usize> RecvFuture<CAP> {
    /// Constructs a recv future for socket `fd`.
    pub const fn new(fd: i32) -> Self {
        Self {
            fd,
            buf: [0u8; CAP],
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for RecvFuture<CAP> {
    type Output = (i32, [u8; CAP]);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the no-buffer socket futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("RecvFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => {
                    Poll::Ready((slot.result, mem::replace(&mut this.buf, [0u8; CAP])))
                }
                _ => Poll::Pending,
            };
        }
        // SAFETY: Invariant -- `buf` is this future's own `CAP`-byte field,
        // so the pointer is non-null and valid for `CAP` writes.
        // Precondition: the future is pinned in its slab slot and is awaited
        // to completion, so the storage outlives the CQE and no other
        // reference aliases it while the kernel holds the buffer. Failure
        // mode: dropping the future before the CQE frees `buf` under an
        // in-flight kernel write -- undefined behavior, the documented
        // await-to-completion limit.
        let buf = unsafe { InlineBuf::new(this.buf.as_mut_ptr(), CAP) };
        let request = IoRequest::recv(this.fd, buf).with_user_data(binding.token);
        match IoSeam::with_current(binding.worker_id, |seam| seam.submit_read(request)) {
            Some(Some(SubmitResult::Submitted(_))) => {
                this.is_submitted = true;
                Poll::Pending
            }
            // No seam, no driver, or the backend rejected the op. The
            // production path runs on a real driver, so this is the
            // test-seam / unsupported path; resolve with -EINVAL rather
            // than hang.
            _ => Poll::Ready((-22, mem::replace(&mut this.buf, [0u8; CAP]))),
        }
    }
}

/// A future that sends the first `len` bytes of an inline `CAP`-byte buffer
/// over socket `fd`.
///
/// The send counterpart of [`RecvFuture`]: the first poll submits a send
/// op, handing the kernel an [`InlineBuf`] over this future's own `buf`
/// with its first `len` bytes marked initialized -- addressed by the
/// polling task's identity token for the `user_data` round trip -- and
/// yields `Pending`. A later poll, woken by the completion drain, returns
/// the kernel result: the bytes sent (a short count when the socket send
/// buffer fills), or a negative `-errno`.
///
/// The buffer lives inline in this future, which the runtime pins in its
/// slab slot, so the kernel reads stable memory with no heap allocation.
/// `CAP` must leave the future small enough for the fixed task slot.
///
/// Await it to completion. While the send is in flight the kernel holds a
/// pointer into `buf`, so dropping the future before the completion arrives
/// frees that storage under an in-flight read -- undefined behavior. Do not
/// place it in a branch that may be dropped before it resolves. This 0.1.0
/// limit lifts when the per-op cancel-and-await-CQE path lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the `user_data`
/// round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct SendFuture<const CAP: usize> {
    /// Target socket file descriptor.
    fd: i32,
    /// Inline source; the kernel reads `len` bytes here, pinned with the future.
    buf: [u8; CAP],
    /// Number of valid bytes in `buf` to send.
    len: usize,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<const CAP: usize> SendFuture<CAP> {
    /// Constructs a send future for socket `fd` over `data`, sending its
    /// first `len` bytes (clamped to `CAP`).
    pub const fn new(fd: i32, data: [u8; CAP], len: usize) -> Self {
        Self {
            fd,
            buf: data,
            len: if len < CAP { len } else { CAP },
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for SendFuture<CAP> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the recv future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("SendFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => Poll::Ready(slot.result),
                _ => Poll::Pending,
            };
        }
        // SAFETY: Invariant -- `buf` is this future's own `CAP`-byte field,
        // so the pointer is non-null and valid for `CAP` bytes.
        // Precondition: the future is pinned in its slab slot and is awaited
        // to completion, so the storage outlives the CQE and no other
        // reference aliases it while the kernel reads the buffer. Failure
        // mode: dropping the future before the CQE frees `buf` under an
        // in-flight kernel read -- undefined behavior, the documented
        // await-to-completion limit.
        let mut buf = unsafe { InlineBuf::new(this.buf.as_mut_ptr(), CAP) };
        buf.set_init(this.len);
        let request = IoRequest::send(this.fd, buf).with_user_data(binding.token);
        match IoSeam::with_current(binding.worker_id, |seam| seam.submit(request)) {
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
