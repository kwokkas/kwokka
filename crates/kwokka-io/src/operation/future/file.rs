//! Pinned-storage completion futures for file operations.
//!
//! [`FileReadFuture`] and [`FileWriteFuture`] carry their byte storage inline
//! and hand the kernel a borrowed pointer into it through [`InlineBuf`].
//! Submits and completion reads travel the poll boundary, the same path the
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

/// A future that reads from `fd` into an inline `CAP`-byte buffer.
///
/// The file counterpart of `RecvFuture` plus an offset: the first poll
/// submits a read op, handing the kernel an [`InlineBuf`] over this
/// future's own `buf` -- addressed by the polling task's identity token
/// for the `user_data` round trip -- and yields `Pending`. A later poll,
/// woken by the completion drain, returns the kernel result paired with
/// the filled buffer.
///
/// The buffer lives inline in this future, which the runtime pins in its
/// slab slot, so the kernel writes into stable memory with no heap
/// allocation. `CAP` must leave the future small enough for the fixed task
/// slot.
///
/// Await it to completion. While the read is in flight the kernel holds a
/// pointer into `buf`, so dropping the future before the completion
/// arrives frees that storage under an in-flight write -- undefined
/// behavior. Do not place it in a branch that may be dropped before it
/// resolves. This 0.1.0 limit lifts when the per-op cancel-and-await-CQE
/// path lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the `user_data`
/// round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct FileReadFuture<const CAP: usize> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the read starts at.
    offset: u64,
    /// Inline destination; the kernel writes here, pinned with the future.
    buf: [u8; CAP],
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<const CAP: usize> FileReadFuture<CAP> {
    /// Constructs a read future for `fd` starting at byte `offset`.
    pub const fn new(fd: i32, offset: u64) -> Self {
        Self {
            fd,
            offset,
            buf: [0u8; CAP],
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for FileReadFuture<CAP> {
    type Output = (i32, [u8; CAP]);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the socket futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileReadFuture requires the runtime task waker; await it directly");
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
        let request = IoRequest::read(this.fd, buf, this.offset).with_user_data(binding.token);
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

/// A future that writes the first `len` bytes of an inline `CAP`-byte
/// buffer to `fd` at `offset`.
///
/// The file counterpart of `SendFuture` plus an offset: the first poll
/// submits a write op, handing the kernel an [`InlineBuf`] over this
/// future's own `buf` with its first `len` bytes marked initialized --
/// addressed by the polling task's identity token for the `user_data`
/// round trip -- and yields `Pending`. A later poll, woken by the
/// completion drain, returns the kernel result: the bytes written, or a
/// negative `-errno`.
///
/// The buffer lives inline in this future, which the runtime pins in its
/// slab slot, so the kernel reads stable memory with no heap allocation.
/// `CAP` must leave the future small enough for the fixed task slot.
///
/// Await it to completion. While the write is in flight the kernel holds a
/// pointer into `buf`, so dropping the future before the completion
/// arrives frees that storage under an in-flight read -- undefined
/// behavior. Do not place it in a branch that may be dropped before it
/// resolves. This 0.1.0 limit lifts when the per-op cancel-and-await-CQE
/// path lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the `user_data`
/// round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct FileWriteFuture<const CAP: usize> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the write starts at.
    offset: u64,
    /// Inline source; the kernel reads `len` bytes here, pinned with the future.
    buf: [u8; CAP],
    /// Number of valid bytes in `buf` to write.
    len: usize,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<const CAP: usize> FileWriteFuture<CAP> {
    /// Constructs a write future for `fd` at byte `offset` over `data`,
    /// writing its first `len` bytes (clamped to `CAP`).
    pub const fn new(fd: i32, offset: u64, data: [u8; CAP], len: usize) -> Self {
        Self {
            fd,
            offset,
            buf: data,
            len: if len < CAP { len } else { CAP },
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for FileWriteFuture<CAP> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the read future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileWriteFuture requires the runtime task waker; await it directly");
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
        let request = IoRequest::write(this.fd, buf, this.offset).with_user_data(binding.token);
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
