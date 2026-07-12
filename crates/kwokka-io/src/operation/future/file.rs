//! In-flight-slot completion futures for file operations.
//!
//! [`FileReadFuture`] and [`FileWriteFuture`] are generic over the caller's
//! buffer ([`IoBufMut`] / [`IoBuf`]) and keep the kernel-facing bytes in the
//! worker's in-flight buffer registry, handing the kernel a borrowed pointer
//! into the slot through [`InlineBuf`], the same path the socket futures use.
//! The caller's buffer is the source (write, copied into the slot on submit) or
//! the sink (read, copied out of the slot on completion). The future owns the
//! buffer for the whole op; a read returns it alongside the byte count on
//! `Ready`, while a write keeps its source until the op resolves and drops it.
//! The slot, not the caller's buffer, is what the kernel touches, so an early
//! drop is safe: it queues a cancel the worker's cancel drain reclaims once the
//! op completes, never under an in-flight kernel access.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use crate::{
    boundary::{self, IoSeam},
    buffer::oneshot::inflight::{INFLIGHT_BUF_STRIDE, InflightSlotKey},
    operation::{InlineBuf, IoBuf, IoBufMut, IoRequest, SubmitResult, future::bytes_from_cqe},
};

/// A future that reads from `fd` at `offset` into an owned buffer.
///
/// The file counterpart of [`RecvFuture`](crate::operation::RecvFuture) plus an
/// offset: the first poll allocates a slot in the worker's in-flight buffer
/// registry, hands the kernel an [`InlineBuf`] over it -- addressed by the
/// polling task's identity token for the `user_data` round trip -- and yields
/// `Pending`. A later poll, woken by the completion drain, copies the slot bytes
/// into the caller's buffer and returns an [`io::Result`] byte count paired with
/// it: the bytes read (`0` at end of file), or the mapped [`io::Error`]. The
/// buffer moves out with the future on `Ready`.
///
/// The slot bytes are owned by the worker's registry, not by the caller's
/// buffer, so dropping the future before the completion arrives is safe: the
/// drop queues a cancel for the in-flight op and the slot is freed only once the
/// kernel signals the op is done. A buffer whose
/// [`capacity`][IoBufMut::capacity] exceeds the in-flight slot stride resolves
/// immediately as an unsupported submit rather than truncating the caller's
/// declared capacity.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker (for
/// example inside a combinator that wraps the waker): the `user_data` round trip
/// decodes the polling task from the waker, so await it directly. Also panics
/// when polled again after resolving: the buffer moves out with the `Ready`
/// value, so a repeat poll has nothing left to return.
#[must_use = "futures do nothing unless polled"]
pub struct FileReadFuture<B: IoBufMut> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the read starts at.
    offset: u64,
    /// The caller's destination buffer. `Some` from construction until it moves
    /// out with the `Ready` value.
    buf: Option<B>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBufMut> FileReadFuture<B> {
    /// Constructs a read future for `fd` at byte `offset` into `buf`.
    pub const fn new(fd: i32, offset: u64, buf: B) -> Self {
        Self {
            fd,
            offset,
            buf: Some(buf),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBufMut> Future for FileReadFuture<B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The polling task's identity is encoded in its waker; the poll boundary
        // decoder rejects a waker the runtime did not build, the same contract
        // the socket futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileReadFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("FileReadFuture polled after resolving; await it only once");
            };
            let Some(mut buf) = this.buf.take() else {
                panic!("FileReadFuture polled after resolving; await it only once");
            };
            let outcome = IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result().map(|slot| {
                    let result = bytes_from_cqe(slot.result);
                    match &result {
                        Ok(count) => seam.harvest_into_buf(key, *count, &mut buf),
                        Err(_) => seam.free_slot(key),
                    }
                    result
                })
            });
            return if let Some(Some(result)) = outcome {
                this.key = None;
                Poll::Ready((result, buf))
            } else {
                this.buf = Some(buf);
                Poll::Pending
            };
        }
        let Some(cap) = this.buf.as_ref().map(IoBufMut::capacity) else {
            panic!("FileReadFuture polled after resolving; await it only once");
        };
        if cap > INFLIGHT_BUF_STRIDE as usize {
            let Some(buf) = this.buf.take() else {
                panic!("FileReadFuture polled after resolving; await it only once");
            };
            // The buffer's declared capacity exceeds the in-flight slot stride;
            // the read cannot stay within the slot, so this resolves as an
            // unsupported submit rather than truncating the caller's declared
            // capacity (mirrors `copy_into_slot`'s write-side rejection instead
            // of a silent truncation).
            return Poll::Ready((bytes_from_cqe(-22), buf));
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, ptr) = seam.allocate_slot(token)?;
                // SAFETY: Invariant -- `ptr` addresses `key`'s slot in the worker's
                // InflightBufSlab, valid for INFLIGHT_BUF_STRIDE writes while the
                // slab lives and the slot stays occupied; the stride check above
                // bounds `cap <= INFLIGHT_BUF_STRIDE`, so the kernel's `cap` writes
                // stay in the slot. Precondition: the slab owns the bytes for the
                // op lifetime, freed by `harvest_into_buf` on the successful-CQE
                // drain, by `free_slot` on the submit-failure branch below (after
                // this `InlineBuf` is consumed by `submit_read`, so no live pointer
                // aliases the slot), or by a stale-rejected cancel -- never by this
                // future's drop while the kernel holds the pointer, so the storage
                // outlives the CQE with nothing aliasing it while the kernel
                // writes. Failure mode: a `cap` past the stride, or freeing the
                // slot before the CQE, lets the kernel write out of bounds or into
                // reused memory -- UB; the stride check and slot ownership exclude
                // both.
                let inline = unsafe { InlineBuf::new(ptr, cap) };
                let request = IoRequest::read(fd, inline, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit_read(request) {
                    Some(key)
                } else {
                    // No driver or the backend rejected the op; return the slot.
                    seam.free_slot(key);
                    None
                }
            });
        if let Some(Some(key)) = submitted {
            this.key = Some(key);
            this.is_submitted = true;
            Poll::Pending
        } else {
            // No seam, no slab, or the submit failed. The production path runs on
            // a real driver with a slab, so this is the test-seam / unsupported
            // path; resolve with -EINVAL rather than hang.
            let Some(buf) = this.buf.take() else {
                panic!("FileReadFuture polled after resolving; await it only once");
            };
            Poll::Ready((bytes_from_cqe(-22), buf))
        }
    }
}

impl<B: IoBufMut> Drop for FileReadFuture<B> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

/// A future that writes an owned buffer's initialized bytes to `fd` at `offset`.
///
/// The file counterpart of [`SendFuture`](crate::operation::SendFuture) plus an
/// offset: the first poll allocates a slot in the worker's in-flight buffer
/// registry, copies the caller's buffer's initialized bytes into it via
/// [`IoSeam::copy_into_slot`], hands the kernel an [`InlineBuf`] over the slot --
/// addressed by the polling task's identity token for the `user_data` round trip
/// -- and yields `Pending`. A later poll, woken by the completion drain, returns
/// an [`io::Result`] byte count: the bytes written, or the mapped [`io::Error`].
/// A write does not read its buffer back, so the future keeps it until the op
/// resolves and drops it rather than returning it.
///
/// The kernel reads the slot copy, not the caller's buffer, so dropping the
/// future before the completion arrives is safe: the drop queues a cancel for
/// the in-flight op and the slot is freed only once the kernel signals the op is
/// done. A buffer whose initialized length exceeds the in-flight slot stride
/// resolves immediately as an unsupported submit (`copy_into_slot` rejects rather
/// than truncates).
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker (for
/// example inside a combinator that wraps the waker): the `user_data` round trip
/// decodes the polling task from the waker, so await it directly. Also panics
/// when polled again after resolving: the slot key clears on `Ready`, so a
/// repeat poll has no in-flight op left to observe.
#[must_use = "futures do nothing unless polled"]
pub struct FileWriteFuture<B: IoBuf> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the write starts at.
    offset: u64,
    /// The caller's source buffer, held for the op lifetime. The kernel reads a
    /// slot copy, not this buffer, so it drops with the future.
    buf: Option<B>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBuf> FileWriteFuture<B> {
    /// Constructs a write future for `fd` at byte `offset` over `buf`.
    pub const fn new(fd: i32, offset: u64, buf: B) -> Self {
        Self {
            fd,
            offset,
            buf: Some(buf),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBuf> Future for FileWriteFuture<B> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // The polling task's identity is encoded in its waker; the poll boundary
        // decoder rejects a waker the runtime did not build, the same contract
        // the read future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileWriteFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("FileWriteFuture polled after resolving; await it only once");
            };
            let outcome = IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result().map(|slot| {
                    seam.free_slot(key);
                    bytes_from_cqe(slot.result)
                })
            });
            return if let Some(Some(result)) = outcome {
                this.key = None;
                Poll::Ready(result)
            } else {
                Poll::Pending
            };
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let Some(buf) = this.buf.as_ref() else {
            panic!("FileWriteFuture polled after resolving; await it only once");
        };
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, ptr) = seam.allocate_slot(token)?;
                if !seam.copy_into_slot(key, buf) {
                    seam.free_slot(key);
                    return None;
                }
                let len = buf.bytes_init();
                // SAFETY: Invariant -- `ptr` addresses `key`'s slot, valid for
                // INFLIGHT_BUF_STRIDE bytes while the slab lives and the slot
                // stays occupied; `copy_into_slot` above already rejected a `buf`
                // whose initialized length exceeds the stride, so `len <=
                // INFLIGHT_BUF_STRIDE` here. Precondition: the slab owns the bytes
                // for the op lifetime, freed by `free_slot` on the completion
                // drain or the submit-failure branch below (after this `InlineBuf`
                // is consumed by `submit`, so no live pointer aliases the slot), or
                // by a stale-rejected cancel -- never by this future's drop while
                // the kernel holds the pointer, so the storage outlives the CQE
                // with nothing aliasing it while the kernel reads. Failure mode:
                // freeing the slot before the CQE lets the kernel read reused
                // memory -- UB, excluded by the slot ownership.
                let mut inline = unsafe { InlineBuf::new(ptr, len) };
                inline.set_init(len);
                let request = IoRequest::write(fd, inline, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit(request) {
                    Some(key)
                } else {
                    seam.free_slot(key);
                    None
                }
            });
        if let Some(Some(key)) = submitted {
            this.key = Some(key);
            this.is_submitted = true;
            Poll::Pending
        } else {
            // No seam, no slab, or the submit failed; resolve with -EINVAL rather
            // than hang on the test-seam / unsupported path.
            Poll::Ready(bytes_from_cqe(-22))
        }
    }
}

impl<B: IoBuf> Drop for FileWriteFuture<B> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{pin::pin, ptr::NonNull, task::Context};
    use std::task::Waker;

    use super::*;
    use crate::{
        boundary::{
            CANCEL_INBOX_CAPACITY, CancelInbox, CancelInboxGuard, SeamGuard, WakeSlot,
            WakerBinding, WakerDecoder, decode_waker, register_decoder,
        },
        buffer::oneshot::inflight::InflightBufSlab,
        operation::FixedBuf,
    };

    fn stub(waker: &Waker) -> Option<WakerBinding> {
        waker.will_wake(Waker::noop()).then_some(WakerBinding {
            token: 7,
            worker_id: 3,
        })
    }

    static STUB: WakerDecoder = stub;

    // Registers the seam decoder and returns the binding the futures decode;
    // `register_decoder` is first-wins, so the worker id is read back rather
    // than assumed.
    fn poll_binding() -> WakerBinding {
        register_decoder(&STUB);
        let Some(binding) = decode_waker(Waker::noop()) else {
            panic!("a registered decoder yields a binding");
        };
        binding
    }

    #[test]
    fn file_read_first_poll_allocates_then_frees_without_driver() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            None,
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        // No driver: the first poll allocates a slot, builds the InlineBuf over
        // it, has the submit refused, frees the slot, and resolves with -EINVAL.
        let Poll::Ready((result, _)) = pin!(FileReadFuture::new(5, 0, [0u8; 8])).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the read immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    fn file_read_rejects_buffer_over_slot_stride() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            None,
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let big = [0u8; INFLIGHT_BUF_STRIDE as usize + 1];
        let Poll::Ready((result, _)) = pin!(FileReadFuture::new(5, 0, big)).poll(&mut cx) else {
            panic!("an oversized buffer resolves immediately");
        };
        assert!(
            result.is_err(),
            "a buffer past the slot stride is rejected, not truncated",
        );
    }

    #[test]
    fn file_write_first_poll_copies_then_frees_without_driver() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            None,
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        // The first poll allocates a slot, copies the input into it, builds the
        // InlineBuf, has the submit refused, and frees the slot.
        let Poll::Ready(result) =
            pin!(FileWriteFuture::new(5, 0, FixedBuf::new(*b"payload!", 4))).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the write immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    fn file_write_rejects_buffer_over_slot_stride() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            None,
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let over = INFLIGHT_BUF_STRIDE as usize + 1;
        let big = FixedBuf::new([0u8; INFLIGHT_BUF_STRIDE as usize + 1], over);
        let Poll::Ready(result) = pin!(FileWriteFuture::new(5, 0, big)).poll(&mut cx) else {
            panic!("an oversized buffer resolves immediately");
        };
        assert!(
            result.is_err(),
            "copy_into_slot rejects a source past the slot stride",
        );
    }

    #[test]
    fn file_read_completion_harvests_slot_bytes() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let Some(ptr) = slab.slot_ptr(key) else {
            panic!("a live slot yields a pointer");
        };
        // SAFETY: `ptr` addresses the live slot's stride-wide region; the test
        // writes 4 bytes within it, standing in for a kernel write, and owns the
        // slab exclusively. Failure mode: a write past the stride would corrupt
        // an adjacent slot or mmap page.
        unsafe {
            ptr.copy_from(b"file".as_ptr(), 4);
        }
        let mut future = FileReadFuture::new(5, 0, [0u8; 8]);
        future.key = Some(key);
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 4,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready((result, out)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the read");
        };
        assert_eq!(result.ok(), Some(4));
        assert_eq!(&out[..4], b"file", "the slot bytes were copied out");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the harvest freed the slot",
        );
    }

    #[test]
    fn file_write_completion_frees_slot() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = FileWriteFuture::new(5, 0, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 4,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the write");
        };
        assert_eq!(result.ok(), Some(4));
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the completion freed the slot",
        );
    }

    #[test]
    fn file_read_drop_queues_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = FileReadFuture::new(5, 0, [0u8; 8]);
        future.key = Some(key);
        future.is_submitted = true;
        drop(future);
        let Some(cancelled) = inbox.pop() else {
            panic!("dropping an in-flight future queues a cancel");
        };
        assert_eq!(cancelled.slot, key.slot);
        assert_eq!(cancelled.op_token, binding.token);
    }

    #[test]
    fn file_write_drop_queues_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = FileWriteFuture::new(5, 0, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        drop(future);
        let Some(cancelled) = inbox.pop() else {
            panic!("dropping an in-flight future queues a cancel");
        };
        assert_eq!(cancelled.slot, key.slot);
        assert_eq!(cancelled.op_token, binding.token);
    }

    #[test]
    fn file_read_completion_frees_slot_on_error() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = FileReadFuture::new(5, 0, [0u8; 8]);
        future.key = Some(key);
        future.is_submitted = true;
        let wake = WakeSlot {
            result: -5,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready((result, _)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the read");
        };
        assert!(result.is_err(), "a negative CQE result maps to an error");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the error path freed the slot without harvesting",
        );
    }

    #[test]
    fn file_write_completion_returns_error() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = FileWriteFuture::new(5, 0, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        let wake = WakeSlot {
            result: -5,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the write");
        };
        assert!(result.is_err(), "a negative CQE result maps to an error");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the completion freed the slot",
        );
    }
}
