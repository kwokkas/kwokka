//! In-flight-slot completion futures for file operations.
//!
//! [`FileReadFuture`] and [`FileWriteFuture`] keep their byte storage in the
//! worker's in-flight buffer registry and hand the kernel a borrowed pointer
//! into the slot through [`InlineBuf`]. Submits and completion reads travel the
//! poll boundary, the same path the socket futures use. The slot, not the
//! future, owns the bytes, so an early drop is safe: it queues a cancel that the
//! worker's cancel drain reclaims once the op completes, never under an
//! in-flight kernel access.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use crate::{
    boundary::{self, IoSeam},
    buffer::inflight::InflightSlotKey,
    operation::{
        InlineBuf, IoBufMut, IoRequest, SubmitResult,
        future::{assert_cap_fits, bytes_from_cqe},
    },
};

/// A future that reads from `fd` into a per-worker in-flight slot.
///
/// The file counterpart of `RecvFuture` plus an offset: the first poll
/// allocates a slot in the worker's in-flight buffer
/// registry, hands the kernel an [`InlineBuf`] over it -- addressed by the
/// polling task's identity token for the `user_data` round trip -- and yields
/// `Pending`. A later poll, woken by the completion drain, copies the slot
/// bytes out and returns an [`io::Result`] byte count paired with them: the
/// bytes read (`0` at end of file), or the mapped [`io::Error`].
///
/// The slot bytes are owned by the worker's registry, not by this future, so
/// dropping the future before the completion arrives is safe: the drop queues a
/// cancel for the in-flight op and the slot is freed only once the kernel
/// signals the op is done. `CAP` must not exceed the in-flight slot stride,
/// enforced at compile time.
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
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<const CAP: usize> FileReadFuture<CAP> {
    /// Constructs a read future for `fd` starting at byte `offset`.
    pub const fn new(fd: i32, offset: u64) -> Self {
        Self {
            fd,
            offset,
            key: None,
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for FileReadFuture<CAP> {
    type Output = (io::Result<usize>, [u8; CAP]);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        const { assert_cap_fits::<CAP>() };
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the socket futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileReadFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            // Unreachable in correct use: a successful submit sets `is_submitted`
            // and `key = Some` together, and the completion clears `key` only
            // with `Ready`. This guards a poll after `Ready` (caller misuse).
            let Some(key) = this.key else {
                return Poll::Ready((bytes_from_cqe(-22), [0u8; CAP]));
            };
            return match IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result().map(|slot| {
                    let mut out = [0u8; CAP];
                    let result = bytes_from_cqe(slot.result);
                    match result {
                        Ok(count) => seam.harvest_into(key, count, &mut out),
                        Err(_) => seam.free_slot(key),
                    }
                    (result, out)
                })
            }) {
                Some(Some((result, out))) => {
                    this.key = None;
                    Poll::Ready((result, out))
                }
                _ => Poll::Pending,
            };
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, ptr) = seam.allocate_slot(token)?;
                // SAFETY: Invariant -- `ptr` addresses `key`'s slot in the worker's
                // InflightBufSlab, valid for INFLIGHT_BUF_STRIDE writes while the
                // slab lives and the slot stays occupied; the const guard bounds
                // CAP <= stride, so the kernel's CAP writes stay in the slot.
                // Precondition: the slab owns the bytes for the op lifetime,
                // freed by `harvest_into` on the successful-CQE drain, by
                // `free_slot` on the submit-failure branch below (after this
                // `InlineBuf` is consumed by `submit_read`, so no live pointer
                // aliases the slot), or by a stale-rejected cancel -- never by
                // this future's drop while the kernel holds the pointer, so the
                // storage outlives the CQE with nothing aliasing it while the
                // kernel writes. Failure mode: a CAP past the stride, or freeing
                // the slot before the CQE, lets the kernel write out of bounds or
                // into reused memory -- UB; the guard and slot ownership exclude
                // both.
                let buf = unsafe { InlineBuf::new(ptr, CAP) };
                let request = IoRequest::read(fd, buf, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit_read(request) {
                    Some(key)
                } else {
                    // No driver or the backend rejected the op; return the slot.
                    seam.free_slot(key);
                    None
                }
            });
        match submitted {
            Some(Some(key)) => {
                this.key = Some(key);
                this.is_submitted = true;
                Poll::Pending
            }
            // No seam, no slab, or the submit failed. The production path runs
            // on a real driver with a slab, so this is the test-seam /
            // unsupported path; resolve with -EINVAL rather than hang.
            _ => Poll::Ready((bytes_from_cqe(-22), [0u8; CAP])),
        }
    }
}

impl<const CAP: usize> Drop for FileReadFuture<CAP> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

/// A future that writes the first `len` bytes of an inline `CAP`-byte
/// buffer to `fd` at `offset`.
///
/// The file counterpart of `SendFuture` plus an offset: the first poll
/// allocates a slot in the worker's in-flight buffer
/// registry, copies the future's first `len` input bytes into it, hands the
/// kernel an [`InlineBuf`] over the slot -- addressed by the polling task's
/// identity token for the `user_data` round trip -- and yields `Pending`. A
/// later poll, woken by the completion drain, returns an [`io::Result`]: the
/// bytes written, or the mapped [`io::Error`].
///
/// The kernel reads the slot copy, not the future's inline input, so dropping
/// the future before the completion arrives is safe: the drop queues a cancel
/// for the in-flight op and the slot is freed only once the kernel signals the
/// op is done. `CAP` must not exceed the in-flight slot stride, enforced at
/// compile time.
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
    /// Inline source copied into the slot on the first poll; the kernel reads
    /// the slot copy, so this storage is free to drop with the future.
    buf: [u8; CAP],
    /// Number of valid bytes in `buf` to write.
    len: usize,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
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
            key: None,
            is_submitted: false,
        }
    }
}

impl<const CAP: usize> Future for FileWriteFuture<CAP> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        const { assert_cap_fits::<CAP>() };
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the read future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("FileWriteFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            // Unreachable in correct use: a successful submit sets `is_submitted`
            // and `key = Some` together, and the completion clears `key` only
            // with `Ready`. This guards a poll after `Ready` (caller misuse).
            let Some(key) = this.key else {
                return Poll::Ready(bytes_from_cqe(-22));
            };
            return match IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result().map(|slot| {
                    seam.free_slot(key);
                    bytes_from_cqe(slot.result)
                })
            }) {
                Some(Some(result)) => {
                    this.key = None;
                    Poll::Ready(result)
                }
                _ => Poll::Pending,
            };
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let len = this.len;
        let src = this.buf.as_ptr();
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, ptr) = seam.allocate_slot(token)?;
                // SAFETY: Invariant -- `ptr` addresses `key`'s slot, valid for
                // INFLIGHT_BUF_STRIDE writes; `src` is this future's own `buf`,
                // valid for `len` reads with `len <= CAP <= stride`; the slot and
                // the inline buffer are distinct allocations, so the copy never
                // overlaps. Precondition: `len` was clamped to CAP at construction.
                // Failure mode: a len past the stride writes outside the slot --
                // excluded by the const guard and the constructor clamp.
                unsafe {
                    ptr.copy_from_nonoverlapping(src, len);
                }
                // SAFETY: Invariant -- `ptr` addresses `key`'s slot, valid for
                // INFLIGHT_BUF_STRIDE bytes while the slab lives and the slot stays
                // occupied; the const guard bounds CAP <= stride. Precondition: the
                // slab owns the bytes for the op lifetime, freed by `free_slot` on
                // the completion drain or the submit-failure branch below (after
                // this `InlineBuf` is consumed by `submit`, so no live pointer
                // aliases the slot), or by a stale-rejected cancel -- never by this
                // future's drop while the kernel holds the pointer, so the storage
                // outlives the CQE with nothing aliasing it while the kernel reads.
                // Failure mode: freeing the slot before the CQE lets the kernel read
                // reused memory -- UB, excluded by the slot ownership.
                let mut buf = unsafe { InlineBuf::new(ptr, CAP) };
                buf.set_init(len);
                let request = IoRequest::write(fd, buf, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit(request) {
                    Some(key)
                } else {
                    seam.free_slot(key);
                    None
                }
            });
        match submitted {
            Some(Some(key)) => {
                this.key = Some(key);
                this.is_submitted = true;
                Poll::Pending
            }
            // No seam, no slab, or the submit failed; resolve with -EINVAL
            // rather than hang on the test-seam / unsupported path.
            _ => Poll::Ready(bytes_from_cqe(-22)),
        }
    }
}

impl<const CAP: usize> Drop for FileWriteFuture<CAP> {
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
        buffer::inflight::InflightBufSlab,
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
        let Poll::Ready((result, _)) = pin!(FileReadFuture::<8>::new(5, 0)).poll(&mut cx) else {
            panic!("a driverless seam resolves the read immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
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
            pin!(FileWriteFuture::<8>::new(5, 0, *b"payload!", 4)).poll(&mut cx)
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
        let mut future = FileReadFuture::<8>::new(5, 0);
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
        let mut future = FileWriteFuture::<8>::new(5, 0, *b"payload!", 4);
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
        let mut future = FileReadFuture::<8>::new(5, 0);
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
        let mut future = FileWriteFuture::<8>::new(5, 0, *b"payload!", 4);
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
        let mut future = FileReadFuture::<8>::new(5, 0);
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
        let mut future = FileWriteFuture::<8>::new(5, 0, *b"payload!", 4);
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
