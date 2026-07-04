//! In-flight-slot completion future for zero-copy send (`SEND_ZC`).
//!
//! [`SendZcFuture`] keeps its byte storage in the worker's in-flight buffer
//! registry and hands the kernel a borrowed pointer into the slot through
//! [`InlineBuf`], the same as the plain `SendFuture`. A zero-copy send posts
//! two completions: the send result, then a notification
//! that the kernel has released the buffer. The future stays pending until that
//! notification, so it resolves only once the buffer is free to reuse, and an
//! early drop queues a cancel the worker reclaims once the op completes, never
//! under an in-flight kernel access.

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
        CqeFlags, InlineBuf, IoBufMut, IoRequest, SubmitResult,
        future::{assert_cap_fits, bytes_from_cqe},
    },
};

/// A future that sends the first `len` bytes of an inline `CAP`-byte buffer over
/// socket `fd`, zero-copy when the backend supports it.
///
/// The first poll allocates a slot in the worker's in-flight buffer registry,
/// copies the future's first `len` input bytes into it, and submits a `SEND_ZC`
/// op over an [`InlineBuf`] when the backend reports support, or a plain copying
/// send otherwise (fallback parity). The kernel reads the slot copy, not the
/// future's inline input.
///
/// A zero-copy send posts two completions sharing the op `user_data`: the send
/// result, then a notification (`IORING_CQE_F_NOTIF`, `io_uring_prep_send_zc.3`)
/// once the kernel has released the buffer. The future stashes the byte count
/// from the first completion and resolves only on the notification, so the
/// awaited value arrives when the buffer is safe to reuse. A first completion
/// without the more-to-come flag (the kernel copied inline, an error, or the
/// plain-send fallback) has no notification and resolves at once.
///
/// Dropping the future before the op completes is safe: the drop queues a cancel
/// for the in-flight op and the slot is freed only once the kernel signals it is
/// done, never under a live kernel read. `CAP` must not exceed the in-flight
/// slot stride, enforced at compile time.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker: the
/// `user_data` round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct SendZcFuture<const CAP: usize> {
    /// Target socket file descriptor.
    fd: i32,
    /// Inline source copied into the slot on the first poll; the kernel reads
    /// the slot copy, so this storage is free to drop with the future.
    buf: [u8; CAP],
    /// Number of valid bytes in `buf` to send.
    len: usize,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
    /// The send result stashed from the first completion, held until the
    /// notification releases the buffer. `None` until the first completion of a
    /// two-stage send arrives.
    primary: Option<io::Result<usize>>,
}

impl<const CAP: usize> SendZcFuture<CAP> {
    /// Constructs a zero-copy send future for socket `fd` over `data`, sending
    /// its first `len` bytes (clamped to `CAP`).
    pub const fn new(fd: i32, data: [u8; CAP], len: usize) -> Self {
        Self {
            fd,
            buf: data,
            len: if len < CAP { len } else { CAP },
            key: None,
            is_submitted: false,
            primary: None,
        }
    }
}

impl<const CAP: usize> Future for SendZcFuture<CAP> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        const { assert_cap_fits::<CAP>() };
        // The polling task's identity is encoded in its waker; the poll boundary
        // decoder rejects a waker the runtime did not build, the same contract
        // the plain send future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("SendZcFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                // Unreachable in correct use: the completion clears `key` only
                // with `Ready`. This guards a poll after `Ready` (caller misuse).
                return Poll::Ready(bytes_from_cqe(-22));
            };
            let resolved = IoSeam::with_current(binding.worker_id, |seam| {
                if this.primary.is_none() {
                    let Some(slot) = seam.completion_result() else {
                        return Poll::Pending;
                    };
                    let result = bytes_from_cqe(slot.result);
                    if !CqeFlags::new(slot.flags).contains(CqeFlags::MORE) {
                        // No notification is coming: the kernel copied the buffer
                        // inline, the op errored, or this was the plain-send
                        // fallback. Free the slot and resolve now.
                        seam.free_slot(key);
                        this.key = None;
                        return Poll::Ready(result);
                    }
                    // A zero-copy send whose notification is still coming. Stash
                    // the count and check readiness in the same poll, since both
                    // completions may already be drained.
                    this.primary = Some(result);
                }
                if seam.slot_notif_ready(key) {
                    // The kernel released the buffer. Free this live future's own
                    // slot and resolve with the stashed count.
                    seam.free_slot(key);
                    this.key = None;
                    let Some(result) = this.primary.take() else {
                        return Poll::Ready(bytes_from_cqe(-22));
                    };
                    Poll::Ready(result)
                } else {
                    Poll::Pending
                }
            });
            return resolved.unwrap_or(Poll::Pending);
        }
        let fd = this.fd;
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
                // the completion drain (on the notification for a zero-copy send,
                // on the primary otherwise) or the submit-failure branch below
                // (after this `InlineBuf` is consumed by `submit`, so no live
                // pointer aliases the slot), or by a stale-rejected cancel -- never
                // by this future's drop while the kernel holds the pointer, so the
                // storage outlives both the send and its notification with nothing
                // aliasing it while the kernel reads.
                // Failure mode: freeing the slot before the notification lets the
                // kernel read reused memory -- UB, excluded by the notif-gated free.
                let mut buf = unsafe { InlineBuf::new(ptr, CAP) };
                buf.set_init(len);
                // Zero-copy send when the backend supports it, else a plain
                // copying send (fallback parity); the resolve path handles both by
                // the completion's more-to-come flag.
                let request = if seam.is_send_zc_supported() {
                    IoRequest::send_zc(fd, buf).with_user_data(token)
                } else {
                    IoRequest::send(fd, buf).with_user_data(token)
                };
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
            // No seam, no slab, or the submit failed; resolve with -EINVAL rather
            // than hang on the test-seam / unsupported path.
            _ => Poll::Ready(bytes_from_cqe(-22)),
        }
    }
}

impl<const CAP: usize> Drop for SendZcFuture<CAP> {
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

    fn poll_binding() -> WakerBinding {
        register_decoder(&STUB);
        let Some(binding) = decode_waker(Waker::noop()) else {
            panic!("a registered decoder yields a binding");
        };
        binding
    }

    #[test]
    fn first_poll_copies_then_frees_without_driver() {
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
        let mut cx = Context::from_waker(Waker::noop());
        // No driver: the first poll allocates a slot, copies the input, builds the
        // InlineBuf, has the submit refused, frees the slot, and resolves -EINVAL.
        let Poll::Ready(result) = pin!(SendZcFuture::<8>::new(5, *b"payload!", 4)).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the send immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    fn a_completion_without_more_resolves_on_the_primary() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = SendZcFuture::<8>::new(5, *b"payload!", 4);
        future.key = Some(key);
        future.is_submitted = true;
        // flags 0: no notification follows (inline copy, error, or plain send).
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
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a completion without more-to-come resolves at once");
        };
        assert_eq!(result.ok(), Some(4));
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the primary completion freed the slot",
        );
    }

    #[test]
    fn a_more_completion_stays_pending_without_a_notification() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = SendZcFuture::<8>::new(5, *b"payload!", 4);
        future.key = Some(key);
        future.is_submitted = true;
        // MORE set with no notification yet: a zero-copy send waiting on its
        // buffer-release notification.
        let wake = WakeSlot {
            result: 4,
            flags: CqeFlags::MORE.bits(),
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            pin!(future).poll(&mut cx).is_pending(),
            "the send stays pending until its notification releases the buffer",
        );
    }

    #[test]
    fn a_more_completion_resolves_once_the_notification_is_ready() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        // Both completions of a same-batch drain are already visible: mark the
        // slot notif-ready before the seam borrows the slab, so the MORE primary
        // and its notification resolve in one poll and the seam pointer is the
        // sole slab access during the poll.
        slab.mark_notif_ready_by_op_token(binding.token);
        let mut future = SendZcFuture::<8>::new(5, *b"payload!", 4);
        future.key = Some(key);
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 4,
            flags: CqeFlags::MORE.bits(),
            buf_id: None,
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(wake),
        );
        let _guard = SeamGuard::install(&seam);
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a MORE primary with a ready notification resolves in one poll");
        };
        assert_eq!(
            result.ok(),
            Some(4),
            "the stashed primary count is returned"
        );
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the notification freed the slot",
        );
    }

    #[test]
    fn drop_queues_a_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = SendZcFuture::<8>::new(5, *b"payload!", 4);
        future.key = Some(key);
        drop(future);
        assert_eq!(
            inbox.pop().map(|queued| queued.slot),
            Some(key.slot),
            "the drop queued a cancel for the in-flight slot",
        );
    }
}
