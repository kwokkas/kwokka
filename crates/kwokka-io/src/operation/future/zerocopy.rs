//! In-flight-slot completion future for zero-copy send (`SEND_ZC`).
//!
//! [`SendZcFuture`] is generic over the caller's buffer ([`IoBuf`]) and keeps
//! its kernel-facing bytes in the worker's in-flight buffer registry, handing
//! the kernel a borrowed pointer into the slot through [`InlineBuf`] -- the
//! same slot-copy path [`SendFuture`](crate::operation::SendFuture) uses. A
//! zero-copy send posts two completions sharing the op `user_data`: the send
//! result, then a notification once the kernel has released the slot (not the
//! caller's buffer, which was only ever a pre-submit copy source). The future
//! stays pending until that notification, so it resolves only once the slot is
//! free to reuse, and an early drop queues a cancel the worker reclaims once
//! the op completes, never under an in-flight kernel access.

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
        CqeFlags, InlineBuf, IoBuf, IoBufMut, IoRequest, SubmitResult, future::bytes_from_cqe,
    },
};

/// A future that sends an owned buffer's initialized bytes over socket `fd`,
/// zero-copy when the backend supports it.
///
/// The send counterpart of [`SendFuture`](crate::operation::SendFuture) with a
/// two-stage completion: the first poll allocates a slot in the worker's
/// in-flight buffer registry, copies the caller's buffer's initialized bytes
/// into it via [`IoSeam::copy_into_slot`], hands the kernel an [`InlineBuf`]
/// over the slot -- addressed by the polling task's identity token for the
/// `user_data` round trip -- and yields `Pending`. The kernel reads the slot
/// copy, never the caller's buffer, which drops freely once the copy completes:
/// it is a pre-submit source, not the kernel-facing memory.
///
/// A zero-copy send posts two completions sharing the op `user_data`: the send
/// result, then a notification (`IORING_CQE_F_NOTIF`, `io_uring_prep_send_zc.3`)
/// once the kernel has released the slot. The future stashes the byte count from
/// the first completion and resolves only on the notification, so the awaited
/// value arrives when the slot is safe to reuse. A first completion without the
/// more-to-come flag (the kernel copied inline, an error, or the plain-send
/// fallback) has no notification and resolves at once.
///
/// A kernel that reports `SEND_ZC` support can still best-effort-refuse an
/// individual send with `-EINVAL`, either at submission (a full ring under
/// resource pressure) or as the operation's own completion. The exact kernel
/// trigger is not pinned down in the reference mirror; the refusal is observed
/// empirically under load. The future treats either refusal as a runtime
/// fallback: it re-submits the same still-held bytes as a plain copying send, so
/// the caller receives the byte count either way and only observes an error for
/// a genuine send failure or a fallback the backend also refuses. The
/// substitution happens at most once.
///
/// Dropping the future before the op completes is safe: the drop queues a cancel
/// for the in-flight op and the slot is freed only once the kernel signals it is
/// done, never under a live kernel read. A buffer whose initialized length
/// exceeds the in-flight slot stride resolves immediately as an unsupported
/// submit (`copy_into_slot` rejects rather than truncates).
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker (for
/// example inside a combinator that wraps the waker): the `user_data` round trip
/// decodes the polling task from the waker, so await it directly. Also panics
/// when polled again after resolving: the slot key clears on `Ready`, so a
/// repeat poll has no in-flight op left to observe.
#[must_use = "futures do nothing unless polled"]
pub struct SendZcFuture<B: IoBuf> {
    /// Target socket file descriptor.
    fd: i32,
    /// The caller's source buffer. The kernel reads the slot copy, not this
    /// buffer, so it drops freely with the future once the first poll submits.
    buf: Option<B>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
    /// The send result stashed from the first completion, held until the
    /// notification releases the slot. `None` until the first completion of a
    /// two-stage send arrives.
    primary: Option<io::Result<usize>>,
    /// Whether the op currently in flight at `key` was submitted as `SEND_ZC`
    /// (`true`) or a plain send (`false`). Gates the runtime-refusal fallback:
    /// only a zero-copy submit's `-EINVAL` completion re-submits as plain; an
    /// already-plain send's `-EINVAL` is a genuine error and surfaces as one.
    was_zero_copy: bool,
    /// Whether the runtime-refusal fallback has already re-submitted once. A
    /// second `-EINVAL` after the fallback surfaces as an error rather than
    /// substituting again.
    has_fallen_back: bool,
}

impl<B: IoBuf> SendZcFuture<B> {
    /// Constructs a zero-copy send future for socket `fd` over `buf`.
    pub const fn new(fd: i32, buf: B) -> Self {
        Self {
            fd,
            buf: Some(buf),
            key: None,
            is_submitted: false,
            primary: None,
            was_zero_copy: false,
            has_fallen_back: false,
        }
    }
}

/// The outcome of a submit attempt through the seam.
enum SubmitOutcome {
    /// The op reached the driver; its slot is held at this key.
    Submitted(InflightSlotKey),
    /// The op reached the driver but the driver refused it (a full submission
    /// queue or an unsupported op). Retryable: a plain resubmit may succeed once
    /// the queue drains.
    Refused,
    /// No backend to submit through (no driver or slab), or the buffer could not
    /// be staged. Not retryable through this seam.
    Unavailable,
}

/// Copies `buf`'s initialized bytes into a fresh in-flight slot and submits a
/// send on `fd`, zero-copy when `is_zero_copy` else plain, reporting whether the op
/// reached the driver ([`SubmitOutcome::Submitted`]), the driver refused it
/// ([`SubmitOutcome::Refused`], retryable), or no backend was available
/// ([`SubmitOutcome::Unavailable`]). Frees the slot on every non-submitted path.
/// Shared by the initial submit and the runtime-refusal plain resubmit.
fn submit_send<B: IoBuf>(
    seam: &IoSeam,
    fd: i32,
    token: u64,
    buf: &B,
    is_zero_copy: bool,
) -> SubmitOutcome {
    let Some((key, ptr)) = seam.allocate_slot(token) else {
        return SubmitOutcome::Unavailable;
    };
    if !seam.copy_into_slot(key, buf) {
        seam.free_slot(key);
        return SubmitOutcome::Unavailable;
    }
    let len = buf.bytes_init();
    // SAFETY: Invariant -- `ptr` addresses `key`'s slot, valid for
    // INFLIGHT_BUF_STRIDE bytes while the slab lives and the slot stays occupied;
    // `copy_into_slot` above already rejected a `buf` whose initialized length
    // exceeds the stride, so `len <= INFLIGHT_BUF_STRIDE` here. Precondition: the
    // slab owns the bytes for the op lifetime, freed by `free_slot` on the
    // completion drain (on the notification for a zero-copy send, on the primary
    // otherwise), the submit-failure branch below (after this `InlineBuf` is
    // consumed by `submit`, so no live pointer aliases the slot), the
    // runtime-refusal fallback's free-then-resubmit, or a stale-rejected cancel
    // -- never by the future's drop while the kernel holds the pointer, so the
    // storage outlives both the send and its notification with nothing aliasing
    // it while the kernel reads. Failure mode: freeing the slot before the
    // notification lets the kernel read reused memory -- UB, excluded by the
    // notif-gated free.
    let mut inline = unsafe { InlineBuf::new(ptr, len) };
    inline.set_init(len);
    // Zero-copy send when requested, else a plain copying send (fallback parity);
    // the resolve path handles both by the completion's more-to-come flag.
    let request = if is_zero_copy {
        IoRequest::send_zc(fd, inline).with_user_data(token)
    } else {
        IoRequest::send(fd, inline).with_user_data(token)
    };
    match seam.submit(request) {
        Some(SubmitResult::Submitted(_)) => SubmitOutcome::Submitted(key),
        Some(_) => {
            seam.free_slot(key);
            SubmitOutcome::Refused
        }
        None => {
            seam.free_slot(key);
            SubmitOutcome::Unavailable
        }
    }
}

/// The outcome of a completion poll, resolved outside the [`IoSeam::with_current`]
/// borrow so the refused arm can self-wake with the poll's `Context` (which the
/// closure does not have).
enum ZcStep {
    /// The future may resolve with this result.
    Ready(io::Result<usize>),
    /// The in-flight op has not completed; the driver wakes the task.
    Waiting,
    /// The zero-copy send was runtime-refused and its slot is freed; the future
    /// is reset to unsubmitted, so the next poll re-submits the still-held bytes
    /// plain through the first-submit path. The poll self-wakes to reach it.
    Refused,
}

impl<B: IoBuf> Future for SendZcFuture<B> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // The polling task's identity is encoded in its waker; the poll boundary
        // decoder rejects a waker the runtime did not build, the same contract
        // the plain send future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("SendZcFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("SendZcFuture polled after resolving; await it only once");
            };
            let step = IoSeam::with_current(binding.worker_id, |seam| {
                if this.primary.is_none() {
                    let Some(slot) = seam.completion_result() else {
                        return ZcStep::Waiting;
                    };
                    let result = bytes_from_cqe(slot.result);
                    if !CqeFlags::new(slot.flags).contains(CqeFlags::MORE) {
                        if this.was_zero_copy && !this.has_fallen_back && slot.result == -22 {
                            // The kernel best-effort-refused this zero-copy send at
                            // runtime with -EINVAL (observed under load; the exact
                            // kernel trigger is unconfirmed). An errored SEND_ZC
                            // carries no MORE flag, so no notification follows and
                            // the slot is free now (io_uring_prep_send_zc.3). Free
                            // the slot and reset to
                            // unsubmitted; the poll self-wakes and the next poll
                            // re-submits the still-held bytes plain through the
                            // already-tested first-submit path, so a transient
                            // submit failure retries next tick instead of surfacing
                            // the stale refusal.
                            seam.free_slot(key);
                            this.key = None;
                            this.is_submitted = false;
                            this.has_fallen_back = true;
                            this.was_zero_copy = false;
                            return ZcStep::Refused;
                        }
                        seam.free_slot(key);
                        this.key = None;
                        return ZcStep::Ready(result);
                    }
                    // A zero-copy send whose notification is still coming. Stash
                    // the count and check readiness in the same poll, since both
                    // completions may already be drained.
                    this.primary = Some(result);
                }
                if seam.slot_notif_ready(key) {
                    // The kernel released the slot. Free this live future's own
                    // slot and resolve with the stashed count.
                    seam.free_slot(key);
                    this.key = None;
                    let Some(result) = this.primary.take() else {
                        return ZcStep::Ready(bytes_from_cqe(-22));
                    };
                    ZcStep::Ready(result)
                } else {
                    ZcStep::Waiting
                }
            });
            return match step.unwrap_or(ZcStep::Waiting) {
                ZcStep::Ready(result) => Poll::Ready(result),
                ZcStep::Waiting => Poll::Pending,
                ZcStep::Refused => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            };
        }
        let fd = this.fd;
        let token = binding.token;
        // After a runtime refusal the resubmit must be a plain send, never another
        // zero-copy attempt, which the kernel just refused.
        let can_zero_copy = !this.has_fallen_back;
        let Some(buf) = this.buf.as_ref() else {
            panic!("SendZcFuture polled after resolving; await it only once");
        };
        let outcome = IoSeam::with_current(binding.worker_id, |seam| {
            let is_zero_copy = can_zero_copy && seam.is_send_zc_supported();
            (
                submit_send(seam, fd, token, buf, is_zero_copy),
                is_zero_copy,
            )
        });
        match outcome {
            Some((SubmitOutcome::Submitted(key), is_zero_copy)) => {
                this.key = Some(key);
                this.is_submitted = true;
                this.was_zero_copy = is_zero_copy;
                Poll::Pending
            }
            Some((SubmitOutcome::Refused, true)) if !this.has_fallen_back => {
                // A zero-copy submit the driver refused (a full submission queue
                // under resource pressure). Arm the plain fallback and self-wake to
                // retry once on the next poll, when the queue may have drained.
                this.has_fallen_back = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => {
                // No backend, or a submit refused with no retry left; resolve
                // -EINVAL rather than hang on the test-seam / unsupported path.
                Poll::Ready(bytes_from_cqe(-22))
            }
        }
    }
}

impl<B: IoBuf> Drop for SendZcFuture<B> {
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
        operation::FixedBuf,
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
        let Poll::Ready(result) =
            pin!(SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4))).poll(&mut cx)
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
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
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
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        // MORE set with no notification yet: a zero-copy send waiting on its
        // slot-release notification.
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
            "the send stays pending until its notification releases the slot",
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
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
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
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        drop(future);
        assert_eq!(
            inbox.pop().map(|queued| queued.slot),
            Some(key.slot),
            "the drop queued a cancel for the in-flight slot",
        );
    }

    fn poll_seam<B: IoBuf>(
        future: &mut SendZcFuture<B>,
        cx: &mut Context<'_>,
        worker_id: u8,
        slab: &mut InflightBufSlab,
        wake: Option<WakeSlot>,
    ) -> Poll<io::Result<usize>> {
        let seam = IoSeam::new(worker_id, None, Some(NonNull::from(slab)), wake);
        let _guard = SeamGuard::install(&seam);
        core::pin::Pin::new(future).poll(cx)
    }

    #[test]
    fn refusal_falls_back_and_resolves() {
        let binding = poll_binding();
        let wid = binding.worker_id;
        let Ok(mut slab) = InflightBufSlab::new(wid, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(wid, &mut inbox);
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        future.was_zero_copy = true;
        let mut cx = Context::from_waker(Waker::noop());

        // Inject the runtime EINVAL refusal (one CQE, no MORE, no notif). The future
        // frees the slot, resets to unsubmitted, and self-wakes.
        let einval = WakeSlot {
            result: -22,
            flags: 0,
            buf_id: None,
        };
        assert!(
            poll_seam(&mut future, &mut cx, wid, &mut slab, Some(einval)).is_pending(),
            "the refusal frees the slot and self-wakes rather than resolving",
        );
        assert!(
            !future.is_submitted,
            "the refusal reset the future to unsubmitted"
        );
        assert!(future.key.is_none(), "the refusal freed the zero-copy slot");
        assert!(
            future.has_fallen_back,
            "the refusal armed the plain fallback"
        );
        assert!(
            !future.was_zero_copy,
            "the pending resubmit is plain, not zero-copy"
        );

        // Stand in for the plain resubmit the next poll performs: a fresh slab slot
        // marked in-flight as a plain send. The real submit path is exercised by the
        // socket send-zc e2e tests; keeping this slot test-owned avoids mixing a real
        // in-flight op with a synthetic completion.
        let Some(plain_key) = slab.allocate(binding.token) else {
            panic!("the slab allocates the resubmit slot");
        };
        future.key = Some(plain_key);
        future.is_submitted = true;

        // Inject the plain send's completion. The future resolves Ok with the byte
        // count and frees the resubmit slot -- the full round trip.
        let done = WakeSlot {
            result: 4,
            flags: 0,
            buf_id: None,
        };
        let step = poll_seam(&mut future, &mut cx, wid, &mut slab, Some(done));
        let Poll::Ready(Ok(count)) = step else {
            panic!("the plain completion resolves the fallback future with its byte count");
        };
        assert_eq!(
            count, 4,
            "the resolved count is the plain send's byte count"
        );
        assert!(future.key.is_none(), "the resolved future freed its slot");
    }

    #[test]
    fn a_second_einval_after_fallback_surfaces_as_an_error() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        future.was_zero_copy = true;
        // The fallback already fired once; a second -EINVAL must not substitute
        // again but surface as the error.
        future.has_fallen_back = true;
        let wake = WakeSlot {
            result: -22,
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
            panic!("a second -EINVAL after the fallback resolves rather than re-substituting");
        };
        assert_eq!(
            result.err().and_then(|err| err.raw_os_error()),
            Some(22),
            "the second refusal surfaces as the error",
        );
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the terminal error freed the slot",
        );
    }

    #[test]
    fn a_plain_sends_einval_is_not_eligible_for_fallback() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = SendZcFuture::new(5, FixedBuf::new(*b"payload!", 4));
        future.key = Some(key);
        future.is_submitted = true;
        // was_zero_copy stays false: this op went out as a plain send, so
        // its -EINVAL is a genuine error, not a zero-copy refusal to substitute.
        let wake = WakeSlot {
            result: -22,
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
        let polled = core::pin::Pin::new(&mut future).poll(&mut cx);
        let Poll::Ready(result) = polled else {
            panic!("a plain send's -EINVAL resolves rather than falling back");
        };
        assert_eq!(
            result.err().and_then(|err| err.raw_os_error()),
            Some(22),
            "the plain send's refusal surfaces as the error",
        );
        assert!(
            !future.has_fallen_back,
            "a plain send is not eligible for the zero-copy fallback",
        );
    }
}
