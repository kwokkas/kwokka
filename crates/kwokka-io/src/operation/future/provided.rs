//! Provided-buffer completion future for socket receives.
//!
//! [`ProvidedRecvFuture`] submits a single-shot recv whose buffer the kernel
//! selects from the worker's registered provided-buffer ring, so no per-op
//! userspace buffer is pinned while the op waits. The completion names the
//! chosen buffer, and the future resolves into a
//! [`ProvidedBuf`] -- a borrowed view over the
//! pool's bytes that recycles the buffer on drop. No byte is copied between
//! the kernel write and the caller's read.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use crate::{
    boundary::{self, IoSeam, WakeSlot},
    buffer::ring::pool::ProvidedBuf,
    operation::SubmitResult,
};

/// A future that receives from socket `fd` into a kernel-selected provided
/// buffer.
///
/// The first poll submits a provided-buffer recv -- addressed by the polling
/// task's identity token for the `user_data` round trip -- and yields
/// `Pending`. A later poll, woken by the completion drain, resolves the
/// kernel-selected buffer id into a [`ProvidedBuf`] borrowing the worker
/// pool's bytes: a short read yields a short view, and end of stream yields an
/// empty one. A negative completion maps to the corresponding [`io::Error`];
/// a backend with no provided-buffer group resolves
/// [`io::ErrorKind::Unsupported`], the caller's signal to fall back to the
/// inline-buffer recv path.
///
/// The pool owns the bytes, not this future, so dropping it before the
/// completion arrives is safe: the drop queues a cancel, and if the op
/// completed with a buffer anyway, the completion drain recycles that buffer
/// rather than leaking it. Disposal is tracked by the task's token, so a task
/// that drops an in-flight recv should not issue another provided recv until
/// the dropped op has settled -- two ops sharing one token cannot be told
/// apart by the drain.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the `user_data`
/// round trip decodes the polling task from the waker, so await it
/// directly.
#[must_use = "futures do nothing unless polled"]
pub struct ProvidedRecvFuture {
    /// Source socket file descriptor.
    fd: i32,
    /// The worker and token the recv submitted under, once in flight. `Some`
    /// gates the cancel on drop; cleared when the op resolves, so a completed
    /// recv is never cancelled.
    submitted: Option<ProvidedRecvOp>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

/// The worker and `user_data` token a submitted provided recv is cancelled by.
#[derive(Clone, Copy)]
struct ProvidedRecvOp {
    worker_id: u8,
    token: u64,
}

impl ProvidedRecvFuture {
    /// Constructs a provided-buffer recv future for socket `fd`.
    pub const fn new(fd: i32) -> Self {
        Self {
            fd,
            submitted: None,
            is_submitted: false,
        }
    }
}

impl Future for ProvidedRecvFuture {
    type Output = io::Result<ProvidedBuf>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The polling task's identity is encoded in its waker; the poll
        // boundary decoder rejects a waker the runtime did not build, the
        // same contract the other socket futures hold.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("ProvidedRecvFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            // Unreachable in correct use: a successful submit sets both
            // fields together, and the completion clears `submitted` only
            // with `Ready`. This guards a poll after `Ready` (caller misuse).
            let Some(op) = this.submitted else {
                return Poll::Ready(Err(io::Error::from_raw_os_error(22)));
            };
            return match IoSeam::with_current(op.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => {
                    this.submitted = None;
                    Poll::Ready(resolve(op.worker_id, slot))
                }
                _ => Poll::Pending,
            };
        }
        let fd = this.fd;
        let submitted = IoSeam::with_current(binding.worker_id, |seam| {
            seam.submit_provided_recv(fd, binding.token)
        });
        match submitted {
            Some(Some(SubmitResult::Submitted(_))) => {
                this.submitted = Some(ProvidedRecvOp {
                    worker_id: binding.worker_id,
                    token: binding.token,
                });
                this.is_submitted = true;
                Poll::Pending
            }
            // No provided-buffer group on this backend: the caller falls back
            // to the inline-buffer recv path (fallback parity).
            Some(Some(SubmitResult::Unsupported)) => {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::Unsupported)))
            }
            // No seam, no driver, or the backend rejected the op. The
            // production path runs on a real driver, so this is the test-seam
            // / refused path; resolve with -EINVAL rather than hang.
            _ => Poll::Ready(Err(io::Error::from_raw_os_error(22))),
        }
    }
}

impl Drop for ProvidedRecvFuture {
    fn drop(&mut self) {
        if let Some(op) = self.submitted {
            // The submitted recv made the task `io_bound`, so this drop runs
            // on the owning worker and the cancel reaches the inbox
            // single-writer.
            boundary::push_provided_recv_cancel_for_worker(op.worker_id, op.token);
        }
    }
}

/// Resolves a provided recv's completion into the buffer view it names.
///
/// A negative result is the mapped `-errno`. A nonnegative result normally
/// carries the kernel-selected buffer id (`io_uring_prep_recv.3`: a
/// `BUFFER_SELECT` recv reports the chosen buffer in the CQE flags); end of
/// stream may complete without consuming a buffer, which resolves into the
/// empty view. Data without a buffer id cannot name its bytes -- a
/// driver-plumbing fault surfaced as [`io::ErrorKind::InvalidData`] rather
/// than a panic.
fn resolve(worker_id: u8, slot: WakeSlot) -> io::Result<ProvidedBuf> {
    if slot.result < 0 {
        return Err(io::Error::from_raw_os_error(-slot.result));
    }
    let len = u32::try_from(slot.result).unwrap_or(0);
    match slot.buf_id {
        Some(buf_id) => Ok(ProvidedBuf::new(
            worker_id,
            boundary::provided_pool_epoch(worker_id),
            buf_id,
            len,
        )),
        None if slot.result == 0 => Ok(ProvidedBuf::empty()),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provided recv completed with data but no kernel-selected buffer",
        )),
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::Future,
        pin::pin,
        ptr::NonNull,
        task::{Context, Poll},
    };
    use std::task::Waker;

    use crate::{
        boundary::{
            CANCEL_INBOX_CAPACITY, CancelInbox, CancelInboxGuard, IoSeam,
            PROVIDED_RECV_CANCEL_SLOT, ProvidedPoolGuard, SeamGuard, WakeSlot, WakerBinding,
            WakerDecoder, decode_waker, register_decoder,
        },
        buffer::{ring::pool::BufRingPool, slot::BufGroupId},
        operation::future::provided::ProvidedRecvFuture,
    };

    fn stub(waker: &Waker) -> Option<WakerBinding> {
        waker.will_wake(Waker::noop()).then_some(WakerBinding {
            token: 7,
            worker_id: 3,
        })
    }

    static STUB: WakerDecoder = stub;

    // Registers the seam decoder and returns the binding the future decodes;
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
    fn first_poll_without_driver_resolves_err() {
        let binding = poll_binding();
        let seam = IoSeam::new(binding.worker_id, None, None, None);
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(ProvidedRecvFuture::new(5)).poll(&mut cx) else {
            panic!("a driverless seam resolves the recv immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backend_without_group_resolves_unsupported() {
        let binding = poll_binding();
        let mut driver = crate::DriverType::Epoll(());
        let seam = IoSeam::new(
            binding.worker_id,
            Some(NonNull::from(&mut driver)),
            None,
            None,
        );
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(ProvidedRecvFuture::new(5)).poll(&mut cx) else {
            panic!("a groupless backend resolves the recv immediately");
        };
        let Err(error) = result else {
            panic!("a groupless backend cannot receive into a provided buffer");
        };
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::Unsupported,
            "the fallback signal is the Unsupported error kind",
        );
    }

    #[test]
    fn completion_resolves_the_selected_buffer() {
        let binding = poll_binding();
        let Ok(pool) = BufRingPool::new(4, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let _pool_guard = ProvidedPoolGuard::install_pool(binding.worker_id, Some(&pool));
        let mut future = ProvidedRecvFuture::new(5);
        future.submitted = Some(super::ProvidedRecvOp {
            worker_id: binding.worker_id,
            token: binding.token,
        });
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 4,
            flags: 0,
            buf_id: Some(2),
        };
        let seam = IoSeam::new(binding.worker_id, None, None, Some(wake));
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        let Ok(view) = result else {
            panic!("a buffer-carrying completion resolves into a view");
        };
        assert_eq!(view.len(), 4, "the view spans the kernel-confirmed count");
        assert_eq!(&view[..], &[0u8; 4], "fresh pool storage reads as zeros");
    }

    #[test]
    fn end_of_stream_without_buffer_resolves_empty() {
        let binding = poll_binding();
        let mut future = ProvidedRecvFuture::new(5);
        future.submitted = Some(super::ProvidedRecvOp {
            worker_id: binding.worker_id,
            token: binding.token,
        });
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 0,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(binding.worker_id, None, None, Some(wake));
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        let Ok(view) = result else {
            panic!("end of stream resolves into the empty view");
        };
        assert!(view.is_empty(), "no buffer was consumed at end of stream");
    }

    #[test]
    fn data_without_buffer_is_invalid_data() {
        let binding = poll_binding();
        let mut future = ProvidedRecvFuture::new(5);
        future.submitted = Some(super::ProvidedRecvOp {
            worker_id: binding.worker_id,
            token: binding.token,
        });
        future.is_submitted = true;
        let wake = WakeSlot {
            result: 4,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(binding.worker_id, None, None, Some(wake));
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        let Err(error) = result else {
            panic!("data with no buffer id cannot name its bytes");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn negative_completion_maps_to_errno() {
        let binding = poll_binding();
        let mut future = ProvidedRecvFuture::new(5);
        future.submitted = Some(super::ProvidedRecvOp {
            worker_id: binding.worker_id,
            token: binding.token,
        });
        future.is_submitted = true;
        let wake = WakeSlot {
            result: -105,
            flags: 0,
            buf_id: None,
        };
        let seam = IoSeam::new(binding.worker_id, None, None, Some(wake));
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(result) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        let Err(error) = result else {
            panic!("a negative CQE result maps to an error");
        };
        assert_eq!(
            error.raw_os_error(),
            Some(105),
            "-ENOBUFS surfaces as its errno, the caller's re-arm signal",
        );
    }

    #[test]
    fn drop_in_flight_queues_a_provided_cancel() {
        let binding = poll_binding();
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = ProvidedRecvFuture::new(5);
        future.submitted = Some(super::ProvidedRecvOp {
            worker_id: binding.worker_id,
            token: binding.token,
        });
        future.is_submitted = true;
        drop(future);
        let Some(cancelled) = inbox.pop() else {
            panic!("dropping an in-flight future queues a cancel");
        };
        assert_eq!(cancelled.slot, PROVIDED_RECV_CANCEL_SLOT);
        assert_eq!(cancelled.op_token, binding.token);
        assert_eq!(cancelled.worker_id, binding.worker_id);
    }
}
