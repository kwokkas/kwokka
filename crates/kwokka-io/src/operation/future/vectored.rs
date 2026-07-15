//! In-flight-slot completion futures for vectored file operations.
//!
//! [`VectoredWriteFuture`] and [`VectoredReadFuture`] are the `writev` /
//! `readv` counterparts of [`FileWriteFuture`](crate::operation::FileWriteFuture)
//! / [`FileReadFuture`](crate::operation::FileReadFuture): they keep the caller's
//! `N` buffers in the worker's in-flight registry and let the seam stage the
//! `iovec` array and the gathered or scattered payload in one slot. The futures
//! never touch the slot directly -- the seam gathers the buffers on submit and
//! scatters them back on completion, so the future only moves the resulting
//! array pointer into the submit and reads the result on a later poll. The slot,
//! not the caller's buffers, is what the kernel touches, so an early drop queues
//! a cancel the worker reclaims once the op completes.
//!
//! The inline slot bounds the total: a `writev` whose buffers together exceed
//! the slot payload is refused rather than partially gathered, and a `readv`
//! offers the kernel at most the slot payload, so a larger destination reads
//! short and the caller reads again. A zero-copy path for larger transfers is a
//! later addition.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use crate::{
    boundary::{self, IoSeam},
    buffer::oneshot::inflight::InflightSlotKey,
    operation::{
        IoBuf, IoBufMut, IoRequest, SubmitResult,
        core::vectored::{IoVec, IoVecMut},
        future::bytes_from_cqe,
    },
};

/// A future that writes `N` owned buffers' initialized bytes to `fd` at
/// `offset` in one `writev`.
///
/// The first poll allocates a slot in the worker's in-flight registry, has the
/// seam gather every buffer's initialized bytes into it and lay out the `iovec`
/// array -- addressed by the polling task's identity token for the `user_data`
/// round trip -- and yields `Pending`. A later poll, woken by the completion
/// drain, returns an [`io::Result`] byte count alongside the buffers, which move
/// out with the `Ready` value.
///
/// The kernel reads the slot copy, not the caller's buffers, so dropping the
/// future before the completion arrives is safe: the drop queues a cancel for
/// the in-flight op and the slot is freed only once the kernel signals the op is
/// done. Buffers whose initialized bytes together exceed the slot payload
/// capacity resolve immediately as an unsupported submit rather than gathering
/// only some of them.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker: the
/// `user_data` round trip decodes the polling task from the waker, so await it
/// directly. Also panics when polled again after resolving: the buffers move out
/// with the `Ready` value, so a repeat poll has nothing left to return.
#[must_use = "futures do nothing unless polled"]
pub struct VectoredWriteFuture<B: IoBuf, const N: usize> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the write starts at.
    offset: u64,
    /// The caller's source buffers. `Some` from construction until they move out
    /// with the `Ready` value.
    iov: Option<IoVec<B, N>>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBuf, const N: usize> VectoredWriteFuture<B, N> {
    /// Constructs a vectored write future for `fd` at byte `offset` over `iov`.
    pub const fn new(fd: i32, offset: u64, iov: IoVec<B, N>) -> Self {
        Self {
            fd,
            offset,
            iov: Some(iov),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBuf, const N: usize> Future for VectoredWriteFuture<B, N> {
    type Output = (io::Result<usize>, [B; N]);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("VectoredWriteFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("VectoredWriteFuture polled after resolving; await it only once");
            };
            let outcome = IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result().map(|slot| {
                    seam.free_slot(key);
                    bytes_from_cqe(slot.result)
                })
            });
            return if let Some(Some(result)) = outcome {
                this.key = None;
                let Some(iov) = this.iov.take() else {
                    panic!("VectoredWriteFuture polled after resolving; await it only once");
                };
                Poll::Ready((result, iov.into_bufs()))
            } else {
                Poll::Pending
            };
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let Some(iov) = this.iov.as_ref() else {
            panic!("VectoredWriteFuture polled after resolving; await it only once");
        };
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, _) = seam.allocate_slot(token)?;
                let Some((iovec, count)) = seam.build_writev(key, iov) else {
                    // No slab, or the buffers exceed the slot capacity; return
                    // the slot rather than gather only some of them.
                    seam.free_slot(key);
                    return None;
                };
                let request =
                    IoRequest::writev_prepared(fd, iovec, count, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit_internal(request) {
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
            let Some(iov) = this.iov.take() else {
                panic!("VectoredWriteFuture polled after resolving; await it only once");
            };
            // No seam, no slab, oversized, or the submit failed; resolve with
            // -EINVAL rather than hang on the test-seam / unsupported path.
            Poll::Ready((bytes_from_cqe(-22), iov.into_bufs()))
        }
    }
}

impl<B: IoBuf, const N: usize> Drop for VectoredWriteFuture<B, N> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

/// A future that reads from `fd` at `offset` into `N` owned buffers in one
/// `readv`.
///
/// The first poll allocates a slot in the worker's in-flight registry, has the
/// seam lay out an `iovec` per buffer over it -- addressed by the polling task's
/// identity token for the `user_data` round trip -- and yields `Pending`. A
/// later poll, woken by the completion drain, scatters the received bytes across
/// the buffers, records each buffer's filled length, and returns an
/// [`io::Result`] byte count alongside the buffers, which move out with the
/// `Ready` value.
///
/// The kernel writes the slot, not the caller's buffers, so dropping the future
/// before the completion arrives is safe: the drop queues a cancel for the
/// in-flight op and the slot is freed only once the kernel signals the op is
/// done. The buffers together offer the kernel at most the slot payload
/// capacity, so a larger destination reads short rather than the op failing --
/// the caller reads again for the rest.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker: the
/// `user_data` round trip decodes the polling task from the waker, so await it
/// directly. Also panics when polled again after resolving: the buffers move out
/// with the `Ready` value, so a repeat poll has nothing left to return.
#[must_use = "futures do nothing unless polled"]
pub struct VectoredReadFuture<B: IoBufMut, const N: usize> {
    /// Target file descriptor.
    fd: i32,
    /// Byte offset the read starts at.
    offset: u64,
    /// The caller's destination buffers. `Some` from construction until they
    /// move out with the `Ready` value.
    iov: Option<IoVecMut<B, N>>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBufMut, const N: usize> VectoredReadFuture<B, N> {
    /// Constructs a vectored read future for `fd` at byte `offset` into `iov`.
    pub const fn new(fd: i32, offset: u64, iov: IoVecMut<B, N>) -> Self {
        Self {
            fd,
            offset,
            iov: Some(iov),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBufMut, const N: usize> Future for VectoredReadFuture<B, N> {
    type Output = (io::Result<usize>, [B; N]);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("VectoredReadFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("VectoredReadFuture polled after resolving; await it only once");
            };
            let Some(mut iov) = this.iov.take() else {
                panic!("VectoredReadFuture polled after resolving; await it only once");
            };
            let outcome = IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result()
                    .map(|slot| match bytes_from_cqe(slot.result) {
                        Ok(count) => {
                            seam.harvest_vectored(key, count, &mut iov);
                            Ok(count)
                        }
                        Err(error) => {
                            seam.free_slot(key);
                            Err(error)
                        }
                    })
            });
            return if let Some(Some(result)) = outcome {
                this.key = None;
                Poll::Ready((result, iov.into_bufs()))
            } else {
                this.iov = Some(iov);
                Poll::Pending
            };
        }
        let fd = this.fd;
        let offset = this.offset;
        let token = binding.token;
        let Some(iov) = this.iov.as_ref() else {
            panic!("VectoredReadFuture polled after resolving; await it only once");
        };
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, _) = seam.allocate_slot(token)?;
                let Some((iovec, count)) = seam.build_readv(key, iov) else {
                    seam.free_slot(key);
                    return None;
                };
                let request =
                    IoRequest::readv_prepared(fd, iovec, count, offset).with_user_data(token);
                if let Some(SubmitResult::Submitted(_)) = seam.submit_internal(request) {
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
            let Some(iov) = this.iov.take() else {
                panic!("VectoredReadFuture polled after resolving; await it only once");
            };
            // No seam, no slab, or the submit failed; resolve with -EINVAL
            // rather than hang on the test-seam / unsupported path.
            Poll::Ready((bytes_from_cqe(-22), iov.into_bufs()))
        }
    }
}

impl<B: IoBufMut, const N: usize> Drop for VectoredReadFuture<B, N> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{pin::pin, ptr::NonNull, task::Context};

    use super::*;
    use crate::{
        boundary::{
            CANCEL_INBOX_CAPACITY, CancelInbox, CancelInboxGuard, IoSeam, SeamGuard, TEST_DECODER,
            WakeSlot, WakerBinding, decode_waker, register_decoder, reserve_worker_id, test_waker,
        },
        buffer::oneshot::inflight::InflightBufSlab,
        operation::FixedBuf,
    };

    fn poll_binding() -> WakerBinding {
        register_decoder(&TEST_DECODER);
        let waker = test_waker(reserve_worker_id());
        let Some(binding) = decode_waker(&waker) else {
            panic!("a registered decoder yields a binding");
        };
        binding
    }

    fn sources() -> IoVec<FixedBuf<8>, 2> {
        IoVec::new([
            FixedBuf::new(*b"ab......", 2),
            FixedBuf::new(*b"cd......", 2),
        ])
    }

    #[test]
    fn writev_first_poll_builds_then_frees_without_driver() {
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
        let waker = test_waker(binding.worker_id);
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready((result, _bufs)) =
            pin!(VectoredWriteFuture::new(5, 0, sources())).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the writev immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    fn readv_first_poll_builds_then_frees_without_driver() {
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
        let waker = test_waker(binding.worker_id);
        let mut cx = Context::from_waker(&waker);
        let dst = IoVecMut::new([[0u8; 8], [0u8; 8]]);
        let Poll::Ready((result, _bufs)) = pin!(VectoredReadFuture::new(5, 0, dst)).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the readv immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    fn writev_completion_returns_bytes_and_frees_the_slot() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = VectoredWriteFuture::new(5, 0, sources());
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
        let waker = test_waker(binding.worker_id);
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready((result, _bufs)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the writev");
        };
        assert_eq!(result.ok(), Some(4), "the CQE byte count surfaces");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the completion freed the slot",
        );
    }

    #[test]
    fn readv_completion_scatters_bytes_into_the_buffers() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let seam = IoSeam::new(
            binding.worker_id,
            None,
            Some(NonNull::from(&mut slab)),
            Some(WakeSlot {
                result: 4,
                flags: 0,
                buf_id: None,
            }),
        );
        // Stage the slot payload as a landed readv: gather "abcd" into the same
        // two-entry layout the readv future harvests back out.
        assert!(
            seam.build_writev(key, &sources()).is_some(),
            "staging the slot succeeds",
        );
        let mut future = VectoredReadFuture::new(5, 0, IoVecMut::new([[0u8; 8], [0u8; 8]]));
        future.key = Some(key);
        future.is_submitted = true;
        let _guard = SeamGuard::install(&seam);
        let waker = test_waker(binding.worker_id);
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready((result, bufs)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the readv");
        };
        assert_eq!(result.ok(), Some(4), "the CQE byte count surfaces");
        // Four bytes into two eight-capacity buffers: the first takes all four.
        assert_eq!(
            &bufs[0][..4],
            b"abcd",
            "the payload scatters into the first buffer"
        );
    }

    #[test]
    fn writev_drop_queues_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = VectoredWriteFuture::new(5, 0, sources());
        future.key = Some(key);
        future.is_submitted = true;
        drop(future);
        let Some(cancelled) = inbox.pop() else {
            panic!("dropping an in-flight future queues a cancel");
        };
        assert_eq!(
            cancelled.op_token, key.op_token,
            "the cancel carries the slot's token"
        );
    }
}
