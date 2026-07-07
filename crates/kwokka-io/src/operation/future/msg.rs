//! In-flight-slot completion futures for datagram message operations.
//!
//! [`SendMsgFuture`] and [`RecvMsgFuture`] are the `sendmsg` / `recvmsg`
//! counterparts of the socket [`RecvFuture`](crate::operation::RecvFuture) /
//! [`SendFuture`](crate::operation::SendFuture): they keep the caller's buffer in
//! the worker's in-flight registry and let the seam stage the `msghdr`, its
//! `iovec`, and the peer or sender address contiguously in one slot. Unlike the
//! plain socket futures, the futures here never touch the slot directly -- the
//! seam builds the header and copies the payload, so the future only moves the
//! resulting `msghdr` pointer into the submit and reads the result back on a
//! later poll. The slot, not the caller's buffer, is what the kernel touches,
//! so an early drop queues a cancel the worker reclaims once the op completes.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;

use crate::{
    addr::SockAddr,
    boundary::{self, IoSeam},
    buffer::inflight::InflightSlotKey,
    operation::{
        IoBuf, IoBufMut, IoRequest, SubmitResult, core::msghdr::MAX_MSG_INLINE_CAP,
        future::bytes_from_cqe,
    },
};

/// A future that sends an owned buffer's initialized bytes as a datagram to
/// `addr` over socket `fd`.
///
/// The first poll allocates a slot in the worker's in-flight registry, has the
/// seam pack `addr` and copy the buffer's initialized bytes into it and build
/// the `sendmsg` header -- addressed by the polling task's identity token for
/// the `user_data` round trip -- and yields `Pending`. A later poll, woken by
/// the completion drain, returns an [`io::Result`] byte count: the bytes sent,
/// or the mapped [`io::Error`]. The buffer is a send source the caller does not
/// read back, so the future keeps it until the op resolves and drops it.
///
/// The kernel reads the slot copy, not the caller's buffer, so dropping the
/// future before the completion arrives is safe: the drop queues a cancel for
/// the in-flight op and the slot is freed only once the kernel signals the op
/// is done. A buffer whose initialized length exceeds the slot payload capacity
/// resolves immediately as an unsupported submit rather than truncating the
/// datagram.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker: the
/// `user_data` round trip decodes the polling task from the waker, so await it
/// directly. Also panics when polled again after resolving: the slot key clears
/// on `Ready`, so a repeat poll has no in-flight op left to observe.
#[must_use = "futures do nothing unless polled"]
pub struct SendMsgFuture<B: IoBuf> {
    /// Target socket file descriptor.
    fd: i32,
    /// Destination datagram address, packed into the slot on submit.
    addr: SockAddr,
    /// The caller's source buffer, held for the op lifetime. The kernel reads a
    /// slot copy, not this buffer, so it drops with the future.
    buf: Option<B>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBuf> SendMsgFuture<B> {
    /// Constructs a sendmsg future for socket `fd` to `addr` over `buf`.
    pub const fn new(fd: i32, addr: SockAddr, buf: B) -> Self {
        Self {
            fd,
            addr,
            buf: Some(buf),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBuf> Future for SendMsgFuture<B> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("SendMsgFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("SendMsgFuture polled after resolving; await it only once");
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
        let token = binding.token;
        let Some(buf) = this.buf.as_ref() else {
            panic!("SendMsgFuture polled after resolving; await it only once");
        };
        let addr = &this.addr;
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, _) = seam.allocate_slot(token)?;
                let Some(msghdr) = seam.build_send_msg(key, buf, addr) else {
                    // No slab, or the payload exceeds the slot capacity; return
                    // the slot rather than truncate the datagram.
                    seam.free_slot(key);
                    return None;
                };
                let request = IoRequest::sendmsg_prepared(fd, msghdr).with_user_data(token);
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
            // No seam, no slab, oversized, or the submit failed; resolve with
            // -EINVAL rather than hang on the test-seam / unsupported path.
            // Clear the buffer so a repeat poll hits the documented panic
            // instead of re-attempting the submit and double-sending.
            this.buf = None;
            Poll::Ready(bytes_from_cqe(-22))
        }
    }
}

impl<B: IoBuf> Drop for SendMsgFuture<B> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

/// A future that receives a datagram from socket `fd` into an owned buffer,
/// returning the sender address alongside the byte count.
///
/// The first poll allocates a slot in the worker's in-flight registry, has the
/// seam build the `recvmsg` header over it -- addressed by the polling task's
/// identity token for the `user_data` round trip -- and yields `Pending`. A
/// later poll, woken by the completion drain, reads the kernel-written sender
/// address, copies the slot payload into the caller's buffer, and returns an
/// [`io::Result`] byte count paired with the sender ([`None`] on error or an
/// unparsed family, [`Some`] on a valid datagram including a zero-length one)
/// and the buffer, which moves out with the future on `Ready`.
///
/// The slot bytes are owned by the worker's registry, so dropping the future
/// before the completion arrives is safe: the drop queues a cancel and the slot
/// is freed only once the kernel signals the op is done. A buffer whose
/// [`capacity`][IoBufMut::capacity] exceeds the slot payload capacity resolves
/// immediately as an unsupported submit: offering the kernel less than the
/// buffer's declared capacity would truncate the datagram with no way to
/// recover the dropped bytes.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker: the
/// `user_data` round trip decodes the polling task from the waker, so await it
/// directly. Also panics when polled again after resolving: the buffer moves
/// out with the `Ready` value, so a repeat poll has nothing left to return.
#[must_use = "futures do nothing unless polled"]
pub struct RecvMsgFuture<B: IoBufMut> {
    /// Source socket file descriptor.
    fd: i32,
    /// The caller's destination buffer. `Some` from construction until it moves
    /// out with the `Ready` value.
    buf: Option<B>,
    /// In-flight slot handle once submitted; `None` before submit and after the
    /// completion frees the slot. A `Some` value at drop queues a cancel.
    key: Option<InflightSlotKey>,
    /// Whether the op has been submitted; gates the submit-once transition.
    is_submitted: bool,
}

impl<B: IoBufMut> RecvMsgFuture<B> {
    /// Constructs a recvmsg future for socket `fd` into `buf`.
    pub const fn new(fd: i32, buf: B) -> Self {
        Self {
            fd,
            buf: Some(buf),
            key: None,
            is_submitted: false,
        }
    }
}

impl<B: IoBufMut> Future for RecvMsgFuture<B> {
    type Output = (io::Result<usize>, Option<SockAddr>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("RecvMsgFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        if this.is_submitted {
            let Some(key) = this.key else {
                panic!("RecvMsgFuture polled after resolving; await it only once");
            };
            let Some(mut buf) = this.buf.take() else {
                panic!("RecvMsgFuture polled after resolving; await it only once");
            };
            let outcome = IoSeam::with_current(binding.worker_id, |seam| {
                seam.completion_result()
                    .map(|slot| match bytes_from_cqe(slot.result) {
                        // Read the sender before the harvest, which frees the slot
                        // out from under it.
                        Ok(count) => {
                            let sender = seam.read_msg_sender(key);
                            seam.harvest_msg_payload(key, count, &mut buf);
                            (Ok(count), sender)
                        }
                        Err(error) => {
                            seam.free_slot(key);
                            (Err(error), None)
                        }
                    })
            });
            return if let Some(Some((result, sender))) = outcome {
                this.key = None;
                Poll::Ready((result, sender, buf))
            } else {
                this.buf = Some(buf);
                Poll::Pending
            };
        }
        let Some(cap) = this.buf.as_ref().map(IoBufMut::capacity) else {
            panic!("RecvMsgFuture polled after resolving; await it only once");
        };
        if cap > MAX_MSG_INLINE_CAP {
            let Some(buf) = this.buf.take() else {
                panic!("RecvMsgFuture polled after resolving; await it only once");
            };
            // A datagram recv cannot exceed the slot payload capacity; offering
            // the kernel less would truncate the datagram (dropping the excess
            // with no follow-up read to recover it), so reject rather than
            // truncate the caller's declared capacity.
            return Poll::Ready((bytes_from_cqe(-22), None, buf));
        }
        let fd = this.fd;
        let token = binding.token;
        let submitted =
            IoSeam::with_current(binding.worker_id, |seam| -> Option<InflightSlotKey> {
                let (key, _) = seam.allocate_slot(token)?;
                let Some(msghdr) = seam.build_recv_msg(key, cap) else {
                    seam.free_slot(key);
                    return None;
                };
                let request = IoRequest::recvmsg_prepared(fd, msghdr).with_user_data(token);
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
            let Some(buf) = this.buf.take() else {
                panic!("RecvMsgFuture polled after resolving; await it only once");
            };
            // No seam, no slab, or the submit failed; resolve with -EINVAL
            // rather than hang on the test-seam / unsupported path.
            Poll::Ready((bytes_from_cqe(-22), None, buf))
        }
    }
}

impl<B: IoBufMut> Drop for RecvMsgFuture<B> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            boundary::push_cancel_for_worker(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{pin::pin, ptr::NonNull, task::Context};
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        task::Waker,
    };

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

    fn peer() -> SockAddr {
        SockAddr::V4(SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 9), 7777))
    }

    #[test]
    fn send_msg_first_poll_builds_then_frees_without_driver() {
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
        let Poll::Ready(result) = pin!(SendMsgFuture::new(
            5,
            peer(),
            FixedBuf::new(*b"payload!", 4)
        ))
        .poll(&mut cx) else {
            panic!("a driverless seam resolves the send immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        let Some((key, _)) = seam.allocate_slot(0) else {
            panic!("the freed slot reallocates");
        };
        assert_eq!(key.slot, 0, "the first poll returned the slot it allocated");
    }

    #[test]
    #[should_panic(expected = "SendMsgFuture polled after resolving")]
    fn send_msg_fuses_after_a_failed_submit() {
        let binding = poll_binding();
        let seam = IoSeam::new(binding.worker_id, None, None, None);
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut future = pin!(SendMsgFuture::new(
            5,
            peer(),
            FixedBuf::new(*b"payload!", 4)
        ));
        let Poll::Ready(result) = future.as_mut().poll(&mut cx) else {
            panic!("a driverless seam resolves the send immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        // The failed submit fused the future; a repeat poll panics instead of
        // silently re-submitting and double-sending the datagram.
        drop(future.as_mut().poll(&mut cx));
    }

    #[test]
    fn recv_msg_first_poll_builds_then_frees_without_driver() {
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
        let Poll::Ready((result, sender, _)) = pin!(RecvMsgFuture::new(5, [0u8; 8])).poll(&mut cx)
        else {
            panic!("a driverless seam resolves the recv immediately");
        };
        assert!(result.is_err(), "the refused submit maps to an error");
        assert_eq!(sender, None, "no sender on the refused path");
    }

    #[test]
    fn recv_msg_rejects_buffer_over_payload_cap() {
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
        let big = [0u8; MAX_MSG_INLINE_CAP + 1];
        let Poll::Ready((result, _, _)) = pin!(RecvMsgFuture::new(5, big)).poll(&mut cx) else {
            panic!("an oversized buffer resolves immediately");
        };
        assert!(
            result.is_err(),
            "a buffer past the payload capacity is rejected, not truncated",
        );
    }

    #[test]
    fn send_msg_completion_returns_bytes() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = SendMsgFuture::new(5, peer(), FixedBuf::new(*b"payload!", 4));
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
            panic!("a captured completion resolves the send");
        };
        assert_eq!(result.ok(), Some(4));
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the completion freed the slot",
        );
    }

    #[test]
    fn recv_msg_completion_harvests_and_reads_sender() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
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
        let sender = peer();
        // Stage the slot as a landed recvmsg: `build_send_msg` packs the sender
        // into the addr region and the payload into the payload region, the
        // exact bytes a completed recvmsg leaves behind.
        assert!(
            seam.build_send_msg(key, &FixedBuf::new(*b"data", 4), &sender)
                .is_some(),
            "staging the slot succeeds",
        );
        let mut future = RecvMsgFuture::new(5, [0u8; 8]);
        future.key = Some(key);
        future.is_submitted = true;
        let _guard = SeamGuard::install(&seam);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready((result, got_sender, out)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        assert_eq!(result.ok(), Some(4));
        assert_eq!(got_sender, Some(sender), "the sender address round-trips");
        assert_eq!(&out[..4], b"data", "the payload copies out of the slot");
    }

    #[test]
    fn recv_msg_completion_frees_slot_on_error() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut future = RecvMsgFuture::new(5, [0u8; 8]);
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
        let Poll::Ready((result, sender, _)) = pin!(future).poll(&mut cx) else {
            panic!("a captured completion resolves the recv");
        };
        assert!(result.is_err(), "a negative CQE result maps to an error");
        assert_eq!(sender, None, "no sender on the error path");
        assert!(
            seam.allocate_slot(0)
                .is_some_and(|(reused, _)| reused.slot == key.slot),
            "the error path freed the slot without harvesting",
        );
    }

    #[test]
    fn send_msg_drop_queues_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = SendMsgFuture::new(5, peer(), FixedBuf::new(*b"payload!", 4));
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
    fn recv_msg_drop_queues_cancel_when_in_flight() {
        let binding = poll_binding();
        let Ok(mut slab) = InflightBufSlab::new(binding.worker_id, 8) else {
            panic!("mmap must succeed for the test slab");
        };
        let Some(key) = slab.allocate(binding.token) else {
            panic!("the slab allocates a slot");
        };
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        let _inbox_guard = CancelInboxGuard::install(binding.worker_id, &mut inbox);
        let mut future = RecvMsgFuture::new(5, [0u8; 8]);
        future.key = Some(key);
        future.is_submitted = true;
        drop(future);
        let Some(cancelled) = inbox.pop() else {
            panic!("dropping an in-flight future queues a cancel");
        };
        assert_eq!(cancelled.slot, key.slot);
        assert_eq!(cancelled.op_token, binding.token);
    }
}
