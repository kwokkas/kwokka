//! [`IoRequest`] to `io_uring` SQE conversion.
//!
//! Group A, Group B, and driver-internal ops map to the corresponding
//! `io_uring::opcode` struct. The resulting [`Entry`] carries `user_data`
//! and SQE flags derived from [`CommonFields`] and [`OpFlags`].
//!
//! [`IoRequest`]: crate::operation::IoRequest
//! [`Entry`]: io_uring::squeue::Entry
//! [`CommonFields`]: crate::operation::CommonFields
//! [`OpFlags`]: crate::operation::OpFlags

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
#![allow(
    clippy::cast_sign_loss,
    reason = "fd-to-registered-index casts (i32 -> u32) are inherent to the Fixed(fd) io_uring ABI"
)]

use io_uring::squeue::Entry;

use crate::{
    operation::{
        CommonFields, ControlPayload, IoBuf, IoBufMut, IoRequest, OpCode, OpFlags, OpPayload,
    },
    uring::{
        opcode::{control, io, net, sync},
        setup::flags::sqe_flags,
    },
};

/// Scratch buffers owned by the caller for SQE fields that must
/// outlive the [`Entry`] until submission.
pub(crate) struct SubmitScratch {
    /// Packed socket address for connect ops.
    pub addr: [u8; 128],
    /// Timespec for timeout ops.
    pub timespec: io_uring::types::Timespec,
}

impl SubmitScratch {
    /// Zero-initialized scratch.
    pub(crate) const fn new() -> Self {
        Self {
            addr: [0u8; 128],
            timespec: io_uring::types::Timespec::new(),
        }
    }
}

/// Build an `io_uring` SQE from an [`IoRequest`] with no buffer.
///
/// Handles accept, connect, close, Group B socket ops, and all
/// driver-internal ops (timeout, cancel, `msg_ring`, poll). The
/// caller must keep `scratch` alive until the SQE is submitted
/// to the ring.
///
/// # Panics
///
/// Panics on an unsupported `(opcode, payload)` combination.
pub(crate) fn build_entry(request: &IoRequest<()>, scratch: &mut SubmitScratch) -> Entry {
    let entry = match (&request.opcode, &request.payload) {
        (OpCode::Accept, OpPayload::Fd) => net::build_accept(request.fd, request.flags),
        (OpCode::RecvProvided, OpPayload::Fd) => {
            net::build_recv_provided(request.fd, request.common.buf_group, request.flags)
        }
        (OpCode::Connect, OpPayload::Socket { addr, .. }) => {
            net::build_connect(request.fd, addr, &mut scratch.addr, request.flags)
        }
        (OpCode::Close, OpPayload::Fd) => sync::build_close(request.fd),
        (OpCode::Timeout, OpPayload::Control(ControlPayload::Timeout { duration_ns })) => {
            control::build_timeout(*duration_ns, &mut scratch.timespec)
        }
        (OpCode::Cancel, OpPayload::Control(ControlPayload::Cancel { target })) => {
            control::build_cancel(target.user_data())
        }
        (OpCode::MsgRing, OpPayload::Control(ControlPayload::MsgRing { msg })) => {
            control::build_msg_ring(request.fd, *msg)
        }
        (OpCode::Poll, OpPayload::Control(ControlPayload::PollAdd { events })) => {
            control::build_poll_add(request.fd, *events, request.flags)
        }
        (OpCode::Poll, OpPayload::Control(ControlPayload::PollRemove { token })) => {
            control::build_poll_remove(token.user_data())
        }
        (OpCode::Fsync, OpPayload::Fd) => io::build_fsync(request.fd, request.flags),
        (
            OpCode::Splice,
            OpPayload::Splice {
                fd_out,
                off_in,
                off_out,
                nbytes,
                splice_flags,
            },
        ) => io::build_splice(
            request.fd,
            *off_in,
            *fd_out,
            *off_out,
            *nbytes,
            *splice_flags,
            request.flags,
        ),
        (
            OpCode::Tee,
            OpPayload::Splice {
                fd_out,
                nbytes,
                splice_flags,
                ..
            },
        ) => io::build_tee(request.fd, *fd_out, *nbytes, *splice_flags, request.flags),
        (OpCode::Shutdown, OpPayload::Fd) => net::build_shutdown(request.fd, request.flags),
        (
            OpCode::Socket,
            OpPayload::NewSocket {
                domain,
                socket_type,
                protocol,
            },
        ) => net::build_socket(*domain, *socket_type, *protocol),
        (opcode, _) => panic!("unsupported opcode {opcode:?} in build_entry"),
    };
    apply_common(entry, &request.common, request.flags)
}

/// Build an SQE from a read-side [`IoRequest`].
///
/// Vectored read falls back to nop (pending `IoVec` integration).
///
/// # Panics
///
/// Panics on an unsupported `(opcode, payload)` combination.
pub(crate) fn build_entry_read<B: IoBufMut>(request: &IoRequest<B>) -> Entry {
    let entry = match (&request.opcode, &request.payload) {
        (OpCode::Read, OpPayload::Buffer { .. }) if request.flags.vectored => build_nop(),
        (OpCode::Read, OpPayload::Buffer { buf, offset }) => io::build_read(
            request.fd,
            #[allow(
                clippy::as_ptr_cast_mut,
                clippy::ptr_cast_constness,
                reason = "buf is IoBufMut but borrowed shared through OpPayload; as_mut_ptr needs &mut"
            )]
            buf.as_ptr().cast_mut(),
            buf.capacity(),
            *offset,
            request.flags,
        ),
        (OpCode::Recv, OpPayload::Buffer { buf, .. }) => net::build_recv(
            request.fd,
            #[allow(
                clippy::as_ptr_cast_mut,
                clippy::ptr_cast_constness,
                reason = "buf is IoBufMut but borrowed shared through OpPayload; as_mut_ptr needs &mut"
            )]
            buf.as_ptr().cast_mut(),
            buf.capacity(),
            request.flags,
        ),
        (opcode, _) => panic!("unsupported opcode {opcode:?} in build_entry_read"),
    };
    apply_common(entry, &request.common, request.flags)
}

/// Build an SQE from a write-side [`IoRequest`].
///
/// Vectored write falls back to nop (pending `IoVec` integration).
///
/// # Panics
///
/// Panics on an unsupported `(opcode, payload)` combination.
pub(crate) fn build_entry_write<B: IoBuf>(request: &IoRequest<B>) -> Entry {
    let entry = match (&request.opcode, &request.payload) {
        (OpCode::Write, OpPayload::Buffer { .. }) if request.flags.vectored => build_nop(),
        (OpCode::Write, OpPayload::Buffer { buf, offset }) => io::build_write(
            request.fd,
            buf.as_ptr(),
            buf.bytes_init(),
            *offset,
            request.flags,
        ),
        (OpCode::Send, OpPayload::Buffer { buf, .. }) => {
            net::build_send(request.fd, buf.as_ptr(), buf.bytes_init(), request.flags)
        }
        (opcode, _) => panic!("unsupported opcode {opcode:?} in build_entry_write"),
    };
    apply_common(entry, &request.common, request.flags)
}

fn apply_common(entry: Entry, common: &CommonFields, flags: OpFlags) -> Entry {
    entry.user_data(common.user_data).flags(sqe_flags(flags))
}

fn build_nop() -> Entry {
    io_uring::opcode::Nop::new().build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{IoBuf, IoBufMut, SubmitToken};

    struct MockBuf {
        data: [u8; 64],
        len: usize,
    }

    impl MockBuf {
        fn new(capacity: usize) -> Self {
            Self {
                data: [0u8; 64],
                len: capacity.min(64),
            }
        }

        fn filled(count: usize) -> Self {
            let mut buf = Self {
                data: [0xAB; 64],
                len: count.min(64),
            };
            buf.data[count.min(64)..].fill(0);
            buf
        }
    }

    impl IoBuf for MockBuf {
        fn as_ptr(&self) -> *const u8 {
            self.data.as_ptr()
        }

        fn bytes_init(&self) -> usize {
            self.len
        }
    }

    impl IoBufMut for MockBuf {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.data.as_mut_ptr()
        }

        fn capacity(&self) -> usize {
            self.data.len()
        }

        fn set_init(&mut self, count: usize) {
            self.len = count;
        }
    }

    fn scratch() -> SubmitScratch {
        SubmitScratch::new()
    }

    #[test]
    fn accept_builds_without_panic() {
        let request = IoRequest::<()>::accept(3).with_user_data(42);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn recv_provided_builds_without_panic() {
        let request = IoRequest::<()>::recv_provided(3, crate::buffer::slot::BufGroupId::new(0));
        assert!(request.flags.buffer_select);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn recv_multishot_provided_builds_without_panic() {
        let request =
            IoRequest::<()>::recv_multishot_provided(3, crate::buffer::slot::BufGroupId::new(0));
        assert!(request.flags.multishot);
        assert!(request.flags.buffer_select);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn close_builds_without_panic() {
        let request = IoRequest::<()>::close(5);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn timeout_builds_without_panic() {
        let mut scratch = scratch();
        let request = IoRequest::<()>::timeout(1_500_000_000);
        let _entry = build_entry(&request, &mut scratch);
    }

    #[test]
    fn cancel_builds_without_panic() {
        let request = IoRequest::<()>::cancel(SubmitToken::new(77));
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn msg_ring_builds_without_panic() {
        let request = IoRequest::<()>::msg_ring(10, 0x1234);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn poll_add_builds_without_panic() {
        let request = IoRequest::<()>::poll_add(3, libc::POLLIN as u32);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn read_builds_without_panic() {
        let request = IoRequest::read(3, MockBuf::new(64), 0);
        let _entry = build_entry_read(&request);
    }

    #[test]
    fn write_builds_without_panic() {
        let request = IoRequest::write(3, MockBuf::filled(4), 0);
        let _entry = build_entry_write(&request);
    }

    #[test]
    fn recv_builds_without_panic() {
        let request = IoRequest::recv(3, MockBuf::new(128));
        let _entry = build_entry_read(&request);
    }

    #[test]
    fn send_builds_without_panic() {
        let request = IoRequest::send(3, MockBuf::filled(4));
        let _entry = build_entry_write(&request);
    }

    #[test]
    fn fixed_fd_builds_without_panic() {
        let request = IoRequest::<()>::accept(3).with_registered_fd(5);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn fsync_builds_without_panic() {
        let request = IoRequest::<()>::fsync(3);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn splice_builds_without_panic() {
        let request = IoRequest::<()>::splice(3, 0, 4, 0, 4096, 0);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn tee_builds_without_panic() {
        let request = IoRequest::<()>::tee(3, 4, 4096, 0);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn shutdown_builds_without_panic() {
        let request = IoRequest::<()>::shutdown(3);
        let _entry = build_entry(&request, &mut scratch());
    }

    #[test]
    fn socket_builds_without_panic() {
        let request = IoRequest::<()>::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        let _entry = build_entry(&request, &mut scratch());
    }
}
