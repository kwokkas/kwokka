//! The request itself: its payload shapes, and the fields every op carries.

#[cfg(unix)]
use std::ptr::NonNull;

use crate::{
    addr::SockAddr,
    operation::core::{OpCode, OpFlags, SubmitToken},
};

/// An I/O request ready for submission to a backend.
///
/// `B` is the buffer type. Ops that carry no buffer use `IoRequest<()>`.
/// The backend builds the SQE from resolved fields; `B` is not carried past
/// submission.
#[non_exhaustive]
pub struct IoRequest<B = ()> {
    /// Target file descriptor (unregistered path) or slot (registered path).
    pub fd: i32,
    /// Logical operation.
    pub opcode: OpCode,
    /// Variant modifier flags (fixed-buf, zero-copy, multishot, vectored).
    pub flags: OpFlags,
    /// Fields common to all ops.
    pub common: CommonFields,
    /// Op-specific payload.
    pub payload: OpPayload<B>,
}

/// Fields shared across all operation types.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct CommonFields {
    /// Arbitrary `user_data` echoed back in the CQE.
    pub user_data: u64,
    /// Registered buffer slot index (`OpFlags::fixed_buf` auto-set on assignment).
    pub registered_buf: Option<u16>,
    /// Registered fd slot index (`OpFlags::fixed_fd` auto-set on assignment).
    pub registered_fd: Option<u32>,
    /// Provided-buffer group id (`OpFlags::buffer_select` auto-set on assignment).
    pub buf_group: Option<u16>,
}

/// Op-specific payload carried by [`IoRequest`].
#[non_exhaustive]
pub enum OpPayload<B> {
    /// Buffer operation (read, write, send, recv).
    Buffer {
        /// The I/O buffer -- owned by the request until the CQE arrives.
        buf: B,
        /// File offset for read/write; 0 for socket ops.
        offset: u64,
    },
    /// Socket operation with an address (connect).
    Socket {
        /// Remote address to connect to.
        addr: SockAddr,
    },
    /// Send or receive a datagram via a pre-built `libc::msghdr`.
    ///
    /// The `msghdr`, its `iovec`, and the packed address all live in one
    /// worker in-flight slot (`operation::core::msghdr`); this variant
    /// carries only the pointer into that slot, never owned bytes.
    #[cfg(unix)]
    Msg {
        /// Pointer to the `msghdr` staged in the worker's in-flight slot.
        msghdr: NonNull<libc::msghdr>,
    },
    /// Scatter/gather read or write via a pre-built `iovec` array.
    ///
    /// The `iovec` array and the gathered or scattered payload live in one
    /// worker in-flight slot (`operation::core::vectored`); this variant carries
    /// only the pointer into that slot and the entry count, never owned bytes.
    #[cfg(unix)]
    Vectored {
        /// Pointer to the `iovec` array staged in the worker's in-flight slot.
        iovec: NonNull<libc::iovec>,
        /// Number of entries in the array.
        count: u32,
        /// File offset the read or write starts at.
        offset: u64,
    },
    /// Splice/tee data transfer between file descriptors.
    Splice {
        /// Output file descriptor.
        fd_out: i32,
        /// Input offset (-1 for pipe).
        off_in: i64,
        /// Output offset (-1 for pipe).
        off_out: i64,
        /// Bytes to transfer.
        nbytes: u32,
        /// `SPLICE_F_*` flags.
        splice_flags: u32,
    },
    /// Create a new socket (domain, type, protocol).
    NewSocket {
        /// Socket domain (e.g. `AF_INET`).
        domain: i32,
        /// Socket type (e.g. `SOCK_STREAM`).
        socket_type: i32,
        /// Protocol number (usually 0).
        protocol: i32,
    },
    /// Single file-descriptor operation (close, fsync, fallocate).
    Fd,
    /// Driver-internal control operation (timeout, cancel, `msg_ring`, poll).
    Control(ControlPayload),
}

/// Payload for driver-internal control operations.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ControlPayload {
    /// Arm a completion timeout.
    Timeout {
        /// Timeout duration in nanoseconds.
        duration_ns: u64,
    },
    /// Cancel an in-flight operation identified by its submit token.
    Cancel {
        /// Token of the operation to cancel.
        target: SubmitToken,
    },
    /// Wake another ring's driver loop via `IORING_OP_MSG_RING`.
    ///
    /// The target ring fd is carried in [`IoRequest::fd`], not in this payload.
    /// `result` and `sentinel` become the target CQE's `res` and `user_data`,
    /// two independent channels per `io_uring_prep_msg_ring.3`.
    MsgRing {
        /// Value echoed in the target ring's CQE `res` field.
        result: i32,
        /// Marker echoed in the target ring's CQE `user_data` field.
        sentinel: u64,
    },
    /// Add a poll-readiness watch.
    PollAdd {
        /// `POLL*` event mask.
        events: u32,
    },
    /// Remove a poll-readiness watch.
    PollRemove {
        /// Token of the `PollAdd` to cancel.
        token: SubmitToken,
    },
}

impl<B> IoRequest<B> {
    pub(crate) fn build(fd: i32, opcode: OpCode, payload: OpPayload<B>) -> Self {
        Self {
            fd,
            opcode,
            flags: OpFlags::new(),
            common: CommonFields::default(),
            payload,
        }
    }

    /// Overrides the `user_data` tag returned in the CQE.
    #[must_use]
    pub const fn with_user_data(mut self, ud: u64) -> Self {
        self.common.user_data = ud;
        self
    }

    /// Marks the request as using a registered buffer slot; sets `OpFlags::fixed_buf`.
    #[must_use]
    pub const fn with_registered_buf(mut self, slot: u16) -> Self {
        self.common.registered_buf = Some(slot);
        self.flags = self.flags.with_fixed_buf(true);
        self
    }

    /// Marks the request as using a registered fd slot; sets `OpFlags::fixed_fd`.
    #[must_use]
    pub const fn with_registered_fd(mut self, slot: u32) -> Self {
        self.common.registered_fd = Some(slot);
        self.flags = self.flags.with_fixed_fd(true);
        self
    }

    /// Enables multishot mode.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_multishot(mut self) -> Self {
        self.flags = self.flags.with_multishot(true);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::core::{IoBuf, IoBufMut};

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

    #[test]
    fn read_sets_correct_opcode_and_fd() {
        let request = IoRequest::read(3, MockBuf::new(64), 0);
        assert_eq!(request.opcode, OpCode::Read);
        assert_eq!(request.fd, 3);
    }

    #[test]
    fn write_sets_correct_opcode() {
        let request = IoRequest::write(3, MockBuf::new(4), 0);
        assert_eq!(request.opcode, OpCode::Write);
    }

    #[test]
    fn send_zc_sets_correct_opcode_and_fd() {
        let request = IoRequest::send_zc(3, MockBuf::new(4));
        assert_eq!(request.opcode, OpCode::SendZc);
        assert_eq!(request.fd, 3);
        assert!(
            !request.flags.zero_copy,
            "OpCode::SendZc disambiguates, not the flag"
        );
    }

    #[test]
    fn recv_multishot_sets_flag() {
        let request = IoRequest::recv_multishot(3, MockBuf::new(64));
        assert!(request.flags.multishot);
    }

    #[test]
    fn recv_multishot_provided_sets_flags() {
        let request = IoRequest::<()>::recv_multishot_provided(
            3,
            crate::buffer::registration::slot::BufGroupId::new(0),
        );
        assert!(request.flags.multishot, "the multishot flag is set");
        assert!(request.flags.buffer_select, "the buffer-select flag is set");
        assert_eq!(
            request.common.buf_group,
            Some(0),
            "the buffer group carries"
        );
    }

    #[test]
    fn accept_multishot_sets_flag() {
        let request = IoRequest::<()>::accept_multishot(3);
        assert!(request.flags.multishot);
    }

    #[test]
    fn with_user_data_updates_common() {
        let request = IoRequest::<()>::accept(3).with_user_data(42);
        assert_eq!(request.common.user_data, 42);
    }

    #[test]
    fn with_registered_buf_sets_flag_and_slot() {
        let request = IoRequest::read(3, MockBuf::new(64), 0).with_registered_buf(7);
        assert_eq!(request.common.registered_buf, Some(7));
        assert!(request.flags.fixed_buf);
    }

    #[test]
    fn with_registered_fd_sets_flag_and_slot() {
        let request = IoRequest::<()>::accept(3).with_registered_fd(2);
        assert_eq!(request.common.registered_fd, Some(2));
        assert!(request.flags.fixed_fd);
    }

    #[test]
    fn connect_sets_socket_payload() {
        use std::net::{Ipv4Addr, SocketAddrV4};
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let request = IoRequest::<()>::connect(3, addr);
        assert_eq!(request.opcode, OpCode::Connect);
        assert_eq!(request.fd, 3);
    }

    #[cfg(unix)]
    #[test]
    fn sendmsg_prepared_sets_msg_payload() {
        let msghdr = NonNull::dangling();
        let request = IoRequest::sendmsg_prepared(5, msghdr);
        assert_eq!(request.opcode, OpCode::Sendmsg);
        assert_eq!(request.fd, 5);
        assert!(matches!(
            request.payload,
            OpPayload::Msg { msghdr: ptr } if ptr == msghdr
        ));
    }

    #[cfg(unix)]
    #[test]
    fn recvmsg_prepared_sets_msg_payload() {
        let msghdr = NonNull::dangling();
        let request = IoRequest::recvmsg_prepared(6, msghdr);
        assert_eq!(request.opcode, OpCode::Recvmsg);
        assert_eq!(request.fd, 6);
        assert!(matches!(
            request.payload,
            OpPayload::Msg { msghdr: ptr } if ptr == msghdr
        ));
    }

    #[test]
    fn default_flags_are_all_false() {
        let request = IoRequest::<()>::accept(3);
        assert_eq!(request.flags, OpFlags::new());
    }

    #[test]
    fn timeout_uses_control_payload() {
        let request = IoRequest::<()>::timeout(1_000_000);
        assert_eq!(request.opcode, OpCode::Timeout);
        let OpPayload::Control(ControlPayload::Timeout { duration_ns }) = request.payload else {
            panic!("expected Timeout control payload");
        };
        assert_eq!(duration_ns, 1_000_000);
    }
}
