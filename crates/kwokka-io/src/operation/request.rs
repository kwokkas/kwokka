//! I/O request type -- the submit payload handed to a backend.
#![allow(
    dead_code,
    reason = "consumed by the backend submit path, not yet implemented"
)]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) satisfies unreachable_pub on this private module"
)]

use crate::addr::SockAddr;
use crate::operation::{IoBuf, IoBufMut, OpCode, OpFlags, SubmitToken};

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
pub struct CommonFields {
    /// Arbitrary `user_data` echoed back in the CQE.
    pub user_data: u64,
    /// Registered buffer slot index (`OpFlags::fixed_buf` auto-set on assignment).
    pub registered_buf: Option<u16>,
    /// Registered fd slot index (`OpFlags::fixed_fd` auto-set on assignment).
    pub registered_fd: Option<u32>,
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
    /// Socket operation with an address (connect, sendmsg, recvmsg, accept).
    Socket {
        /// Remote address for connect/sendmsg; local for accept.
        addr: SockAddr,
        /// Optional buffer for sendmsg/recvmsg; `None` for accept/connect.
        buf: Option<B>,
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
    MsgRing {
        /// Opaque message value echoed in the target ring's CQE.
        msg: u64,
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
    fn build(fd: i32, opcode: OpCode, payload: OpPayload<B>) -> Self {
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

impl<B: IoBufMut> IoRequest<B> {
    /// Read from `fd` into `buf` at `offset`.
    pub fn read(fd: i32, buf: B, offset: u64) -> Self {
        Self::build(fd, OpCode::Read, OpPayload::Buffer { buf, offset })
    }

    /// Receive from `fd` into `buf`.
    pub fn recv(fd: i32, buf: B) -> Self {
        Self::build(fd, OpCode::Recv, OpPayload::Buffer { buf, offset: 0 })
    }

    /// Receive from `fd` into `buf`, multishot variant.
    pub fn recv_multishot(fd: i32, buf: B) -> Self {
        Self::recv(fd, buf).with_multishot()
    }

    /// Receive a message with ancillary data from `fd` into `buf`.
    pub fn recvmsg(fd: i32, buf: B) -> Self {
        Self::build(fd, OpCode::Recvmsg, OpPayload::Buffer { buf, offset: 0 })
    }

    /// Vectored read from `fd` into `buf` at `offset`.
    pub fn readv(fd: i32, buf: B, offset: u64) -> Self {
        let mut request = Self::build(fd, OpCode::Read, OpPayload::Buffer { buf, offset });
        request.flags = request.flags.with_vectored(true);
        request
    }
}

impl<B: IoBuf> IoRequest<B> {
    /// Write `buf` to `fd` at `offset`.
    pub fn write(fd: i32, buf: B, offset: u64) -> Self {
        Self::build(fd, OpCode::Write, OpPayload::Buffer { buf, offset })
    }

    /// Send `buf` over `fd`.
    pub fn send(fd: i32, buf: B) -> Self {
        Self::build(fd, OpCode::Send, OpPayload::Buffer { buf, offset: 0 })
    }

    /// Send `buf` to `addr` over `fd`.
    pub fn sendmsg(fd: i32, addr: SockAddr, buf: B) -> Self {
        Self::build(
            fd,
            OpCode::Sendmsg,
            OpPayload::Socket {
                addr,
                buf: Some(buf),
            },
        )
    }

    /// Vectored write of `buf` to `fd` at `offset`.
    pub fn writev(fd: i32, buf: B, offset: u64) -> Self {
        let mut request = Self::build(fd, OpCode::Write, OpPayload::Buffer { buf, offset });
        request.flags = request.flags.with_vectored(true);
        request
    }
}

impl IoRequest<()> {
    /// Accept a connection on `fd`.
    pub fn accept(fd: i32) -> Self {
        Self::build(fd, OpCode::Accept, OpPayload::Fd)
    }

    /// Accept connections on `fd`, multishot variant.
    pub fn accept_multishot(fd: i32) -> Self {
        Self::accept(fd).with_multishot()
    }

    /// Connect `fd` to `addr`.
    pub fn connect(fd: i32, addr: SockAddr) -> Self {
        Self::build(fd, OpCode::Connect, OpPayload::Socket { addr, buf: None })
    }

    /// Close `fd`.
    pub fn close(fd: i32) -> Self {
        Self::build(fd, OpCode::Close, OpPayload::Fd)
    }

    /// Flush data and metadata for `fd`.
    pub fn fsync(fd: i32) -> Self {
        Self::build(fd, OpCode::Fsync, OpPayload::Fd)
    }

    /// Pre-allocate or deallocate disk space for `fd`.
    pub fn fallocate(fd: i32) -> Self {
        Self::build(fd, OpCode::Fallocate, OpPayload::Fd)
    }

    /// Advise the kernel on access pattern for `fd`.
    pub fn fadvise(fd: i32) -> Self {
        Self::build(fd, OpCode::Fadvise, OpPayload::Fd)
    }

    /// Move data between file descriptors without copying to userspace.
    pub fn splice(
        fd_in: i32,
        off_in: i64,
        fd_out: i32,
        off_out: i64,
        nbytes: u32,
        splice_flags: u32,
    ) -> Self {
        Self::build(
            fd_in,
            OpCode::Splice,
            OpPayload::Splice {
                fd_out,
                off_in,
                off_out,
                nbytes,
                splice_flags,
            },
        )
    }

    /// Duplicate data from `fd_in` to `fd_out` without consuming it.
    pub fn tee(fd_in: i32, fd_out: i32, nbytes: u32, splice_flags: u32) -> Self {
        Self::build(
            fd_in,
            OpCode::Tee,
            OpPayload::Splice {
                fd_out,
                off_in: -1,
                off_out: -1,
                nbytes,
                splice_flags,
            },
        )
    }

    /// Shut down a socket `fd`.
    pub fn shutdown(fd: i32) -> Self {
        Self::build(fd, OpCode::Shutdown, OpPayload::Fd)
    }

    /// Create a new socket with the given domain, type, and protocol.
    pub fn socket(domain: i32, socket_type: i32, protocol: i32) -> Self {
        Self::build(
            -1,
            OpCode::Socket,
            OpPayload::NewSocket {
                domain,
                socket_type,
                protocol,
            },
        )
    }
}

impl IoRequest<()> {
    /// Arm a completion timeout.
    #[doc(hidden)]
    pub fn timeout(duration_ns: u64) -> Self {
        Self::build(
            -1,
            OpCode::Timeout,
            OpPayload::Control(ControlPayload::Timeout { duration_ns }),
        )
    }

    /// Cancel an in-flight operation.
    #[doc(hidden)]
    pub fn cancel(target: SubmitToken) -> Self {
        Self::build(
            -1,
            OpCode::Cancel,
            OpPayload::Control(ControlPayload::Cancel { target }),
        )
    }

    /// Send a message to another ring.
    #[doc(hidden)]
    pub fn msg_ring(target_ring_fd: i32, msg: u64) -> Self {
        Self::build(
            target_ring_fd,
            OpCode::MsgRing,
            OpPayload::Control(ControlPayload::MsgRing { msg }),
        )
    }

    /// Poll a file descriptor for readiness.
    #[doc(hidden)]
    pub fn poll_add(fd: i32, events: u32) -> Self {
        Self::build(
            fd,
            OpCode::Poll,
            OpPayload::Control(ControlPayload::PollAdd { events }),
        )
    }

    /// Remove a poll watch.
    #[doc(hidden)]
    pub fn poll_remove(fd: i32, token: SubmitToken) -> Self {
        Self::build(
            fd,
            OpCode::Poll,
            OpPayload::Control(ControlPayload::PollRemove { token }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn recv_multishot_sets_flag() {
        let request = IoRequest::recv_multishot(3, MockBuf::new(64));
        assert!(request.flags.multishot);
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
