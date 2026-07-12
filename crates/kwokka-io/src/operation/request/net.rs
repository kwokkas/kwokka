//! Socket lifecycle and message ops: accept, connect, shutdown, and the
//! prepared `msghdr` pair.

#[cfg(unix)]
use std::ptr::NonNull;

use crate::{
    addr::SockAddr,
    buffer::registration::slot::BufGroupId,
    operation::{IoRequest, OpPayload, core::OpCode},
};

impl IoRequest<()> {
    /// Accept a connection on `fd`.
    pub fn accept(fd: i32) -> Self {
        Self::build(fd, OpCode::Accept, OpPayload::Fd)
    }

    /// Accept connections on `fd`, multishot variant.
    pub fn accept_multishot(fd: i32) -> Self {
        Self::accept(fd).with_multishot()
    }

    /// Receive on `fd` into a kernel-selected provided buffer from `group`.
    ///
    /// Carries no caller buffer -- the kernel picks a buffer from the
    /// registered `buf_ring` group and echoes the chosen id in the CQE flags.
    pub fn recv_provided(fd: i32, group: BufGroupId) -> Self {
        let mut request = Self::build(fd, OpCode::RecvProvided, OpPayload::Fd);
        request.common.buf_group = Some(group.0);
        request.flags = request.flags.with_buffer_select(true);
        request
    }

    /// Receive on `fd` into kernel-selected provided buffers, multishot variant.
    ///
    /// One SQE streams a CQE per received buffer until cancelled; each carries
    /// the chosen buffer id in its CQE flags, the same as
    /// [`recv_provided`](Self::recv_provided) but re-armed by the kernel after
    /// every buffer.
    pub fn recv_multishot_provided(fd: i32, group: BufGroupId) -> Self {
        Self::recv_provided(fd, group).with_multishot()
    }

    /// Connect `fd` to `addr`.
    pub fn connect(fd: i32, addr: SockAddr) -> Self {
        Self::build(fd, OpCode::Connect, OpPayload::Socket { addr })
    }

    /// Send a pre-built message over `fd` (`sendmsg`).
    ///
    /// The `msghdr` and its backing bytes live in the caller's future-pinned
    /// in-flight slot, so this carries only the pointer.
    #[cfg(unix)]
    pub(crate) fn sendmsg_prepared(fd: i32, msghdr: NonNull<libc::msghdr>) -> Self {
        Self::build(fd, OpCode::Sendmsg, OpPayload::Msg { msghdr })
    }

    /// Receive a message on `fd` into a pre-built `msghdr` (`recvmsg`).
    ///
    /// The `msghdr` and its backing bytes live in the caller's future-pinned
    /// in-flight slot, so this carries only the pointer.
    #[cfg(unix)]
    pub(crate) fn recvmsg_prepared(fd: i32, msghdr: NonNull<libc::msghdr>) -> Self {
        Self::build(fd, OpCode::Recvmsg, OpPayload::Msg { msghdr })
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
