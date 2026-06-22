//! SQE builders for network opcodes: recv, send, accept, connect, socket, shutdown.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
#![allow(
    clippy::cast_sign_loss,
    reason = "fd-to-registered-index casts (i32 -> u32) are inherent to the Fixed(fd) io_uring ABI"
)]

use io_uring::{
    opcode,
    squeue::Entry,
    types::{Fd, Fixed},
};

use crate::{addr::SockAddr, operation::OpFlags};

/// Build a recv SQE.
pub(crate) fn build_recv(fd: i32, ptr: *mut u8, capacity: usize, flags: OpFlags) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "capacity bounded by buffer allocation"
    )]
    let len = capacity as u32;
    if flags.fixed_fd {
        opcode::Recv::new(Fixed(fd as u32), ptr, len).build()
    } else {
        opcode::Recv::new(Fd(fd), ptr, len).build()
    }
}

/// Build a send SQE.
pub(crate) fn build_send(fd: i32, ptr: *const u8, len: usize, flags: OpFlags) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "len bounded by buffer init bytes"
    )]
    let len = len as u32;
    if flags.fixed_fd {
        opcode::Send::new(Fixed(fd as u32), ptr, len).build()
    } else {
        opcode::Send::new(Fd(fd), ptr, len).build()
    }
}

/// Build an accept SQE.
pub(crate) fn build_accept(fd: i32, flags: OpFlags) -> Entry {
    if flags.fixed_fd {
        opcode::Accept::new(Fixed(fd as u32), std::ptr::null_mut(), std::ptr::null_mut()).build()
    } else {
        opcode::Accept::new(Fd(fd), std::ptr::null_mut(), std::ptr::null_mut()).build()
    }
}

/// Build a connect SQE.
pub(crate) fn build_connect(
    fd: i32,
    addr: &SockAddr,
    buf: &mut [u8; 128],
    flags: OpFlags,
) -> Entry {
    let len = addr.pack_into(buf);
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "pack_into writes a valid sockaddr at the buffer start; alignment is guaranteed by the kernel ABI"
    )]
    let ptr = buf.as_ptr().cast::<libc::sockaddr>();
    if flags.fixed_fd {
        opcode::Connect::new(Fixed(fd as u32), ptr, len).build()
    } else {
        opcode::Connect::new(Fd(fd), ptr, len).build()
    }
}

/// Build a socket SQE.
pub(crate) fn build_socket(domain: i32, socket_type: i32, protocol: i32) -> Entry {
    opcode::Socket::new(domain, socket_type, protocol).build()
}

/// Build a shutdown SQE.
///
/// # Note
///
/// Needs a `how` parameter before partial shutdown (`SHUT_RD`/`SHUT_WR`) works.
pub(crate) fn build_shutdown(fd: i32, flags: OpFlags) -> Entry {
    if flags.fixed_fd {
        opcode::Shutdown::new(Fixed(fd as u32), libc::SHUT_RDWR).build()
    } else {
        opcode::Shutdown::new(Fd(fd), libc::SHUT_RDWR).build()
    }
}
