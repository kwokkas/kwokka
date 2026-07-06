//! SQE builders for network opcodes: recv, send, accept, connect, socket, shutdown.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
#![allow(
    clippy::cast_sign_loss,
    reason = "fd-to-registered-index casts (i32 -> u32) are inherent to the Fixed(fd) io_uring ABI"
)]

use std::ptr::NonNull;

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

/// Build a provided-buffer recv SQE.
///
/// Emits a multishot provided recv (`io_uring_prep_recv_multishot.3`) when
/// `flags.multishot` is set: one SQE re-arms and streams a CQE per selected
/// buffer until cancelled. Otherwise a single-shot recv that completes once.
/// `len` is 0, so the kernel fills the buffer it selects from `buf_group` up to
/// that buffer's own size. For the single-shot path the `IOSQE_BUFFER_SELECT`
/// bit is set in `sqe_flags`, not here -- `Recv::build()` sets only the
/// `buf_group` field (`io_uring_prep_recv.3`; the flag is `RecvMulti`-only in
/// the io-uring crate). `buf_group` is always `Some` for a provided-recv
/// request; `0` is the sole per-worker group and a safe default.
pub(crate) fn build_recv_provided(fd: i32, buf_group: Option<u16>, flags: OpFlags) -> Entry {
    if flags.multishot {
        return build_recv_multishot_provided(fd, buf_group, flags);
    }
    let group = buf_group.unwrap_or(0);
    if flags.fixed_fd {
        opcode::Recv::new(Fixed(fd as u32), std::ptr::null_mut(), 0)
            .buf_group(group)
            .build()
    } else {
        opcode::Recv::new(Fd(fd), std::ptr::null_mut(), 0)
            .buf_group(group)
            .build()
    }
}

/// Build a multishot provided-buffer recv SQE (`io_uring_prep_recv_multishot.3`).
///
/// `RecvMulti::build()` sets the `IOSQE_BUFFER_SELECT` bit and the multishot
/// marker itself, so one SQE re-arms and streams a CQE per selected buffer until
/// cancelled. `buf_group` mirrors the single-shot path -- `0` is the sole
/// per-worker group and a safe default.
fn build_recv_multishot_provided(fd: i32, buf_group: Option<u16>, flags: OpFlags) -> Entry {
    let group = buf_group.unwrap_or(0);
    if flags.fixed_fd {
        opcode::RecvMulti::new(Fixed(fd as u32), group).build()
    } else {
        opcode::RecvMulti::new(Fd(fd), group).build()
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
///
/// Emits a multishot accept (`io_uring_prep_multishot_accept.3`) when
/// `flags.multishot` is set: one SQE posts a CQE per incoming connection until
/// the op is cancelled. Otherwise a single-shot accept that completes once.
pub(crate) fn build_accept(fd: i32, flags: OpFlags) -> Entry {
    if flags.multishot {
        return build_accept_multi(fd, flags);
    }
    if flags.fixed_fd {
        opcode::Accept::new(Fixed(fd as u32), std::ptr::null_mut(), std::ptr::null_mut()).build()
    } else {
        opcode::Accept::new(Fd(fd), std::ptr::null_mut(), std::ptr::null_mut()).build()
    }
}

/// Build a multishot accept SQE (`io_uring_prep_multishot_accept.3`).
fn build_accept_multi(fd: i32, flags: OpFlags) -> Entry {
    if flags.fixed_fd {
        opcode::AcceptMulti::new(Fixed(fd as u32)).build()
    } else {
        opcode::AcceptMulti::new(Fd(fd)).build()
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

/// Build a sendmsg SQE (`io_uring_prep_sendmsg.3`).
///
/// The `msghdr` (with its `iovec` and packed address) lives in the caller's
/// future-pinned in-flight slot and stays valid until the op's CQE.
pub(crate) fn build_sendmsg(fd: i32, msghdr: NonNull<libc::msghdr>, flags: OpFlags) -> Entry {
    let ptr = msghdr.as_ptr().cast_const();
    if flags.fixed_fd {
        opcode::SendMsg::new(Fixed(fd as u32), ptr).build()
    } else {
        opcode::SendMsg::new(Fd(fd), ptr).build()
    }
}

/// Build a recvmsg SQE (`io_uring_prep_recvmsg.3`).
///
/// The `msghdr` lives in the caller's future-pinned in-flight slot; the kernel
/// writes the received address and control data into it, valid until the CQE.
pub(crate) fn build_recvmsg(fd: i32, msghdr: NonNull<libc::msghdr>, flags: OpFlags) -> Entry {
    let ptr = msghdr.as_ptr();
    if flags.fixed_fd {
        opcode::RecvMsg::new(Fixed(fd as u32), ptr).build()
    } else {
        opcode::RecvMsg::new(Fd(fd), ptr).build()
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
