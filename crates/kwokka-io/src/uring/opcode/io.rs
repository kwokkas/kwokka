//! SQE builders for file I/O opcodes: read, write, fsync, splice, tee.

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

use crate::operation::OpFlags;

/// Build a read SQE.
pub(crate) fn build_read(
    fd: i32,
    ptr: *mut u8,
    capacity: usize,
    offset: u64,
    flags: OpFlags,
) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "capacity bounded by buffer allocation"
    )]
    let len = capacity as u32;
    if flags.fixed_fd {
        opcode::Read::new(Fixed(fd as u32), ptr, len)
            .offset(offset)
            .build()
    } else {
        opcode::Read::new(Fd(fd), ptr, len).offset(offset).build()
    }
}

/// Build a write SQE.
pub(crate) fn build_write(
    fd: i32,
    ptr: *const u8,
    len: usize,
    offset: u64,
    flags: OpFlags,
) -> Entry {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "len bounded by buffer init bytes"
    )]
    let len = len as u32;
    if flags.fixed_fd {
        opcode::Write::new(Fixed(fd as u32), ptr, len)
            .offset(offset)
            .build()
    } else {
        opcode::Write::new(Fd(fd), ptr, len).offset(offset).build()
    }
}

/// Build a vectored read SQE (`READV`).
///
/// `iovec` points at a `count`-entry `libc::iovec` array pinned in the caller's
/// in-flight slot; `count` is the entry count, not a byte length.
#[cfg(unix)]
pub(crate) fn build_readv(
    fd: i32,
    iovec: *const libc::iovec,
    count: u32,
    offset: u64,
    flags: OpFlags,
) -> Entry {
    if flags.fixed_fd {
        opcode::Readv::new(Fixed(fd as u32), iovec, count)
            .offset(offset)
            .build()
    } else {
        opcode::Readv::new(Fd(fd), iovec, count)
            .offset(offset)
            .build()
    }
}

/// Build a vectored write SQE (`WRITEV`).
///
/// `iovec` points at a `count`-entry `libc::iovec` array pinned in the caller's
/// in-flight slot; `count` is the entry count, not a byte length.
#[cfg(unix)]
pub(crate) fn build_writev(
    fd: i32,
    iovec: *const libc::iovec,
    count: u32,
    offset: u64,
    flags: OpFlags,
) -> Entry {
    if flags.fixed_fd {
        opcode::Writev::new(Fixed(fd as u32), iovec, count)
            .offset(offset)
            .build()
    } else {
        opcode::Writev::new(Fd(fd), iovec, count)
            .offset(offset)
            .build()
    }
}

/// Build an fsync SQE.
pub(crate) fn build_fsync(fd: i32, flags: OpFlags) -> Entry {
    if flags.fixed_fd {
        opcode::Fsync::new(Fixed(fd as u32)).build()
    } else {
        opcode::Fsync::new(Fd(fd)).build()
    }
}

/// Build a splice SQE.
pub(crate) fn build_splice(
    fd_in: i32,
    off_in: i64,
    fd_out: i32,
    off_out: i64,
    nbytes: u32,
    splice_flags: u32,
    flags: OpFlags,
) -> Entry {
    if flags.fixed_fd {
        opcode::Splice::new(
            Fixed(fd_in as u32),
            off_in,
            Fixed(fd_out as u32),
            off_out,
            nbytes,
        )
        .flags(splice_flags)
        .build()
    } else {
        opcode::Splice::new(Fd(fd_in), off_in, Fd(fd_out), off_out, nbytes)
            .flags(splice_flags)
            .build()
    }
}

/// Build a tee SQE.
pub(crate) fn build_tee(
    fd_in: i32,
    fd_out: i32,
    nbytes: u32,
    splice_flags: u32,
    flags: OpFlags,
) -> Entry {
    if flags.fixed_fd {
        opcode::Tee::new(Fixed(fd_in as u32), Fixed(fd_out as u32), nbytes)
            .flags(splice_flags)
            .build()
    } else {
        opcode::Tee::new(Fd(fd_in), Fd(fd_out), nbytes)
            .flags(splice_flags)
            .build()
    }
}
