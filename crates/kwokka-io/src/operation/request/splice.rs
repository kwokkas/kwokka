//! Kernel-internal transfer between descriptors, with no user buffer in
//! the path.

use crate::operation::{IoRequest, OpPayload, core::OpCode};

impl IoRequest<()> {
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
}
