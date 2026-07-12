//! File-descriptor lifecycle ops: close, and the sync and allocation
//! hints that act on a whole file.

use crate::operation::{IoRequest, OpPayload, core::OpCode};

impl IoRequest<()> {
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
}
