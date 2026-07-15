//! Ops that fill a caller buffer: read, recv, and their multishot and
//! vectored forms.

#[cfg(unix)]
use std::ptr::NonNull;

use crate::operation::{
    IoRequest, OpPayload,
    core::{IoBufMut, OpCode},
};

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
}

impl IoRequest<()> {
    /// Vectored read from `fd` into a pre-built `iovec` array at `offset`.
    ///
    /// The array lives in the caller's future-pinned in-flight slot
    /// (`operation::core::vectored`), which the kernel fills; this carries only
    /// the pointer and the entry count, never owned bytes, so the destination
    /// buffers stay put until the CQE.
    #[cfg(unix)]
    pub(crate) fn readv_prepared(
        fd: i32,
        iovec: NonNull<libc::iovec>,
        count: u32,
        offset: u64,
    ) -> Self {
        Self::build(
            fd,
            OpCode::Read,
            OpPayload::Vectored {
                iovec,
                count,
                offset,
            },
        )
    }
}
