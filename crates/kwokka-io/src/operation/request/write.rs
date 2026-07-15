//! Ops that drain a caller buffer: write, send, and their zero-copy and
//! vectored forms.

#[cfg(unix)]
use std::ptr::NonNull;

use crate::operation::{
    IoRequest, OpPayload,
    core::{IoBuf, OpCode},
};

impl<B: IoBuf> IoRequest<B> {
    /// Write `buf` to `fd` at `offset`.
    pub fn write(fd: i32, buf: B, offset: u64) -> Self {
        Self::build(fd, OpCode::Write, OpPayload::Buffer { buf, offset })
    }

    /// Send `buf` over `fd`.
    pub fn send(fd: i32, buf: B) -> Self {
        Self::build(fd, OpCode::Send, OpPayload::Buffer { buf, offset: 0 })
    }

    /// Send `buf` over `fd` zero-copy (`SEND_ZC`).
    ///
    /// The kernel reads `buf` in place rather than copying it into kernel space
    /// (`io_uring_prep_send_zc.3`), so the buffer must outlive the send: the op
    /// posts a notification completion once the kernel has released it.
    pub fn send_zc(fd: i32, buf: B) -> Self {
        Self::build(fd, OpCode::SendZc, OpPayload::Buffer { buf, offset: 0 })
    }
}

impl IoRequest<()> {
    /// Vectored write of a pre-built `iovec` array to `fd` at `offset`.
    ///
    /// The array and the gathered bytes live in the caller's future-pinned
    /// in-flight slot (`operation::core::vectored`); this carries only the
    /// pointer and the entry count, never owned bytes, so the buffers stay put
    /// until the CQE.
    #[cfg(unix)]
    pub(crate) fn writev_prepared(
        fd: i32,
        iovec: NonNull<libc::iovec>,
        count: u32,
        offset: u64,
    ) -> Self {
        Self::build(
            fd,
            OpCode::Write,
            OpPayload::Vectored {
                iovec,
                count,
                offset,
            },
        )
    }
}
