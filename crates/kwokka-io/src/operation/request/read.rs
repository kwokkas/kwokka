//! Ops that fill a caller buffer: read, recv, and their vectored and
//! multishot forms.

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

    /// Vectored read from `fd` into `buf` at `offset`.
    pub fn readv(fd: i32, buf: B, offset: u64) -> Self {
        let mut request = Self::build(fd, OpCode::Read, OpPayload::Buffer { buf, offset });
        request.flags = request.flags.with_vectored(true);
        request
    }
}
