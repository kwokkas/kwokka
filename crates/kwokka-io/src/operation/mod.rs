//! I/O operation types -- submit tokens, completions, op descriptors, and buffers.

pub(crate) mod core;
pub(crate) mod future;
pub(crate) mod request;

pub use core::{
    Completion, CqeFlags, FixedBuf, InlineBuf, IoBuf, IoBufMut, OpCode, OpFlags, SubmitResult,
    SubmitToken,
};

#[cfg(unix)]
pub use future::msg::{RecvMsgFuture, SendMsgFuture};
pub use future::{
    file::{FileReadFuture, FileWriteFuture},
    provided::ProvidedRecvFuture,
    socket::{RecvFuture, SendFuture},
    zerocopy::SendZcFuture,
};
pub use request::{CommonFields, ControlPayload, IoRequest, OpPayload};

pub use crate::buffer::ring::pool::ProvidedBuf;
