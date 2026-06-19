//! I/O operation types -- submit tokens, completions, op descriptors, and buffers.

pub(crate) mod completion;
pub(crate) mod core;
pub(crate) mod request;

pub use completion::{Completion, CqeFlags};
pub use core::{InlineBuf, IoBuf, IoBufMut, OpCode, OpFlags, SubmitResult, SubmitToken};
pub use request::{CommonFields, ControlPayload, IoRequest, OpPayload};
