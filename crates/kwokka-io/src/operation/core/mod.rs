//! Core I/O operation primitives: buffer traits, opcodes, tokens.

mod buffer;
mod op;
mod token;

pub use buffer::{InlineBuf, IoBuf, IoBufMut};
pub use op::{OpCode, OpFlags};
pub use token::{SubmitResult, SubmitToken};
