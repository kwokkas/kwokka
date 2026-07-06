//! Core I/O operation primitives: buffer traits, opcodes, tokens.

#![cfg_attr(
    unix,
    allow(
        clippy::redundant_pub_crate,
        reason = "pub(crate) on module-private items"
    )
)]

mod buffer;
#[cfg(unix)]
pub(crate) mod msghdr;
mod op;
mod token;

pub use buffer::{FixedBuf, InlineBuf, IoBuf, IoBufMut};
pub use op::{OpCode, OpFlags};
pub use token::{SubmitResult, SubmitToken};
