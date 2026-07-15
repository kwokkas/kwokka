//! Core I/O operation primitives: buffer traits, opcodes, tokens.

#![cfg_attr(
    unix,
    allow(
        clippy::redundant_pub_crate,
        reason = "pub(crate) on module-private items"
    )
)]

mod buffer;
mod completion;
#[cfg(unix)]
pub(crate) mod msghdr;
mod op;
mod token;
#[cfg(unix)]
pub(crate) mod vectored;

pub use buffer::{FixedBuf, InlineBuf, IoBuf, IoBufMut};
pub use completion::{Completion, CqeFlags};
pub use op::{OpCode, OpFlags};
pub use token::{SubmitResult, SubmitToken};
