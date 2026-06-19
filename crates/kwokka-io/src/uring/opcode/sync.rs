//! SQE builders for synchronous close opcodes.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use io_uring::{opcode, squeue::Entry, types::Fd};

/// Build a close SQE.
pub(crate) fn build_close(fd: i32) -> Entry {
    opcode::Close::new(Fd(fd)).build()
}
