//! `io_uring` SQE builder modules, split by opcode family.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod control;
pub(crate) mod io;
pub(crate) mod net;
pub(crate) mod sync;
pub(crate) mod xfer;
