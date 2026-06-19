//! Pinned-storage completion futures for buffered I/O operations.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod file;
pub(crate) mod socket;
