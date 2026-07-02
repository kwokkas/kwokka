//! Provided buffer ring types: kernel ABI struct, ring handle, and pool.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod abi;
pub(crate) mod memory;
pub mod pool;

pub(crate) use abi::BufRingEntry;
pub(crate) use memory::BufRing;
