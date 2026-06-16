//! `io_uring` buffer ring kernel ABI struct.
//!
//! Wraps the kernel's `io_uring_buf` ABI as [`BufRingEntry`]. Field
//! `resv` at offset 14 of entry 0 doubles as the shared tail counter via
//! the `io_uring_buf_ring` union layout.

#![allow(dead_code, reason = "pending buf_ring wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

/// `io_uring` buffer entry -- kernel ABI struct.
///
/// Matches `struct io_uring_buf` from the kernel header exactly.
/// Field `resv` at offset 14 of entry 0 doubles as the shared tail
/// counter via the `io_uring_buf_ring` union layout.
///
/// Verified: `liburing/man/io_uring_register_buf_ring.3`.
///
/// Fields are `pub(crate)` so the [`BufRing`](crate::buffer::ring::memory::BufRing)
/// handle in the sibling `memory` module can write entry slots directly.
#[repr(C, align(16))]
pub(crate) struct BufRingEntry {
    pub(crate) addr: u64,
    pub(crate) len: u32,
    pub(crate) bid: u16,
    pub(crate) resv: u16,
}

const _: () = assert!(core::mem::size_of::<BufRingEntry>() == 16);

/// Byte offset of the shared tail counter within entry 0 (the `resv` field).
pub(crate) const TAIL_OFFSET: usize = 14;
