//! Pinned-storage completion futures for buffered I/O operations.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io;

use crate::buffer::inflight::INFLIGHT_BUF_STRIDE;

pub(crate) mod file;
pub(crate) mod provided;
pub(crate) mod socket;
pub(crate) mod zerocopy;

/// Compile-time guard that a buffered future's `CAP` fits one in-flight slot.
///
/// The kernel writes (recv / read) or reads (send / write) at most `CAP` bytes
/// of a slot whose width is `INFLIGHT_BUF_STRIDE`; a `CAP` past the stride would
/// address memory outside the slot. Each buffered future evaluates this in a
/// `const` block at the top of `poll`, so the bound is a compile error, not a
/// runtime check.
pub(crate) const fn assert_cap_fits<const CAP: usize>() {
    const {
        assert!(
            CAP <= INFLIGHT_BUF_STRIDE as usize,
            "buffered future CAP must fit the in-flight slot stride"
        );
    }
}

/// Maps a raw completion result (the `io_uring` CQE `res`, the value the
/// kernel returns from the I/O syscall) into a byte count.
///
/// A non-negative result is the number of bytes transferred; `0` is a
/// valid count (end of stream on a read, a zero-length write), not an
/// error.
///
/// # Errors
///
/// Returns the [`io::Error`] for `-errno` when the result is negative.
pub(crate) fn bytes_from_cqe(result: i32) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    Ok(usize::try_from(result).unwrap_or(0))
}
