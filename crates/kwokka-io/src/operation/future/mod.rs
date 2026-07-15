//! Pinned-storage completion futures for buffered I/O operations.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io;

pub(crate) mod file;
#[cfg(unix)]
pub(crate) mod msg;
pub(crate) mod provided;
pub(crate) mod socket;
#[cfg(unix)]
pub(crate) mod vectored;
pub(crate) mod zerocopy;

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
