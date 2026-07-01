//! `io_uring` buffer and file descriptor registration wrappers.
//!
//! Thin wrappers around `Submitter::register_buffers`,
//! `register_files`, and their unregister counterparts. These
//! are called by `UringDriver` to implement
//! the [`IoDriver`](crate::IoDriver) registration trait methods.

#![allow(dead_code, reason = "pending registered-file wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::{io, os::fd::RawFd};

use io_uring::IoUring;

use crate::RegisterError;

/// Register a set of buffers with the kernel for fixed I/O.
///
/// # Safety
///
/// The caller must ensure the `iovec` pointers remain valid until
/// `unregister_buffers` is called or the ring is dropped. Violation
/// causes the kernel to read/write freed memory.
///
/// # Errors
///
/// Returns [`RegisterError::InvalidArgument`] if the kernel rejects
/// the registration (e.g. too many buffers, invalid alignment).
pub(crate) unsafe fn register_buffers(
    ring: &IoUring,
    bufs: &[libc::iovec],
) -> Result<(), RegisterError> {
    // SAFETY: Caller guarantees each iovec base pointer and length
    // remain valid until unregister_buffers or ring drop. If violated,
    // the kernel reads/writes freed memory -- undefined behavior.
    unsafe {
        ring.submitter()
            .register_buffers(bufs)
            .map_err(io_to_register_error)
    }
}

/// Unregister all previously registered buffers.
///
/// # Errors
///
/// Returns [`RegisterError::InvalidArgument`] if no buffers are
/// currently registered.
pub(crate) fn unregister_buffers(ring: &IoUring) -> Result<(), RegisterError> {
    ring.submitter()
        .unregister_buffers()
        .map_err(io_to_register_error)
}

/// Register a set of file descriptors for fixed-fd operations.
///
/// Each fd may be -1 (sparse slot, filled later via update).
/// The kernel waits for the ring to idle before completing.
///
/// # Errors
///
/// Returns [`RegisterError::InvalidArgument`] if the kernel rejects
/// the registration.
pub(crate) fn register_files(ring: &IoUring, fds: &[RawFd]) -> Result<(), RegisterError> {
    ring.submitter()
        .register_files(fds)
        .map_err(io_to_register_error)
}

/// Unregister all previously registered file descriptors.
///
/// # Errors
///
/// Returns [`RegisterError::InvalidArgument`] if no files are
/// currently registered.
pub(crate) fn unregister_files(ring: &IoUring) -> Result<(), RegisterError> {
    ring.submitter()
        .unregister_files()
        .map_err(io_to_register_error)
}

/// Register a provided-buffer ring group with the kernel.
///
/// Wraps `Submitter::register_buf_ring_with_flags`
/// (`IORING_REGISTER_PBUF_RING`, 5.19+; see `io_uring_register_buf_ring.3`).
/// The kernel selects buffers from this ring for `IOSQE_BUFFER_SELECT`
/// recv operations, so the caller never hands the kernel a per-op buffer.
/// No explicit unregister is issued: closing the ring fd on driver drop
/// tears down the `io_uring` instance and its registrations
/// (`io_uring_setup.2`), which is why the caller only needs to keep the
/// region mapped until that close.
///
/// The region at `ring_addr` (`entries` slots of 16 bytes) must stay mapped
/// until the ring fd closes; `BufRingPool` owns it for the pool's lifetime
/// and the pool is declared after the ring in `UringDriver`, so the region
/// always outlives the kernel's metadata reads.
///
/// # Errors
///
/// Returns [`RegisterError::InvalidArgument`] if the kernel rejects the
/// registration (unsupported, `entries` over 32768, or `bgid` in use),
/// or [`RegisterError::SlotExhausted`] on `EBUSY` / `ENOMEM`.
pub(crate) fn register_buf_ring(
    ring: &IoUring,
    ring_addr: u64,
    entries: u16,
    bgid: u16,
) -> Result<(), RegisterError> {
    // SAFETY: Invariant -- the ring_addr region (entries * 16 bytes) stays
    // mapped until the ring fd closes: `BufRingPool` owns it and is declared
    // after the ring in `UringDriver`, so the kernel's metadata reads always
    // hit live memory.
    // Precondition: ring_addr points to a zero-filled, page-aligned mmap of
    // at least entries * 16 bytes, which `BufRingPool::new` guarantees.
    // Failure mode: a region freed while registered lets the kernel read
    // unmapped pages on the next buffer selection (undefined behavior).
    unsafe {
        ring.submitter()
            .register_buf_ring_with_flags(ring_addr, entries, bgid, 0)
            .map_err(io_to_register_error)
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err passes io::Error by value; taking &io::Error would require a closure wrapper"
)]
fn io_to_register_error(error: io::Error) -> RegisterError {
    match error.raw_os_error() {
        Some(libc::EBUSY | libc::ENOMEM) => RegisterError::SlotExhausted,
        _ => RegisterError::InvalidArgument,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_to_register_error_einval() {
        let error = io::Error::from_raw_os_error(libc::EINVAL);
        assert_eq!(io_to_register_error(error), RegisterError::InvalidArgument);
    }

    #[test]
    fn io_to_register_error_ebusy() {
        let error = io::Error::from_raw_os_error(libc::EBUSY);
        assert_eq!(io_to_register_error(error), RegisterError::SlotExhausted);
    }

    #[test]
    fn io_to_register_error_enomem() {
        let error = io::Error::from_raw_os_error(libc::ENOMEM);
        assert_eq!(io_to_register_error(error), RegisterError::SlotExhausted);
    }

    #[test]
    fn io_to_register_error_unknown() {
        let error = io::Error::from_raw_os_error(libc::EPERM);
        assert_eq!(io_to_register_error(error), RegisterError::InvalidArgument);
    }
}
