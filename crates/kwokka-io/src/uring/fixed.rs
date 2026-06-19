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
