//! Cross-thread worker wake primitive -- platform-uniform surface.
//!
//! On Linux this is the `uring` wake fd: an eventfd a parked worker arms
//! on its own ring so a remote signal completes the park as a CQE. Other
//! platforms get inert stubs so the runtime stays free of platform
//! branches; their parks remain completion- and timer-driven until a
//! native wake primitive lands with the backend.

#[cfg(target_os = "linux")]
pub use crate::uring::wake::{WAKE_FD_USER_DATA, close_wake_fd, create_wake_fd, signal_wake_fd};

#[cfg(not(target_os = "linux"))]
mod inert {
    use std::io;

    /// CQE `user_data` marking the wake-fd read; never produced here.
    ///
    /// Matches the Linux constant, so every platform agrees on the
    /// sentinel value.
    pub const WAKE_FD_USER_DATA: u64 = u64::MAX;

    /// Inert stand-in: no wake fd exists off Linux yet.
    ///
    /// # Errors
    ///
    /// Never fails; the sentinel descriptor is negative and every other
    /// entry point ignores it.
    pub fn create_wake_fd() -> io::Result<i32> {
        Ok(-1)
    }

    /// Inert stand-in; reports success so callers stay branch-free.
    ///
    /// # Errors
    ///
    /// Never fails.
    pub fn signal_wake_fd(_fd: i32) -> io::Result<()> {
        Ok(())
    }

    /// Inert stand-in; nothing to close.
    pub fn close_wake_fd(_fd: i32) {}
}

#[cfg(not(target_os = "linux"))]
pub use inert::{WAKE_FD_USER_DATA, close_wake_fd, create_wake_fd, signal_wake_fd};
