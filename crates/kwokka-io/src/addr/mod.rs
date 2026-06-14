//! Socket address types for `io_uring` SQE submission.
//!
//! [`UnixAddr`] handles filesystem-path, abstract (Linux), and unnamed Unix
//! domain addresses. Pack helpers write POSIX `sockaddr_storage`-compatible
//! bytes into a caller-owned buffer for SQE pointer-lifetime safety.

pub(crate) mod pack;
mod socket;
mod unix;

pub use socket::{AddressFamily, SockAddr};
pub use unix::{AddrError, UnixAddr};
