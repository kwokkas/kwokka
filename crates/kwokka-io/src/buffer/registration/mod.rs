//! Userspace slot registries for buffers and file descriptors.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

#[cfg(target_os = "linux")]
pub(crate) mod buffers;
#[cfg(target_os = "linux")]
pub(crate) mod fds;
pub(crate) mod slot;

#[cfg(target_os = "linux")]
pub(crate) use buffers::RegisteredBuffers;
#[cfg(target_os = "linux")]
pub(crate) use fds::RegisteredFds;
