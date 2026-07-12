//! Userspace slot registries for buffers and file descriptors.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod buffers;
pub(crate) mod fds;
pub(crate) mod slot;

pub(crate) use buffers::RegisteredBuffers;
pub(crate) use fds::RegisteredFds;
