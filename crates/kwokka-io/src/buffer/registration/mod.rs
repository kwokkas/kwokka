//! Userspace slot registries for buffers and file descriptors.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]
#![expect(unused_imports, reason = "pending io_uring backend wire-up")]

pub(crate) mod buffers;
pub(crate) mod fds;

pub(crate) use buffers::RegisteredBuffers;
pub(crate) use fds::RegisteredFds;
