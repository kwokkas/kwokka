//! What names a task: the `TaskRef` handle and the waker built from it.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod identity;
pub(crate) mod waker;
