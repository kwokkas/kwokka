//! Scheduler primitives -- where a runnable task waits and who may run it.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

#[cfg(target_os = "linux")]
pub(crate) mod affine;
pub(crate) mod runnable;
#[cfg(feature = "steal")]
pub(crate) mod stealing;
