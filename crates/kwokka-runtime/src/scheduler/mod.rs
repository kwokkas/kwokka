//! Scheduler primitives -- local run queue and dispatch.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

#[cfg(target_os = "linux")]
pub(crate) mod affine;
pub(crate) mod dispatch;
pub(crate) mod queue;
#[cfg(feature = "steal")]
pub(crate) mod stealing;
