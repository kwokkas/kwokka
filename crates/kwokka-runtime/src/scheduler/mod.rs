//! Scheduler primitives -- local run queue and dispatch.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod dispatch;
pub(crate) mod queue;
#[cfg(feature = "steal")]
pub(crate) mod stealing;
