//! Per-worker thread infrastructure -- identity, shard state, and event loop.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod cycle;
pub(crate) mod endpoint;
pub(crate) mod frame;
mod id;
pub(crate) mod inbox;
pub(crate) mod reap;
pub(crate) mod registry;
pub(crate) mod shard;
pub(crate) mod wake;

pub use id::{WorkerError, WorkerId};
