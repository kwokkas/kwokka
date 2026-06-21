//! Per-worker thread infrastructure -- identity, shard state, and event loop.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod coordination;
pub(crate) mod cycle;
pub(crate) mod endpoint;
pub(crate) mod frame;
mod id;
pub(crate) mod inbox;
pub(crate) mod polling;
pub(crate) mod reap;
pub(crate) mod shard;
pub(crate) mod wake;

pub(crate) use coordination as registry;

pub use id::{WorkerError, WorkerId};
