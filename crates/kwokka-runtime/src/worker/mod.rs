//! Per-worker thread infrastructure -- identity, shard state, and event loop.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod coordination;
pub(crate) mod cycle;
mod id;
pub(crate) mod park;
pub(crate) mod poll;
pub(crate) mod queue;
pub(crate) mod shard;

pub(crate) use coordination as registry;
pub use id::{WorkerError, WorkerId};
