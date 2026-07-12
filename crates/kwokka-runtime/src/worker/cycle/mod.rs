//! One turn of a worker: the pass that polls what is runnable, and the drain
//! that materializes what a poll asked to spawn.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod pass;
pub(crate) mod spawn;

pub(crate) use pass::{Tick, tick};
pub(crate) use spawn::drain_spawns;
