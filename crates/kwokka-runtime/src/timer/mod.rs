//! Hierarchical timer wheel for per-worker scheduling.
//!
//! The timer wheel manages entries in userspace. The worker loop
//! queries the next expiry to derive the `io_uring_enter` timeout,
//! and advances the wheel to yield expired [`TaskRef`](crate::task::TaskRef)
//! values without calling `waker.wake()`.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod clock;
pub(crate) mod request;
mod sleeping;
pub(crate) mod wheel;

pub use sleeping::{Sleep, sleep};
