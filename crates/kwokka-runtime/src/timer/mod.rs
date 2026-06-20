//! Hierarchical timer wheel for per-worker scheduling.
//!
//! The timer wheel manages entries in userspace. The worker loop
//! queries the next expiry to derive the `io_uring_enter` timeout,
//! and advances the wheel to yield expired [`TaskRef`](crate::task::TaskRef)
//! values without calling `waker.wake()`.

#![allow(
    dead_code,
    reason = "timer registration and cancellation are pending scheduler wire-up"
)]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod clock;
pub(crate) mod entry;
pub(crate) mod handle;
pub(crate) mod slot;
pub(crate) mod wheel;

pub(crate) use handle::{TimerHandle, nz_to_slab, slab_to_nz};
