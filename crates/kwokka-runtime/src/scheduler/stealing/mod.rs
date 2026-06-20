//! Work-stealing queue substrate -- per-worker deques, steal handles, and
//! the shared overflow injector, all carrying [`TaskRef`] handles only.
//!
//! The 512-byte task bodies never enter these queues: a thief that wins a
//! handle relocates the slot through the separate steal transport. Compiled
//! only under the `steal` feature, so the default affine build carries none
//! of it.
//!
//! [`TaskRef`]: crate::task::TaskRef

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the stealing run loop consumes this substrate, landing with the multi-worker bootstrap"
    )
)]

pub(crate) mod deque;
pub(crate) mod handoff;
pub(crate) mod injector;
pub(crate) mod relocate;
pub(crate) mod steal;
