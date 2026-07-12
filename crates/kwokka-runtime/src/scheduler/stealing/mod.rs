//! Work-stealing substrate -- handoff protocol and cross-slab task
//! relocation, carrying [`TaskRef`] handles only.
//!
//! The 512-byte task bodies never enter queues directly: a thief wins a
//! handle through the per-worker handoff ring, then relocates the slot
//! through the steal transport defined here. The actual run-loop
//! composition (serve/receive calls, the worker steal ring) lives in the
//! bootstrap layer.
//!
//! [`TaskRef`]: crate::task::TaskRef

pub(crate) mod forward;
pub(crate) mod handoff;
pub(crate) mod relocate;
