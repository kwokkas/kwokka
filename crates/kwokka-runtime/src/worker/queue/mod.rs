//! Per-worker single-producer queues: the spawn inbox, the timer-arm inbox,
//! and the reap queue.

pub(crate) mod arm;
pub(crate) mod inbox;
pub(crate) mod reap;
