//! Scheduler, worker, task, memory, timer, and sync primitives.

#![allow(
    dead_code,
    reason = "the task, worker, and scheduler core is wired up by the runtime bootstrap in a later PR"
)]

pub mod scheduler;
pub mod sync;
pub mod task;
pub mod timer;
pub mod worker;
