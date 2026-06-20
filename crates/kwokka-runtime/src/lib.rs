//! Async runtime primitives.
//!
//! [`Runtime::affine`] is the entry point; it builds on the task,
//! scheduler, worker, timer, and sync layers this crate exposes, over
//! kwokka-core's generational slab and bump arena.
//!
//! - [`Runtime`] - the affine/stealing runtime entry point
//! - [`task::TaskRef`] - 64-bit packed task handle, path-agnostic over slab and arena

pub mod runtime;
pub mod scheduler;
pub mod sync;
pub mod task;
pub mod timer;
pub mod worker;

pub use runtime::{Runtime, RuntimeBuilder};
