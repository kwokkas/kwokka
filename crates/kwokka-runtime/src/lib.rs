#![doc(html_logo_url = "https://cdn.kwokka.dev/images/icon-light.png")]
#![doc(html_favicon_url = "https://cdn.kwokka.dev/images/icon-light.png")]
//! Async runtime primitives.
//!
//! [`Runtime::affine`] and [`Runtime::stealing`] are the entry points;
//! they build on the task, scheduler, worker, timer, and sync layers
//! this crate exposes, over kwokka-core's generational slab.
//!
//! - [`Runtime`] - the affine/stealing runtime entry point
//! - [`task::TaskRef`] - 64-bit packed task handle into the per-worker slab

pub mod runtime;
pub mod scheduler;
pub mod sync;
pub mod task;
pub mod timer;
pub mod worker;

pub use runtime::{Runtime, RuntimeBuilder};
