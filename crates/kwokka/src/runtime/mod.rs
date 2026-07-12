//! Runtime entry points -- scheduler-explicit construction and blocking
//! execution.
//!
//! [`Runtime::affine`] pins a single worker to the calling thread;
//! [`Runtime::stealing`] boots a crew of workers that relocate sleeping
//! tasks toward idle siblings. Both are driven by `block_on`, and custom
//! capacities go through [`RuntimeBuilder`].

pub use kwokka_runtime::{Runtime, RuntimeBuilder};
