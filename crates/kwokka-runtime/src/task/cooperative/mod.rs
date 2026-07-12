//! Handing the worker back to its other tasks without waiting on anything.

mod yielding;

pub use yielding::{YieldNow, yield_now};
