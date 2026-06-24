//! Time-driven task utilities -- wall-clock sleeping.
//!
//! [`sleep`] suspends the current task for at least the given duration
//! and resumes it once the runtime timer fires. The returned [`Sleep`]
//! future parks the task rather than spinning while it waits.

pub use kwokka_runtime::timer::{Sleep, sleep};
