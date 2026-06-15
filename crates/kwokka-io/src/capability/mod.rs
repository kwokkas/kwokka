//! Backend capability detection and feature matrix.
//!
//! - [`CapabilityMatrix`] - snapshot of features available on the running kernel, queried via
//!   `IoDriver::capabilities()`.
//! - [`KernelVersion`] - kernel version triple used to gate feature activation.

mod matrix;

pub use matrix::{CapabilityMatrix, KernelVersion};
