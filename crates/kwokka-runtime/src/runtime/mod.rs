//! The runtime entry point -- `Runtime::affine()`, `Runtime::stealing()`,
//! and the configuration builder that construct and own the workers.

mod affine;
mod bootstrap;
mod builder;
mod handle;
mod stealing;

pub use builder::RuntimeBuilder;
pub use handle::Runtime;
