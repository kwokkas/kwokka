//! The runtime entry point -- `Runtime::affine()`, `Runtime::stealing()`,
//! and the configuration builder that construct and own the workers.

mod bootstrap;
mod builder;
mod handle;
mod stealing;

pub use builder::RuntimeBuilder;
pub use handle::Runtime;
