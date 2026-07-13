//! The runtime entry point -- `Runtime::affine()`, `Runtime::stealing()`,
//! and the configuration builder that construct and own the workers.
//!
//! `build` is the surface a caller touches: the builder that configures a
//! runtime and the handle it yields. `crew` brings the sibling worker threads
//! up per scheduler discipline and owns the shutdown broadcast they watch.
//! `drive` is what a worker does once a root future is running -- one pass of
//! the blocking loop, the drains that pass calls out to, and the root task the
//! loop runs until.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod build;
pub(crate) mod crew;
pub(crate) mod drive;

pub use build::{builder::RuntimeBuilder, handle::Runtime};
