//! The `UringDriver` and the helpers coupled to it alone.
//!
//! `submission.rs` builds the SQEs, `fixed.rs` wraps the kernel registration
//! calls, and `wake.rs` owns the eventfd a parked worker arms. None of the
//! three has a caller outside this directory, and the driver embeds the
//! submission scratch as a field of its own.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod driver;
pub(crate) mod fixed;
pub(crate) mod submission;
pub(crate) mod wake;

pub use driver::UringDriver;
