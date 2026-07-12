//! Reading a completion queue entry back into a kwokka value.
//!
//! Everything here answers one question about a raw CQE: what result does it
//! carry, was the op cancelled, and is a multishot stream still armed.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod cancel;
pub(crate) mod completion;
pub(crate) mod multishot;
