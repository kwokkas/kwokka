//! One worker: what names it, and the state it owns.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod id;
pub(crate) mod state;
