//! Whether a task may move between workers, decided at compile time.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

pub(crate) mod marker;
