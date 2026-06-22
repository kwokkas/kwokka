//! `#[kwokka::main]` lowering -- argument parsing and entry-point
//! expansion.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

mod expand;
mod parse;

pub(crate) use expand::expand_main;
