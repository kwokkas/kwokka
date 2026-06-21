//! Process-wide worker coordination tables: wake inboxes, wake endpoints,
//! worker-id allocation, and (when the steal feature is active) the
//! steal-request, handoff, and settled-note channels.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

mod alloc;
mod inbox;
#[cfg(feature = "steal")]
mod steal;

pub(crate) use alloc::*;
pub(crate) use inbox::*;
#[cfg(feature = "steal")]
pub(crate) use steal::*;
