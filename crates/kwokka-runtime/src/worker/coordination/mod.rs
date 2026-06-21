//! Process-wide worker coordination tables: wake mailboxes, wake endpoints,
//! worker-id allocation, and (when the steal feature is active) the
//! steal-request, handoff, and settled-note channels.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

mod alloc;
mod mailbox;
#[cfg(feature = "steal")]
mod steal;

pub(crate) use alloc::*;
pub(crate) use mailbox::*;
#[cfg(feature = "steal")]
pub(crate) use steal::*;
