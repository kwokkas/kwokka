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

pub(crate) use alloc::{claim_block, claim_one, release, release_block};

pub(crate) use mailbox::{
    INBOX_CAPACITY, enqueue, pop, publish_endpoint, set_parked, signal, withdraw_endpoint,
};
#[cfg(feature = "steal")]
pub(crate) use steal::{
    has_handoff, has_steal_request, pop_handoff, pop_settled, pop_steal_request, push_handoff,
    push_settled, push_steal_request,
};
