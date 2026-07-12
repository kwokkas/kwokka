//! Cancel records for dropped in-flight operations, and the tokens that name
//! their completions.

pub mod guard;
pub mod inbox;
pub mod token;

pub use guard::{CancelInboxGuard, RecvCancelInboxGuard};
pub use inbox::{
    ACCEPT_CANCEL_CAPACITY, AcceptCancelSet, CANCEL_INBOX_CAPACITY, CONNECT_CANCEL_CAPACITY,
    CancelInbox, ConnectCancelSet, PROVIDED_RECV_CANCEL_CAPACITY, ProvidedRecvCancelSet,
    RECV_CANCEL_INBOX_CAPACITY, RecvCancelInbox,
};
pub(crate) use inbox::{ACCEPT_CANCEL_SLOT, CONNECT_CANCEL_SLOT, PROVIDED_RECV_CANCEL_SLOT};
pub(crate) use token::{
    LINK_TIMEOUT_DISCARD_USER_DATA, encode_cancel_sentinel, encode_multishot_sentinel,
    encode_recv_multishot_sentinel, multishot_sentinel_generation, multishot_sentinel_slot,
};
pub use token::{
    MSG_RING_WAKE_USER_DATA, is_cancel_sentinel, is_link_timeout_discard, is_msg_ring_wake,
    is_multishot_sentinel, is_recv_multishot_sentinel,
};
