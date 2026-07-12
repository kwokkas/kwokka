//! Cross-crate I/O seam -- the boundary that lets sibling crates host
//! completion futures.
//!
//! The runtime installs an [`IoSeam`] for the exact window of each task poll
//! (mirroring its poll-frame discipline) and registers a [`WakerDecoder`] once
//! at startup. An I/O future living outside the runtime crate reaches its
//! worker in three steps: decode the polling task's binding from the waker via
//! [`decode_waker`], submit through [`IoSeam::with_current`] keyed by the
//! decoded worker id, and read the completion result back with
//! [`IoSeam::completion_result`] on a later poll.
//!
//! No scheduler type crosses the boundary: the binding is a `u64` token (the
//! request's `user_data` round-trip key) plus a `u8` worker id. Every op
//! submitted through the seam lands on the same per-poll count the runtime's
//! own submit paths use, so in-flight accounting -- the predicate that pins a
//! task to the worker whose ring holds its op -- is preserved by construction.
//!
//! A future that drops with its op still in flight leaves its cancel in
//! [`cancel`], which the owning worker drains on its next tick.
//!
//! The poll boundary is internal infrastructure for the kwokka workspace crates; it is
//! not re-exported by the `kwokka` facade and carries no stability promise.

pub mod cancel;
pub mod seam;

pub use cancel::{
    ACCEPT_CANCEL_CAPACITY, AcceptCancelSet, CANCEL_INBOX_CAPACITY, CONNECT_CANCEL_CAPACITY,
    CancelInbox, CancelInboxGuard, ConnectCancelSet, MSG_RING_WAKE_USER_DATA,
    PROVIDED_RECV_CANCEL_CAPACITY, ProvidedRecvCancelSet, RECV_CANCEL_INBOX_CAPACITY,
    RecvCancelInbox, RecvCancelInboxGuard, is_cancel_sentinel, is_link_timeout_discard,
    is_msg_ring_wake, is_multishot_sentinel, is_recv_multishot_sentinel,
};
pub use seam::{
    cancel::{
        push_accept_cancel_for_worker, push_cancel_for_worker, push_connect_cancel_for_worker,
        push_multishot_cancel_for_worker, push_provided_recv_cancel_for_worker,
        push_recv_multishot_cancel_for_worker,
    },
    drain::{
        MultishotCompletion, dispose_cancelled_accept, dispose_cancelled_connect,
        dispose_cancelled_op, mark_notif_expected, push_multishot_completion,
        push_recv_multishot_completion, reclaim_cancel_completion, reclaim_dropped_slot,
        reclaim_notif, submit_accept_cancel, submit_cancel, submit_cancel_for,
        submit_connect_cancel, submit_multishot_cancel, submit_provided_recv_cancel,
        submit_recv_multishot_cancel,
    },
    pool::{ProvidedPoolGuard, resolve_provided_recv},
    socket::{adopt_accepted_fd, create_datagram_socket, create_stream_socket},
    state::{
        IoSeam, MultishotAlloc, MultishotNext, RecvMultishotAlloc, RecvMultishotNext, SeamGuard,
        WakeSlot,
    },
    waker::{WakerBinding, WakerDecoder, decode_waker, register_decoder},
};
