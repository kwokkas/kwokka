//! Queues a cancel from a dropped future onto its owning worker.

use crate::{
    boundary::cancel::{
        ACCEPT_CANCEL_SLOT, CONNECT_CANCEL_SLOT, PROVIDED_RECV_CANCEL_SLOT,
        encode_multishot_sentinel,
        guard::{cancel_inbox, recv_cancel_inbox},
    },
    buffer::{
        multishot::{MultishotSlotKey, RecvMultishotSlotKey},
        oneshot::inflight::InflightSlotKey,
    },
};

/// Queues a cancel for a dropped buffered future's in-flight op on its worker.
///
/// A no-op when no inbox is installed for `key.worker_id`: the worker's
/// run-loop already tore down, so the op's own completion frees the slot during
/// shutdown, a bounded leak like an overflowed ring.
pub fn push_cancel_for_worker(key: InflightSlotKey) {
    let Some(mut inbox) = cancel_inbox(key.worker_id) else {
        return;
    };
    // SAFETY: Invariant -- a non-null pointer from `cancel_inbox(key.worker_id)`
    // was stored by `CancelInboxGuard::install` over the owning `WorkerShard`'s
    // `cancel_inbox` field. The guard is declared after the shard in the
    // run-loop entry, so Rust LIFO drop nulls this slot before the shard, and
    // its field, is reclaimed; a non-null load therefore names a live field.
    // Precondition (why the single-writer contract holds): a buffered future's
    // `Drop` fires only on the owning worker's thread, because submitting a
    // buffered op sets `header.io_bound = true` in the post-poll fold, which
    // pins the task off the steal path so it never migrates to another worker.
    // Callers must invoke this only from such a future's `Drop`, so the worker
    // that installed the inbox is the only thread that ever pushes -- the inbox
    // needs no atomics. A future that cleared `io_bound`, or a non-buffered
    // future reaching here, would break that contract and must re-establish it.
    // Failure mode: null is the early return above. A cross-thread push (a task
    // with `io_bound = false`) races the single writer; the call-site invariant
    // excludes it. A dangling pointer cannot arise -- LIFO drop order excludes it.
    let inbox = unsafe { inbox.as_mut() };
    inbox.push_cancel(key);
}

/// Queues a cancel for a dropped single-shot accept on its worker.
///
/// The accept op carries the polling task's `token` as its `user_data` and holds
/// no inflight slab slot, so the queued key uses the `ACCEPT_CANCEL_SLOT`
/// marker; the drain routes it to [`submit_accept_cancel`](crate::boundary::submit_accept_cancel).
/// Submitting the accept set the task `io_bound`, so its `Drop` runs on the owning worker and the
/// push is single-writer, the same contract
/// [`push_cancel_for_worker`] holds.
pub fn push_accept_cancel_for_worker(worker_id: u8, token: u64) {
    push_cancel_for_worker(InflightSlotKey {
        slot: ACCEPT_CANCEL_SLOT,
        generation: 0,
        worker_id,
        op_token: token,
    });
}

/// Queues a cancel for a dropped provided-buffer recv on its worker.
///
/// The recv op carries the polling task's `token` as its `user_data` and holds
/// no inflight slab slot, so the queued key uses the
/// `PROVIDED_RECV_CANCEL_SLOT` marker; the drain routes it to
/// [`submit_provided_recv_cancel`](crate::boundary::submit_provided_recv_cancel). Submitting the
/// recv set the task `io_bound`, so its `Drop` runs on the owning worker and the push is
/// single-writer, the same contract [`push_accept_cancel_for_worker`] holds.
pub fn push_provided_recv_cancel_for_worker(worker_id: u8, token: u64) {
    push_cancel_for_worker(InflightSlotKey {
        slot: PROVIDED_RECV_CANCEL_SLOT,
        generation: 0,
        worker_id,
        op_token: token,
    });
}

/// Queues a cancel for a dropped single-shot connect on its worker.
///
/// The connect op carries the polling task's `token` as its `user_data` and
/// holds no inflight slab slot, so the queued key uses the `CONNECT_CANCEL_SLOT`
/// marker; the drain routes it to
/// [`submit_connect_cancel`](crate::boundary::submit_connect_cancel). Submitting the connect set
/// the task `io_bound`, so its `Drop` runs on the owning worker and the push is single-writer, the
/// same contract [`push_accept_cancel_for_worker`] holds.
pub fn push_connect_cancel_for_worker(worker_id: u8, token: u64) {
    push_cancel_for_worker(InflightSlotKey {
        slot: CONNECT_CANCEL_SLOT,
        generation: 0,
        worker_id,
        op_token: token,
    });
}

/// Queues a cancel for a dropped multishot stream's op on its worker.
///
/// The stream's `Drop` calls this. Like a buffered future, a live multishot op
/// is `io_bound`, so the drop runs on the owning worker thread and the push is
/// single-writer. The multishot slot rides an [`InflightSlotKey`] whose
/// `op_token` is the multishot sentinel; the worker's cancel drain routes it to
/// the multishot registry. A no-op when no inbox is installed (a bounded leak at
/// worker teardown, reclaimed by the op's terminal completion).
pub fn push_multishot_cancel_for_worker(key: MultishotSlotKey) {
    push_cancel_for_worker(InflightSlotKey {
        slot: key.slot,
        generation: key.generation,
        worker_id: key.worker_id,
        op_token: encode_multishot_sentinel(key),
    });
}

/// Queues a cancel for a dropped multishot recv stream on its owning worker.
///
/// The dropped stream's op is `io_bound`, so the drop runs on the owning worker
/// and the push is single-writer, the same contract
/// [`push_cancel_for_worker`] holds. Unlike a buffered-op
/// cancel, this pushes into the dedicated [`RecvCancelInbox`](crate::boundary::RecvCancelInbox)
/// rather than the shared [`CancelInbox`](crate::boundary::CancelInbox) ring: a recv slot
/// is per-connection, so the worker drains it separately through
/// [`submit_recv_multishot_cancel`](crate::boundary::submit_recv_multishot_cancel). A no-op when no
/// inbox is installed for `key.worker_id` (a bounded leak at worker teardown, reclaimed by the op's
/// terminal completion).
pub fn push_recv_multishot_cancel_for_worker(key: RecvMultishotSlotKey) {
    let Some(mut inbox) = recv_cancel_inbox(key.worker_id) else {
        return;
    };
    // SAFETY: Invariant -- a non-null pointer from
    // `recv_cancel_inbox(key.worker_id)` was stored by
    // `RecvCancelInboxGuard::install` over the owning `WorkerShard`'s
    // `recv_cancel_inbox` field. The guard is declared after the shard in the
    // run-loop entry, so Rust LIFO drop nulls this slot before the shard, and its
    // field, is reclaimed; a non-null load therefore names a live field.
    // Precondition (why the single-writer contract holds): a recv stream's `Drop`
    // fires only on the owning worker's thread, because submitting the multishot
    // recv set `header.io_bound = true`, which pins the task off the steal path
    // so it never migrates. Callers must invoke this only from such a stream's
    // `Drop`, so the worker that installed the inbox is the only thread that ever
    // pushes -- the inbox needs no atomics.
    // Failure mode: null is the early return above. A cross-thread push races the
    // single writer; the call-site invariant excludes it. A dangling pointer
    // cannot arise -- LIFO drop order excludes it.
    let inbox = unsafe { inbox.as_mut() };
    inbox.push_cancel(key);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::{
        cancel::{CANCEL_INBOX_CAPACITY, CancelInbox, CancelInboxGuard},
        reserve_worker_id,
    };

    #[test]
    fn push_accept_cancel_carries_the_slotless_marker() {
        let worker_id = reserve_worker_id();
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        {
            let _guard = CancelInboxGuard::install(worker_id, &mut inbox);
            push_accept_cancel_for_worker(worker_id, 0xABCD);
        }
        let Some(key) = inbox.pop() else {
            panic!("the accept cancel reached the inbox");
        };
        assert_eq!(
            key.slot, ACCEPT_CANCEL_SLOT,
            "the slotless marker rides along"
        );
        assert_eq!(key.op_token, 0xABCD);
        assert_eq!(key.worker_id, worker_id);
    }

    #[test]
    fn push_connect_cancel_carries_the_slotless_marker() {
        let worker_id = reserve_worker_id();
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        {
            let _guard = CancelInboxGuard::install(worker_id, &mut inbox);
            push_connect_cancel_for_worker(worker_id, 0xC0DE);
        }
        let Some(key) = inbox.pop() else {
            panic!("the connect cancel reached the inbox");
        };
        assert_eq!(
            key.slot, CONNECT_CANCEL_SLOT,
            "the slotless connect marker rides along",
        );
        assert_ne!(key.slot, ACCEPT_CANCEL_SLOT);
        assert_ne!(key.slot, PROVIDED_RECV_CANCEL_SLOT);
        assert_eq!(key.op_token, 0xC0DE);
        assert_eq!(key.worker_id, worker_id);
    }
}
