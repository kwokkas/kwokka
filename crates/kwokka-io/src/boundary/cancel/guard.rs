//! Run-loop brackets that publish a worker's cancel inboxes.
//!
//! A dropped I/O future reaches its worker's inbox from outside the poll window
//! -- task reap, or an early cancel-drop -- so the inbox pointer cannot ride the
//! poll frame. Each run-loop installs it for the loop's whole lifetime instead,
//! and the guard clears it on the way out.
//!
//! The accessors hand back a raw pointer rather than a reference: the pointee is
//! live for exactly the reasons the guard docs state, and the caller performs the
//! deref under that contract.

use core::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::boundary::cancel::inbox::{
    CANCEL_INBOX_CAPACITY, CancelInbox, RECV_CANCEL_INBOX_CAPACITY, RecvCancelInbox,
};

/// One cancel-inbox slot per possible worker id byte, like the seam array.
const CANCEL_INBOX_SLOTS: usize = u8::MAX as usize + 1;

/// The installed cancel inbox for each worker, or null outside a run-loop,
/// indexed by worker id.
///
/// Unlike the seam array, which is poll-window scoped, this is installed for the
/// worker's whole run-loop: a buffered future's `Drop` runs outside the poll
/// window (task reap, or an early cancel-drop) yet still on the owning worker
/// thread, so the cancel must reach the inbox without the poll bracket.
/// `AtomicPtr<CancelInbox>` is `Sync` regardless of `CancelInbox`, so the array
/// is a sound `static` with no `unsafe impl`.
static CANCEL_INBOXES: [AtomicPtr<CancelInbox<CANCEL_INBOX_CAPACITY>>; CANCEL_INBOX_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; CANCEL_INBOX_SLOTS];

/// RAII bracket that installs a worker's cancel inbox for its whole run-loop
/// and clears it on drop.
///
/// Declared after the `WorkerShard` local in each run-loop entry, so Rust LIFO
/// drop clears the static before the shard -- and its `cancel_inbox` field --
/// is reclaimed. A buffered future dropped during shard teardown then finds a
/// null slot and its [`push_cancel_for_worker`](crate::boundary::push_cancel_for_worker) is a
/// no-op, an accepted bounded leak the same as an overflowed ring.
///
/// Not re-entrant: one run-loop per worker installs one guard.
pub struct CancelInboxGuard {
    /// Worker slot to clear on drop.
    worker_id: u8,
}

impl CancelInboxGuard {
    /// Installs `inbox` for `worker_id` for the run-loop, returning the guard
    /// that clears it.
    ///
    /// Takes `&mut` only to form the pointer; the guard stores no reference, so
    /// the caller's borrow of the inbox ends when this returns and the run-loop
    /// can borrow the owning shard again.
    #[must_use]
    pub fn install(worker_id: u8, inbox: &mut CancelInbox<CANCEL_INBOX_CAPACITY>) -> Self {
        CANCEL_INBOXES[worker_id as usize].store(ptr::from_mut(inbox), Ordering::Release);
        Self { worker_id }
    }
}

impl Drop for CancelInboxGuard {
    fn drop(&mut self) {
        CANCEL_INBOXES[self.worker_id as usize].store(ptr::null_mut(), Ordering::Release);
    }
}

/// One recv-cancel-inbox slot per possible worker id byte, like the seam array.
const RECV_CANCEL_INBOX_SLOTS: usize = u8::MAX as usize + 1;

/// The installed recv cancel inbox for each worker, or null outside a run-loop,
/// indexed by worker id.
///
/// Run-loop scoped like [`CANCEL_INBOXES`]: a recv stream's `Drop` runs outside
/// the poll window (task reap, or an early cancel-drop) yet still on the owning
/// worker thread, so the cancel must reach the inbox without the poll bracket. A
/// dedicated static keeps recv cancels off the shared [`CancelInbox`] ring, whose
/// inline capacity sits at the shard's stack-frame budget.
/// `AtomicPtr<RecvCancelInbox>` is `Sync` regardless of `RecvCancelInbox`, so the
/// array is a sound `static` with no `unsafe impl`.
static RECV_CANCEL_INBOXES: [AtomicPtr<RecvCancelInbox<RECV_CANCEL_INBOX_CAPACITY>>;
    RECV_CANCEL_INBOX_SLOTS] = [const { AtomicPtr::new(ptr::null_mut()) }; RECV_CANCEL_INBOX_SLOTS];

/// RAII bracket that installs a worker's recv cancel inbox for its whole run-loop
/// and clears it on drop.
///
/// Declared after the `WorkerShard` local in each run-loop entry, so Rust LIFO
/// drop clears the static before the shard -- and its `recv_cancel_inbox` field
/// -- is reclaimed. A recv stream dropped during shard teardown then finds a null
/// slot and its
/// [`push_recv_multishot_cancel_for_worker`](crate::boundary::push_recv_multishot_cancel_for_worker)
/// is a no-op, an accepted bounded leak the same as an overflowed ring.
///
/// Not re-entrant: one run-loop per worker installs one guard.
pub struct RecvCancelInboxGuard {
    /// Worker slot to clear on drop.
    worker_id: u8,
}

impl RecvCancelInboxGuard {
    /// Installs `inbox` for `worker_id` for the run-loop, returning the guard
    /// that clears it.
    ///
    /// Takes `&mut` only to form the pointer; the guard stores no reference, so
    /// the caller's borrow of the inbox ends when this returns and the run-loop
    /// can borrow the owning shard again.
    #[must_use]
    pub fn install(worker_id: u8, inbox: &mut RecvCancelInbox<RECV_CANCEL_INBOX_CAPACITY>) -> Self {
        RECV_CANCEL_INBOXES[worker_id as usize].store(ptr::from_mut(inbox), Ordering::Release);
        Self { worker_id }
    }
}

impl Drop for RecvCancelInboxGuard {
    fn drop(&mut self) {
        RECV_CANCEL_INBOXES[self.worker_id as usize].store(ptr::null_mut(), Ordering::Release);
    }
}

/// The cancel inbox installed for `worker_id`, or `None` outside a run-loop.
///
/// The pointee is live for exactly the reasons [`CancelInboxGuard`] states: the
/// guard is declared after the owning `WorkerShard`, so LIFO drop nulls the slot
/// before the shard's field is reclaimed. The caller derefs under that contract
/// and under the single-writer rule the push sites document.
pub(crate) fn cancel_inbox(worker_id: u8) -> Option<NonNull<CancelInbox<CANCEL_INBOX_CAPACITY>>> {
    NonNull::new(CANCEL_INBOXES[worker_id as usize].load(Ordering::Acquire))
}

/// The recv cancel inbox installed for `worker_id`, or `None` outside a run-loop.
///
/// Carries the same liveness contract as [`cancel_inbox`], through
/// [`RecvCancelInboxGuard`].
pub(crate) fn recv_cancel_inbox(
    worker_id: u8,
) -> Option<NonNull<RecvCancelInbox<RECV_CANCEL_INBOX_CAPACITY>>> {
    NonNull::new(RECV_CANCEL_INBOXES[worker_id as usize].load(Ordering::Acquire))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        boundary::{
            push_cancel_for_worker, push_recv_multishot_cancel_for_worker, reserve_worker_id,
        },
        buffer::{multishot::RecvMultishotSlotKey, oneshot::inflight::InflightSlotKey},
    };

    #[test]
    fn cancel_guard_routes_then_clears() {
        let worker_id = reserve_worker_id();
        let mut inbox = CancelInbox::<CANCEL_INBOX_CAPACITY>::new();
        {
            let _guard = CancelInboxGuard::install(worker_id, &mut inbox);
            push_cancel_for_worker(InflightSlotKey {
                slot: 1,
                generation: 0,
                worker_id,
                op_token: 0xBEEF,
            });
        }
        // The guard dropped, so the static is null and this push is a no-op.
        push_cancel_for_worker(InflightSlotKey {
            slot: 2,
            generation: 0,
            worker_id,
            op_token: 0,
        });
        let Some(key) = inbox.pop() else {
            panic!("the in-guard push reached the inbox");
        };
        assert_eq!(key.slot, 1);
        assert_eq!(key.op_token, 0xBEEF);
        assert!(inbox.pop().is_none(), "the post-guard push was a no-op");
    }

    #[test]
    fn recv_cancel_inbox_guard_routes_push() {
        let Ok(mut inbox) = RecvCancelInbox::<RECV_CANCEL_INBOX_CAPACITY>::new() else {
            panic!("the inbox mmap must succeed");
        };
        let worker_id = reserve_worker_id();
        let key = RecvMultishotSlotKey {
            slot: 5,
            generation: 9,
            worker_id,
        };
        // With no guard installed, the push finds a null slot and is a no-op.
        push_recv_multishot_cancel_for_worker(key);
        {
            let _guard = RecvCancelInboxGuard::install(worker_id, &mut inbox);
            push_recv_multishot_cancel_for_worker(key);
        }
        // The guard cleared the slot on drop; the one routed push is still queued.
        assert_eq!(
            inbox.pop(),
            Some(key),
            "the guard routed the push into the worker's inbox",
        );
        assert_eq!(inbox.pop(), None, "the no-guard push was a no-op");
    }
}
