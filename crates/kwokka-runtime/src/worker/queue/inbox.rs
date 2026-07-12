//! Per-worker spawn inbox -- the deferred channel a polled task uses to
//! request child spawns without re-entering the slab it lives in.
//!
//! A task being polled cannot touch its worker's task slab: the run-loop
//! holds `&mut tasks` across the poll, so a second borrow to insert a child
//! would alias the slab. Instead the polled task moves an erased child cell
//! into this inbox -- a worker field disjoint from `tasks` -- and the worker
//! drains it after the poll returns, when the slab borrow has ended. This
//! mirrors the deferral the wake registry already uses for self-wakes.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::task::{TaskRef, cell::slot::TaskSlot};

/// Per-worker spawn inbox capacity. A power of two, sized to absorb a burst of
/// child spawns within one poll before the worker drains the inbox.
pub(crate) const SPAWN_INBOX_CAPACITY: usize = 64;

/// One pending child spawn carried across the deferral boundary.
///
/// Holds the spawning task and the type-erased child cell (the future is
/// already moved into a stack [`TaskSlot`], so this carries no heap state).
/// The child id and slab insertion are stamped by the worker at drain time.
pub(crate) struct PendingSpawn {
    /// The task that requested the spawn -- the child's structured parent.
    pub(crate) parent: TaskRef,
    /// The type-erased child task cell, future already moved in.
    pub(crate) cell: TaskSlot,
}

/// Fixed-capacity ring of pending child spawns, drained once per tick.
///
/// `N` must be a power of two. The bound caps per-worker memory under a
/// spawn storm; a full inbox hands the cell back so the caller owns the
/// backpressure policy. No allocation after construction.
pub(crate) struct SpawnInbox<const N: usize> {
    slots: [Option<PendingSpawn>; N],
    head: usize,
    tail: usize,
}

impl<const N: usize> SpawnInbox<N> {
    /// Creates an empty inbox.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is not a power of two or is zero.
    pub(crate) const fn new() -> Self {
        const {
            assert!(
                N > 0 && N.is_power_of_two(),
                "N must be a positive power of 2"
            );
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            tail: 0,
        }
    }

    /// Pushes a pending spawn to the back of the inbox.
    ///
    /// Returns `Some(request)`, the cell handed back unchanged, when the
    /// inbox is full -- so the caller applies backpressure without dropping
    /// the child future. Returns `None` on success. (`PendingSpawn` is large,
    /// so the give-back uses `Option` rather than a large `Result` `Err`.)
    pub(crate) fn push(&mut self, request: PendingSpawn) -> Option<PendingSpawn> {
        if self.tail.wrapping_sub(self.head) >= N {
            return Some(request);
        }
        self.slots[self.tail & (N - 1)] = Some(request);
        self.tail = self.tail.wrapping_add(1);
        None
    }

    /// Pops the oldest pending spawn, or `None` when the inbox is empty.
    pub(crate) const fn pop(&mut self) -> Option<PendingSpawn> {
        if self.head == self.tail {
            return None;
        }
        let request = self.slots[self.head & (N - 1)].take();
        self.head = self.head.wrapping_add(1);
        request
    }

    /// Number of pending spawns.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    /// `true` when no spawns are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };

    use kwokka_core::id::{Namespace, Pip};

    use super::*;
    use crate::task::cell::header::Slot;

    /// Pending future that records its own drop, so a test can prove the
    /// inbox releases undrained cells.
    struct CountingFuture(&'static AtomicUsize);

    impl Future for CountingFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    impl Drop for CountingFuture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn pending_for(parent_raw: u64, drops: &'static AtomicUsize) -> PendingSpawn {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, CountingFuture(drops)).into_erased();
        PendingSpawn {
            parent: TaskRef::from_raw(parent_raw),
            cell,
        }
    }

    #[test]
    fn push_then_pop_is_fifo() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut inbox = SpawnInbox::<4>::new();
        assert!(inbox.push(pending_for(1, &DROPS)).is_none());
        assert!(inbox.push(pending_for(2, &DROPS)).is_none());
        let Some(first) = inbox.pop() else {
            panic!("pop must yield the first request");
        };
        assert_eq!(first.parent.raw(), TaskRef::from_raw(1).raw());
        let Some(second) = inbox.pop() else {
            panic!("pop must yield the second request");
        };
        assert_eq!(second.parent.raw(), TaskRef::from_raw(2).raw());
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn push_to_full_returns_request() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut inbox = SpawnInbox::<2>::new();
        assert!(inbox.push(pending_for(1, &DROPS)).is_none());
        assert!(inbox.push(pending_for(2, &DROPS)).is_none());
        let Some(rejected) = inbox.push(pending_for(3, &DROPS)) else {
            panic!("a full inbox must hand the request back");
        };
        assert_eq!(rejected.parent.raw(), TaskRef::from_raw(3).raw());
    }

    #[test]
    fn pop_empty_returns_none() {
        let mut inbox = SpawnInbox::<2>::new();
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn len_and_empty_reflect_occupancy() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut inbox = SpawnInbox::<4>::new();
        assert!(inbox.is_empty());
        assert_eq!(inbox.len(), 0);
        assert!(inbox.push(pending_for(1, &DROPS)).is_none());
        assert_eq!(inbox.len(), 1);
        assert!(!inbox.is_empty());
        assert!(inbox.pop().is_some());
        assert!(inbox.is_empty());
    }

    #[test]
    fn wrap_around_reuses_slots() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut inbox = SpawnInbox::<2>::new();
        assert!(inbox.push(pending_for(1, &DROPS)).is_none());
        assert!(inbox.pop().is_some());
        assert!(inbox.push(pending_for(2, &DROPS)).is_none());
        assert!(inbox.push(pending_for(3, &DROPS)).is_none());
        let Some(second) = inbox.pop() else {
            panic!("pop must yield after wrap");
        };
        assert_eq!(second.parent.raw(), TaskRef::from_raw(2).raw());
    }

    #[test]
    fn drop_releases_undrained_cells() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        DROPS.store(0, Ordering::Relaxed);
        {
            let mut inbox = SpawnInbox::<4>::new();
            assert!(inbox.push(pending_for(1, &DROPS)).is_none());
            assert!(inbox.push(pending_for(2, &DROPS)).is_none());
        }
        assert_eq!(
            DROPS.load(Ordering::Relaxed),
            2,
            "dropping the inbox drops every undrained child future via the cell",
        );
    }
}
