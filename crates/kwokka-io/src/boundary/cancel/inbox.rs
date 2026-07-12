//! Per-worker cancel rings and sets for dropped in-flight operations.
//!
//! A future whose op is still in flight cannot reclaim its resources on drop --
//! the kernel still holds the buffer pointer, the buffer choice, or the accepted
//! descriptor. It records the pending cancel here instead, and the owning
//! worker drains the record on its next tick.
//!
//! Every ring is single-writer: an in-flight op pins its task to the owning
//! worker, so the drop that pushes runs on the thread that drains. None of them
//! needs an atomic. Overflow is uniformly a bounded leak, never a free under a
//! live kernel access.

use std::io;

use crate::buffer::{inflight::InflightSlotKey, mmap::MmapRegion, multishot::RecvMultishotSlotKey};

/// Per-worker cancel-inbox capacity.
///
/// Sized to hold one cancel per droppable op across the slab-backed
/// per-worker registries -- the buffered-op inflight slab and the multishot
/// slab -- so a worker can queue a cancel for every occupied slot in the same
/// tick. No slab-backed op can drop twice before a drain (its slot stays
/// occupied until the drain reclaims it), so the sum of the two capacities
/// bounds their pending cancels. Slotless ops (dropped accepts and provided
/// recvs) share this window without a reserved share: the worker shard sits
/// at its stack-frame budget, and no ring growth can make an op class with no
/// structural drop bound lossless. Overflow keeps its established meaning --
/// the cancel record is lost, a bounded leak (a slot held to teardown, a
/// descriptor, or a pool buffer id), never a free under a live kernel access.
pub const CANCEL_INBOX_CAPACITY: usize = crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize
    + crate::buffer::multishot::DEFAULT_MULTISHOT_CAP as usize;

/// Fixed-capacity ring of pending cancels for dropped buffered futures.
///
/// A buffered future whose op is still in flight cannot free its bytes on
/// drop -- the kernel still holds the pointer. It instead pushes its
/// [`InflightSlotKey`] here; the owning worker drains the ring each tick,
/// submits a cancel SQE, and marks the slot retire-pending, and the completion
/// drain frees the slot once the kernel signals the op is done.
///
/// The caller keeps an in-flight buffered op pinned to its worker, so every
/// push runs on the owning worker thread. The ring is therefore single-writer
/// and needs no atomics.
///
/// At [`CANCEL_INBOX_CAPACITY`] there is one slot per op that can drop between
/// drains -- across the inflight and multishot slabs -- so overflow is a safety
/// backstop rather than a steady-state case: a full ring drops the cancel, a
/// bounded leak, and the op's own completion still reclaims the slot, so no byte
/// storage leaks permanently.
pub struct CancelInbox<const N: usize> {
    /// Pending cancels, oldest at `head`. `InflightSlotKey` is `Copy`, so a
    /// dropped entry leaks only the cancel request, never owned storage. A
    /// multishot cancel rides the same key with its `op_token` set to the
    /// multishot sentinel, which the drain routes to the multishot registry.
    slots: [Option<InflightSlotKey>; N],
    /// Ring read cursor, always in `[0, N)`.
    head: usize,
    /// Count of queued cancels; `(head + len) % N` is the next write slot.
    len: usize,
}

impl<const N: usize> CancelInbox<N> {
    /// Creates an empty cancel inbox.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is zero.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(N > 0, "N must be positive");
        }
        Self {
            slots: [const { None }; N],
            head: 0,
            len: 0,
        }
    }

    /// Queues a cancel for a dropped buffered future's in-flight op.
    ///
    /// A full ring drops the cancel -- a bounded leak: the op's own completion
    /// still reclaims the slot, so no byte storage leaks. The caller does not
    /// retry; the original CQE frees the slot either way. At
    /// [`CANCEL_INBOX_CAPACITY`] the ring holds every op that can drop between
    /// drains, so the full case is a backstop, not a steady state.
    pub const fn push_cancel(&mut self, key: InflightSlotKey) {
        if self.len >= N {
            return;
        }
        self.slots[(self.head + self.len) % N] = Some(key);
        self.len += 1;
    }

    /// Pops the oldest pending cancel, or `None` when the inbox is empty.
    pub const fn pop(&mut self) -> Option<InflightSlotKey> {
        if self.len == 0 {
            return None;
        }
        let key = self.slots[self.head].take();
        self.head = (self.head + 1) % N;
        self.len -= 1;
        key
    }

    /// Number of pending cancels.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// `true` when no cancels are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for CancelInbox<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-worker capacity for pending multishot recv cancels.
///
/// Sized to the multishot recv registry itself
/// ([`DEFAULT_RECV_MULTISHOT_CAP`](crate::buffer::multishot::DEFAULT_RECV_MULTISHOT_CAP)),
/// so a worker can queue a cancel for every occupied recv slot in one tick. A
/// recv slot stays occupied until its terminal completion frees it, so no slot
/// drops twice before a drain and this window bounds the pending cancels. Kept
/// off the shared [`CancelInbox`] ring -- which sits at the shard's stack-frame
/// budget -- and backed by an `mmap` region so its slots do not inflate the
/// shard's inline frame. Overflow keeps the established meaning: the cancel
/// record is lost, a bounded leak of pool buffer ids reclaimed at pool teardown,
/// never a free under a live kernel access.
pub const RECV_CANCEL_INBOX_CAPACITY: usize =
    crate::buffer::multishot::DEFAULT_RECV_MULTISHOT_CAP as usize;

/// Bytes per queued recv cancel: a `u64` generation, a `u16` slot, and a `u8`
/// worker id, little-endian packed.
const RECV_CANCEL_ENTRY_LEN: usize = 11;

/// Fixed-capacity mmap-backed ring of pending cancels for dropped multishot recv
/// streams.
///
/// A recv stream whose op is still in flight cannot recycle its provided buffers
/// on drop -- the kernel still owns the buffer choice. It instead pushes its
/// [`RecvMultishotSlotKey`] here; the owning worker drains the ring each tick,
/// recycles any queued buffers, marks the slot cancel-pending, and submits a
/// cancel SQE, and the op's terminal completion frees the slot.
///
/// The caller keeps an in-flight recv op pinned to its worker (`io_bound`), so
/// every push runs on the owning worker thread. The ring is therefore
/// single-writer and needs no atomics. Unlike the inline [`CancelInbox`], the
/// slot payload lives in an `mmap` region so the ring's
/// [`RECV_CANCEL_INBOX_CAPACITY`] entries do not inflate the shard's stack frame;
/// only the head/len cursor is inline.
///
/// At [`RECV_CANCEL_INBOX_CAPACITY`] there is one slot per recv op that can drop
/// between drains, so overflow is a safety backstop, not a steady state: a full
/// ring drops the cancel, a bounded leak of pool buffer ids, and the op's own
/// terminal completion still recycles its buffers and frees the slot, so no
/// buffer is freed under a live kernel write.
pub struct RecvCancelInbox<const N: usize> {
    /// mmap-backed ring of `RECV_CANCEL_ENTRY_LEN`-byte packed slot keys, oldest
    /// at `head`.
    storage: MmapRegion,
    /// Ring read cursor, always in `[0, N)`.
    head: usize,
    /// Count of queued cancels; `(head + len) % N` is the next write slot.
    len: usize,
}

impl<const N: usize> RecvCancelInbox<N> {
    /// Creates an empty recv cancel inbox.
    ///
    /// # Errors
    ///
    /// Returns the `mmap` error when backing allocation fails.
    ///
    /// # Panics
    ///
    /// Compile-time panic if `N` is zero.
    pub fn new() -> io::Result<Self> {
        const {
            assert!(N > 0, "N must be positive");
        }
        let storage = MmapRegion::new(N * RECV_CANCEL_ENTRY_LEN)?;
        Ok(Self {
            storage,
            head: 0,
            len: 0,
        })
    }

    /// Queues a cancel for a dropped recv stream's in-flight op.
    ///
    /// A full ring drops the cancel -- a bounded leak: the op's own terminal
    /// completion still recycles its buffers and frees its slot. The caller does
    /// not retry. At [`RECV_CANCEL_INBOX_CAPACITY`] the ring holds every recv op
    /// that can drop between drains, so the full case is a backstop.
    pub fn push_cancel(&mut self, key: RecvMultishotSlotKey) {
        if self.len >= N {
            return;
        }
        let index = (self.head + self.len) % N;
        let offset = index * RECV_CANCEL_ENTRY_LEN;
        let bytes = self.storage.as_mut_slice();
        // Bounds-checked once through `get_mut`; a `None` never occurs (the region
        // is sized `N * RECV_CANCEL_ENTRY_LEN` and `index < N`), but gating here
        // keeps the write panic-free like the recv slab's byte accessors.
        let Some(record) = bytes.get_mut(offset..offset + RECV_CANCEL_ENTRY_LEN) else {
            return;
        };
        record[0..8].copy_from_slice(&key.generation.to_le_bytes());
        record[8..10].copy_from_slice(&key.slot.to_le_bytes());
        record[10] = key.worker_id;
        self.len += 1;
    }

    /// Pops the oldest pending cancel, or `None` when the inbox is empty.
    pub fn pop(&mut self) -> Option<RecvMultishotSlotKey> {
        if self.len == 0 {
            return None;
        }
        let offset = self.head * RECV_CANCEL_ENTRY_LEN;
        let bytes = self.storage.as_slice();
        let record = bytes.get(offset..offset + RECV_CANCEL_ENTRY_LEN)?;
        let Ok(generation) = <[u8; 8]>::try_from(&record[0..8]) else {
            return None;
        };
        let Ok(slot) = <[u8; 2]>::try_from(&record[8..10]) else {
            return None;
        };
        let worker_id = record[10];
        self.head = (self.head + 1) % N;
        self.len -= 1;
        Some(RecvMultishotSlotKey {
            slot: u16::from_le_bytes(slot),
            generation: u64::from_le_bytes(generation),
            worker_id,
        })
    }

    /// Number of pending cancels.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// `true` when no cancels are pending.
    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Per-worker capacity for pending single-shot accept cancels.
///
/// Holds a token from a dropped `accept()` between its cancel submission and the
/// op's completion. The window is short (usually one drain), so a small ring
/// suffices; a full ring drops the record, a bounded leak of one descriptor.
pub const ACCEPT_CANCEL_CAPACITY: usize = 32;

/// [`InflightSlotKey`] `slot` marker for a slotless single-shot accept cancel.
///
/// A single-shot accept carries no inflight slab slot -- it submits under the
/// polling task's token. This reserved slot routes its cancel to
/// [`submit_accept_cancel`](crate::boundary::submit_accept_cancel) rather than the buffered-op
/// path; no real slab slot reaches `u16::MAX` (the inflight cap is far smaller).
pub(crate) const ACCEPT_CANCEL_SLOT: u16 = u16::MAX;

/// Per-worker set of dropped single-shot accepts awaiting their completion.
///
/// A dropped `accept()` cancels its op and records the op's token here; the
/// completion drain closes the accepted fd if the op still produced one, rather
/// than orphaning it in the task wake slot.
pub struct AcceptCancelSet<const N: usize> {
    /// Pending tokens packed in `[0, len)`; order does not matter.
    tokens: [u64; N],
    /// Count of pending tokens.
    len: usize,
}

impl<const N: usize> AcceptCancelSet<N> {
    /// Creates an empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tokens: [0; N],
            len: 0,
        }
    }

    /// Records `token` as a cancelled accept awaiting disposal.
    ///
    /// A full set drops the record: the op's fd is a bounded leak, never
    /// corruption, and the caller does not retry.
    pub(crate) const fn insert(&mut self, token: u64) {
        if self.len < N {
            self.tokens[self.len] = token;
            self.len += 1;
        }
    }

    /// Removes `token` if pending, reporting whether it was.
    pub(crate) const fn take(&mut self, token: u64) -> bool {
        let mut index = 0;
        while index < self.len {
            if self.tokens[index] == token {
                self.tokens[index] = self.tokens[self.len - 1];
                self.len -= 1;
                return true;
            }
            index += 1;
        }
        false
    }

    /// `true` when no cancelled accept is pending.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for AcceptCancelSet<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-worker capacity for pending provided-recv cancels.
///
/// A provided-buffer recv holds no registry slot -- the kernel picks its
/// buffer at completion time -- so nothing structural bounds how many can be
/// dropped between drains. This window tracks the first N pending drops;
/// past it (or past the shared [`CANCEL_INBOX_CAPACITY`] ring feeding it) a
/// drop's cancel record is lost and the op's buffer id is never recycled, a
/// bounded loss of pool entries reclaimed only at pool teardown. Sized to the
/// provided-buffer ring itself -- at most every pool entry can be awaiting
/// disposal at once -- at 8 bytes per slot on the shard.
pub const PROVIDED_RECV_CANCEL_CAPACITY: usize =
    crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize;

/// [`InflightSlotKey`] `slot` marker for a slotless provided-recv cancel.
///
/// A provided-buffer recv carries no inflight slab slot -- it submits under
/// the polling task's token and the kernel owns the buffer choice. This
/// reserved slot routes its cancel to
/// [`submit_provided_recv_cancel`](crate::boundary::submit_provided_recv_cancel) rather
/// than the buffered-op path; it sits one below [`ACCEPT_CANCEL_SLOT`], and no
/// real slab slot reaches either (the inflight cap is far smaller).
pub(crate) const PROVIDED_RECV_CANCEL_SLOT: u16 = u16::MAX - 1;

/// Per-worker set of dropped provided-buffer recvs awaiting their completion.
///
/// A dropped provided recv cancels its op and records the op's token here; the
/// completion drain recycles the kernel-selected buffer if the op still
/// consumed one, rather than orphaning the buffer id in the task wake slot.
pub struct ProvidedRecvCancelSet<const N: usize> {
    /// Pending tokens packed in `[0, len)`; order does not matter.
    tokens: [u64; N],
    /// Count of pending tokens.
    len: usize,
}

impl<const N: usize> ProvidedRecvCancelSet<N> {
    /// Creates an empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tokens: [0; N],
            len: 0,
        }
    }

    /// Records `token` as a cancelled provided recv awaiting disposal.
    ///
    /// A full set drops the record: the op's buffer id is a bounded pool-entry
    /// loss, never corruption, and the caller does not retry.
    pub(crate) const fn insert(&mut self, token: u64) {
        if self.len < N {
            self.tokens[self.len] = token;
            self.len += 1;
        }
    }

    /// Removes `token` if pending, reporting whether it was.
    pub(crate) const fn take(&mut self, token: u64) -> bool {
        let mut index = 0;
        while index < self.len {
            if self.tokens[index] == token {
                self.tokens[index] = self.tokens[self.len - 1];
                self.len -= 1;
                return true;
            }
            index += 1;
        }
        false
    }

    /// `true` when no cancelled provided recv is pending.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for ProvidedRecvCancelSet<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-worker capacity for pending single-shot connect cancels.
///
/// Holds a token from a dropped `connect()` between its cancel submission and
/// the op's completion. A connect submits at most once per worker (the driver
/// packs the address into its single submission scratch), so the window holds
/// few tokens; a full set drops the record, at worst re-exposing the stray-wake
/// window for that one token, never corruption.
pub const CONNECT_CANCEL_CAPACITY: usize = 8;

/// [`InflightSlotKey`] `slot` marker for a slotless single-shot connect cancel.
///
/// A single-shot connect carries no inflight slab slot -- it submits under the
/// polling task's token. This reserved slot routes its cancel to
/// [`submit_connect_cancel`](crate::boundary::submit_connect_cancel) rather than the buffered-op
/// path; it sits one below [`PROVIDED_RECV_CANCEL_SLOT`], and no real slab slot reaches any of them
/// (the inflight cap is far smaller).
pub(crate) const CONNECT_CANCEL_SLOT: u16 = u16::MAX - 2;

/// Per-worker set of dropped single-shot connects awaiting their completion.
///
/// A dropped `connect()` cancels its op and records the op's token here. Unlike
/// accept, a connect produces no descriptor (success is result `0`, not an fd),
/// so the completion drain disposes nothing -- the set exists to divert the
/// belated CQE out of the generic task-token path, so a stray result never
/// overwrites a live task's wake slot.
pub struct ConnectCancelSet<const N: usize> {
    /// Pending tokens packed in `[0, len)`; order does not matter.
    tokens: [u64; N],
    /// Count of pending tokens.
    len: usize,
}

impl<const N: usize> ConnectCancelSet<N> {
    /// Creates an empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tokens: [0; N],
            len: 0,
        }
    }

    /// Records `token` as a cancelled connect awaiting disposal.
    ///
    /// A full set drops the record: the belated CQE then re-enters the generic
    /// path for that one token, never corruption, and the caller does not retry.
    pub(crate) const fn insert(&mut self, token: u64) {
        if self.len < N {
            self.tokens[self.len] = token;
            self.len += 1;
        }
    }

    /// Removes `token` if pending, reporting whether it was.
    pub(crate) const fn take(&mut self, token: u64) -> bool {
        let mut index = 0;
        while index < self.len {
            if self.tokens[index] == token {
                self.tokens[index] = self.tokens[self.len - 1];
                self.len -= 1;
                return true;
            }
            index += 1;
        }
        false
    }

    /// `true` when no cancelled connect is pending.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for ConnectCancelSet<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cancel_key(slot: u16) -> InflightSlotKey {
        InflightSlotKey {
            slot,
            generation: 0,
            worker_id: 3,
            op_token: u64::from(slot),
        }
    }

    #[test]
    fn cancel_push_pop_fifo() {
        let mut inbox = CancelInbox::<4>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        let Some(first) = inbox.pop() else {
            panic!("pop must yield the first cancel");
        };
        assert_eq!(first.slot, 0);
        let Some(second) = inbox.pop() else {
            panic!("pop must yield the second cancel");
        };
        assert_eq!(second.slot, 1);
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_full_inbox_leaks() {
        let mut inbox = CancelInbox::<2>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        inbox.push_cancel(cancel_key(2));
        assert_eq!(
            inbox.len(),
            2,
            "a full inbox drops the overflow cancel as a bounded leak"
        );
        let Some(first) = inbox.pop() else {
            panic!("the queued cancels survive the overflow");
        };
        assert_eq!(
            first.slot, 0,
            "the overflow did not displace a queued cancel"
        );
    }

    #[test]
    fn cancel_inbox_capacity_covers_both_slabs() {
        let droppable = crate::buffer::inflight::DEFAULT_INFLIGHT_CAP as usize
            + crate::buffer::multishot::DEFAULT_MULTISHOT_CAP as usize;
        assert!(
            CANCEL_INBOX_CAPACITY >= droppable,
            "the inbox holds a cancel for every op that can drop between drains, \
             so no worker cancel is silently dropped in production",
        );
    }

    #[test]
    fn cancel_inbox_wraps_across_capacity() {
        // Fill, drain half, refill: exercises the modulo write past the array end
        // so the ring stays correct without a power-of-two capacity.
        let mut inbox = CancelInbox::<3>::new();
        inbox.push_cancel(cancel_key(0));
        inbox.push_cancel(cancel_key(1));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(0));
        inbox.push_cancel(cancel_key(2));
        inbox.push_cancel(cancel_key(3));
        assert_eq!(inbox.len(), 3, "the ring refilled to capacity after wrap");
        assert_eq!(inbox.pop().map(|k| k.slot), Some(1));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(2));
        assert_eq!(inbox.pop().map(|k| k.slot), Some(3));
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_pop_empty_returns_none() {
        let mut inbox = CancelInbox::<2>::new();
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn cancel_len_empty_occupancy() {
        let mut inbox = CancelInbox::<4>::new();
        assert!(inbox.is_empty());
        assert_eq!(inbox.len(), 0);
        inbox.push_cancel(cancel_key(0));
        assert_eq!(inbox.len(), 1);
        assert!(!inbox.is_empty());
        assert!(inbox.pop().is_some());
        assert!(inbox.is_empty());
    }

    #[test]
    fn cancel_wrap_around_reuses_slots() {
        let mut inbox = CancelInbox::<2>::new();
        inbox.push_cancel(cancel_key(0));
        assert!(inbox.pop().is_some());
        inbox.push_cancel(cancel_key(1));
        inbox.push_cancel(cancel_key(2));
        let Some(second) = inbox.pop() else {
            panic!("pop must yield after wrap");
        };
        assert_eq!(second.slot, 1);
    }

    #[test]
    fn cancel_default_is_empty() {
        let inbox = CancelInbox::<4>::default();
        assert!(inbox.is_empty());
    }

    #[test]
    fn recv_cancel_inbox_push_pop_is_fifo() {
        let Ok(mut inbox) = RecvCancelInbox::<4>::new() else {
            panic!("the inbox mmap must succeed");
        };
        assert!(inbox.is_empty());
        let key = |slot, generation| RecvMultishotSlotKey {
            slot,
            generation,
            worker_id: 7,
        };
        inbox.push_cancel(key(1, 100));
        inbox.push_cancel(key(2, 200));
        assert_eq!(inbox.len(), 2);
        assert_eq!(inbox.pop(), Some(key(1, 100)), "oldest pops first");
        assert_eq!(inbox.pop(), Some(key(2, 200)));
        assert_eq!(inbox.pop(), None);
        assert!(inbox.is_empty());
    }

    #[test]
    fn recv_cancel_inbox_drops_on_overflow() {
        let Ok(mut inbox) = RecvCancelInbox::<2>::new() else {
            panic!("the inbox mmap must succeed");
        };
        let key = |slot| RecvMultishotSlotKey {
            slot,
            generation: 0,
            worker_id: 3,
        };
        inbox.push_cancel(key(0));
        inbox.push_cancel(key(1));
        inbox.push_cancel(key(2));
        assert_eq!(inbox.len(), 2, "a full ring drops the newest push");
        assert_eq!(inbox.pop(), Some(key(0)));
        assert_eq!(inbox.pop(), Some(key(1)));
        assert_eq!(inbox.pop(), None, "the overflowing push was dropped");
    }

    #[test]
    fn accept_cancel_set_tracks_tokens() {
        let mut set = AcceptCancelSet::<4>::new();
        assert!(set.is_empty());
        set.insert(0xAA);
        set.insert(0xBB);
        assert!(!set.is_empty());
        assert!(set.take(0xAA), "a recorded token is pending");
        assert!(!set.take(0xAA), "a taken token is no longer pending");
        assert!(set.take(0xBB));
        assert!(set.is_empty());
    }

    #[test]
    fn accept_cancel_set_full_drops_the_record() {
        let mut set = AcceptCancelSet::<2>::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);
        assert!(set.take(1));
        assert!(set.take(2));
        assert!(!set.take(3), "a full set drops the overflow record");
    }

    #[test]
    fn provided_recv_cancel_set_inserts_and_takes() {
        let mut cancels = ProvidedRecvCancelSet::<2>::new();
        assert!(cancels.is_empty());
        cancels.insert(0xA);
        cancels.insert(0xB);
        // A full set drops the record rather than growing.
        cancels.insert(0xC);
        assert!(!cancels.take(0xC), "the overflowed record was dropped");
        assert!(cancels.take(0xB));
        assert!(cancels.take(0xA));
        assert!(cancels.is_empty());
        assert!(!cancels.take(0xA), "a taken token does not linger");
    }

    #[test]
    fn connect_cancel_set_inserts_and_takes() {
        let mut cancels = ConnectCancelSet::<2>::new();
        assert!(cancels.is_empty());
        cancels.insert(0xA);
        cancels.insert(0xB);
        // A full set drops the record rather than growing.
        cancels.insert(0xC);
        assert!(!cancels.take(0xC), "the overflowed record was dropped");
        assert!(cancels.take(0xB));
        assert!(cancels.take(0xA));
        assert!(cancels.is_empty());
        assert!(!cancels.take(0xA), "a taken token does not linger");
    }
}
