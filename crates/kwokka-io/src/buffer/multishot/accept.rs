//! Per-worker registry for in-flight multishot operations.
//!
//! A multishot op submits one SQE that posts many CQEs sharing one
//! `user_data`. The per-task wake slot holds a single result, so those CQEs
//! cannot route through the task header without dropping all but the last one
//! per drain batch. This registry owns a small FIFO per multishot op instead:
//! the completion drain pushes each CQE result into the op's slot and wakes the
//! owning task, which drains the FIFO on its next poll.
//!
//! Zero-heap: fixed-capacity inline storage, never grown. Pure safe Rust --
//! multishot accept carries only `i32` results (an accepted fd or a negative
//! errno), no byte buffer, so no mmap and no raw pointers.
//!
//! Generational: each slot carries a generation bumped on free, so a stale
//! sentinel naming a reused slot is rejected rather than routed to the new
//! occupant.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) restricts slab internals inside the now-pub multishot module"
)]

/// Multishot slots per worker, kept small.
///
/// A worker runs a handful of multishot registrations (one per listener today),
/// and the whole registry is stored inline in the worker shard, so a low slot
/// count bounds the shard's stack frame. The per-connection recv case is
/// revisited when recv lands; it needs an mmap-backed store to scale past this.
pub const DEFAULT_MULTISHOT_CAP: u16 = 8;

/// Per-slot completion FIFO depth, sized to the runtime completion-drain batch
/// so one drain pass of same-op CQEs never overflows.
///
/// Cross-crate invariant: `kwokka_runtime`'s completion-drain batch must not
/// exceed this. kwokka-io cannot import that const (the dependency graph runs
/// io -> runtime, not the reverse), so this is `pub` and a runtime-side test
/// asserts the batch stays within it.
pub const MULTISHOT_FIFO_DEPTH: u16 = 64;

/// Slot-count ceiling that sizes the inline bitmaps and per-slot tables.
const MAX_MULTISHOT_SLOTS: usize = DEFAULT_MULTISHOT_CAP as usize;

/// Bitmap words covering [`MAX_MULTISHOT_SLOTS`].
const BITMAP_WORDS: usize = MAX_MULTISHOT_SLOTS.div_ceil(64);

/// A Copy handle into the multishot registry.
///
/// Held by a multishot stream. The `generation` guards slot reuse: once the
/// slot is freed its generation is bumped, so a stale stream or a stale
/// sentinel that still names the old generation is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultishotSlotKey {
    /// Slot index in the owning worker's registry.
    pub(crate) slot: u16,
    /// Generation captured at allocation.
    pub(crate) generation: u64,
    /// Worker whose registry owns the slot.
    pub(crate) worker_id: u8,
}

/// Outcome of pushing a completion into a slot's FIFO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultishotPush {
    /// The result was queued for the consumer.
    Queued,
    /// The FIFO was full; the caller owns disposing of the result (an accepted
    /// fd must be closed, since the kernel already created it).
    Overflowed,
    /// The sentinel named a freed or reused slot; not queued. Carries the same
    /// disposal contract as `Overflowed`: a positive accept result is a
    /// kernel-created fd the caller must close, or it leaks.
    Stale,
}

/// Per-worker fixed-capacity multishot registry.
pub struct MultishotSlab {
    /// Per-slot ring of pending completion results.
    results: [[i32; MULTISHOT_FIFO_DEPTH as usize]; MAX_MULTISHOT_SLOTS],
    /// Per-slot ring read cursor.
    head: [u16; MAX_MULTISHOT_SLOTS],
    /// Per-slot count of queued results.
    len: [u16; MAX_MULTISHOT_SLOTS],
    occupied: [u64; BITMAP_WORDS],
    /// The op posted its final (no-`MORE`) CQE; no more results will arrive.
    terminated: [u64; BITMAP_WORDS],
    /// A cancel was submitted for the op; its final CQE frees the slot.
    cancel_pending: [u64; BITMAP_WORDS],
    /// Per-slot generation, bumped on free. A `u64` makes the ABA window
    /// effectively unbounded.
    generation: [u64; MAX_MULTISHOT_SLOTS],
    /// Per-slot owning task token, woken when a result lands in the FIFO.
    owner_token: [u64; MAX_MULTISHOT_SLOTS],
    worker_id: u8,
    cap: u16,
}

impl MultishotSlab {
    /// Builds a registry of `cap` slots (clamped to [`DEFAULT_MULTISHOT_CAP`])
    /// for `worker_id`. Infallible: the backing is inline, not mmap.
    #[must_use]
    pub fn new(worker_id: u8, cap: u16) -> Self {
        Self {
            results: [[0; MULTISHOT_FIFO_DEPTH as usize]; MAX_MULTISHOT_SLOTS],
            head: [0; MAX_MULTISHOT_SLOTS],
            len: [0; MAX_MULTISHOT_SLOTS],
            occupied: [0; BITMAP_WORDS],
            terminated: [0; BITMAP_WORDS],
            cancel_pending: [0; BITMAP_WORDS],
            generation: [0; MAX_MULTISHOT_SLOTS],
            owner_token: [0; MAX_MULTISHOT_SLOTS],
            worker_id,
            cap: cap.min(DEFAULT_MULTISHOT_CAP),
        }
    }

    /// Allocates the first free slot for a multishot op, returning its handle.
    ///
    /// `owner_token` is the task woken on each completion. The op's cancel
    /// target is the multishot sentinel derived from the returned key, so no
    /// token is passed in. Returns `None` when every slot is occupied.
    pub(crate) fn allocate(&mut self, owner_token: u64) -> Option<MultishotSlotKey> {
        let slot = self.first_free()?;
        let (word, bit) = word_bit(slot);
        self.occupied[word] |= 1u64 << bit;
        self.terminated[word] &= !(1u64 << bit);
        self.cancel_pending[word] &= !(1u64 << bit);
        self.head[slot as usize] = 0;
        self.len[slot as usize] = 0;
        self.owner_token[slot as usize] = owner_token;
        Some(MultishotSlotKey {
            slot,
            generation: self.generation[slot as usize],
            worker_id: self.worker_id,
        })
    }

    /// Queues a completion `result` for `slot`, marking the slot terminated when
    /// `is_more` is clear. `generation_low16` rejects a stale sentinel.
    ///
    /// The drain matches this slot only while occupied at the sentinel's
    /// generation; a freed or reused slot yields [`MultishotPush::Stale`].
    pub(crate) const fn push(
        &mut self,
        slot: u16,
        generation_low16: u16,
        result: i32,
        is_more: bool,
    ) -> MultishotPush {
        if !self.is_slot_live(slot, generation_low16) {
            return MultishotPush::Stale;
        }
        if !is_more {
            let (word, bit) = word_bit(slot);
            self.terminated[word] |= 1u64 << bit;
        }
        let slot_idx = slot as usize;
        if self.len[slot_idx] >= MULTISHOT_FIFO_DEPTH {
            return MultishotPush::Overflowed;
        }
        let index = (self.head[slot_idx] + self.len[slot_idx]) % MULTISHOT_FIFO_DEPTH;
        self.results[slot_idx][index as usize] = result;
        self.len[slot_idx] += 1;
        MultishotPush::Queued
    }

    /// Pops the oldest queued result for `key`, or `None` when the FIFO is empty
    /// or `key` is stale.
    pub(crate) const fn pop(&mut self, key: MultishotSlotKey) -> Option<i32> {
        if !self.is_live(key) || self.len[key.slot as usize] == 0 {
            return None;
        }
        let slot_idx = key.slot as usize;
        let head = self.head[slot_idx];
        let result = self.results[slot_idx][head as usize];
        self.head[slot_idx] = (head + 1) % MULTISHOT_FIFO_DEPTH;
        self.len[slot_idx] -= 1;
        Some(result)
    }

    /// Returns the owning task token for `slot` at the sentinel's generation, or
    /// `None` when the slot is stale. The drain calls this to wake the owner.
    pub(crate) const fn owner(&self, slot: u16, generation_low16: u16) -> Option<u64> {
        if !self.is_slot_live(slot, generation_low16) {
            return None;
        }
        Some(self.owner_token[slot as usize])
    }

    /// Returns whether `key`'s op has posted its final CQE.
    pub(crate) const fn is_terminated(&self, key: MultishotSlotKey) -> bool {
        if !self.is_live(key) {
            return false;
        }
        let (word, bit) = word_bit(key.slot);
        self.terminated[word] & (1u64 << bit) != 0
    }

    /// Marks `key`'s slot cancel-pending. A stale handle is a no-op.
    pub(crate) const fn mark_cancel_pending(&mut self, key: MultishotSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.cancel_pending[word] |= 1u64 << bit;
    }

    /// Frees `key`'s slot, bumping its generation. A stale handle is a no-op.
    pub(crate) const fn free(&mut self, key: MultishotSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.occupied[word] &= !(1u64 << bit);
        self.terminated[word] &= !(1u64 << bit);
        self.cancel_pending[word] &= !(1u64 << bit);
        self.len[key.slot as usize] = 0;
        let generation = &mut self.generation[key.slot as usize];
        *generation = generation.wrapping_add(1);
    }

    /// Whether `slot` is cancel-pending at `generation_low16`.
    ///
    /// The completion drain reads this by the sentinel's slot and low-16
    /// generation to tell a dropped stream's op (cancel-pending) from a live one.
    pub(crate) const fn is_cancel_pending(&self, slot: u16, generation_low16: u16) -> bool {
        if !self.is_slot_live(slot, generation_low16) {
            return false;
        }
        let (word, bit) = word_bit(slot);
        self.cancel_pending[word] & (1u64 << bit) != 0
    }

    /// Frees `slot` when occupied at `generation_low16`, bumping its generation.
    ///
    /// The drain calls this on the terminal CQE of a cancel-pending op, keyed by
    /// the sentinel's slot and low-16 generation; a stale slot is a no-op.
    pub(crate) const fn free_by_slot(&mut self, slot: u16, generation_low16: u16) {
        if !self.is_slot_live(slot, generation_low16) {
            return;
        }
        let (word, bit) = word_bit(slot);
        self.occupied[word] &= !(1u64 << bit);
        self.terminated[word] &= !(1u64 << bit);
        self.cancel_pending[word] &= !(1u64 << bit);
        self.len[slot as usize] = 0;
        self.generation[slot as usize] = self.generation[slot as usize].wrapping_add(1);
    }

    /// Whether `key` names a currently-occupied slot at its generation.
    pub(crate) const fn is_live(&self, key: MultishotSlotKey) -> bool {
        if key.worker_id != self.worker_id {
            return false;
        }
        if key.slot >= self.cap {
            return false;
        }
        let (word, bit) = word_bit(key.slot);
        self.occupied[word] & (1u64 << bit) != 0
            && self.generation[key.slot as usize] == key.generation
    }

    /// Whether `slot` is occupied at the low-16-bit `generation`.
    const fn is_slot_live(&self, slot: u16, generation_low16: u16) -> bool {
        if slot >= self.cap {
            return false;
        }
        let (word, bit) = word_bit(slot);
        self.occupied[word] & (1u64 << bit) != 0
            && self.generation[slot as usize] & 0xFFFF == generation_low16 as u64
    }

    /// First unoccupied slot below `cap`, or `None` when full.
    const fn first_free(&self) -> Option<u16> {
        let mut slot = 0u16;
        while slot < self.cap {
            let (word, bit) = word_bit(slot);
            if self.occupied[word] & (1u64 << bit) == 0 {
                return Some(slot);
            }
            slot += 1;
        }
        None
    }
}

/// Splits a slot index into its bitmap word and bit offset.
const fn word_bit(slot: u16) -> (usize, usize) {
    (slot as usize / 64, slot as usize % 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slab() -> MultishotSlab {
        MultishotSlab::new(3, DEFAULT_MULTISHOT_CAP)
    }

    fn allocate(registry: &mut MultishotSlab, owner: u64) -> MultishotSlotKey {
        let Some(key) = registry.allocate(owner) else {
            panic!("a free slot was expected");
        };
        key
    }

    #[test]
    fn allocate_then_free_bumps_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0xAA);
        assert!(registry.is_live(key));
        assert_eq!(key.generation, 0);
        registry.free(key);
        assert!(!registry.is_live(key));
        let next = allocate(&mut registry, 0xAA);
        assert_eq!(next.slot, key.slot);
        assert_eq!(next.generation, 1);
    }

    #[test]
    fn push_pop_is_fifo() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert_eq!(
            registry.push(key.slot, gen_low16, 10, true),
            MultishotPush::Queued
        );
        assert_eq!(
            registry.push(key.slot, gen_low16, 11, true),
            MultishotPush::Queued
        );
        assert_eq!(registry.pop(key), Some(10));
        assert_eq!(registry.pop(key), Some(11));
        assert_eq!(registry.pop(key), None);
    }

    #[test]
    fn push_marks_terminated() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert!(!registry.is_terminated(key));
        registry.push(key.slot, gen_low16, 7, false);
        assert!(registry.is_terminated(key));
    }

    #[test]
    fn push_overflows_when_full() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(
                registry.push(key.slot, gen_low16, value, true),
                MultishotPush::Queued
            );
        }
        assert_eq!(
            registry.push(key.slot, gen_low16, 999, true),
            MultishotPush::Overflowed
        );
    }

    #[test]
    fn push_rejects_stale_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        registry.free(key);
        let stale = (key.generation & 0xFFFF) as u16;
        assert_eq!(
            registry.push(key.slot, stale, 5, true),
            MultishotPush::Stale
        );
    }

    #[test]
    fn ring_wraps_around() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..40 {
            registry.push(key.slot, gen_low16, value, true);
            assert_eq!(registry.pop(key), Some(value));
        }
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(
                registry.push(key.slot, gen_low16, value, true),
                MultishotPush::Queued
            );
        }
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(registry.pop(key), Some(value));
        }
    }

    #[test]
    fn owner_resolves_live_slot() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0xDEAD);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert_eq!(registry.owner(key.slot, gen_low16), Some(0xDEAD));
        registry.free(key);
        assert_eq!(registry.owner(key.slot, gen_low16), None);
    }

    #[test]
    fn allocate_returns_none_when_full() {
        let mut registry = MultishotSlab::new(0, 2);
        assert!(registry.allocate(0x1).is_some());
        assert!(registry.allocate(0x2).is_some());
        assert!(registry.allocate(0x3).is_none());
    }

    #[test]
    fn stale_key_reads_are_inert() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        registry.free(key);
        assert!(!registry.is_live(key));
        assert!(!registry.is_terminated(key));
        assert_eq!(registry.pop(key), None);
        registry.free(key);
        registry.mark_cancel_pending(key);
    }

    #[test]
    fn is_live_rejects_other_worker() {
        let mut registry = MultishotSlab::new(3, DEFAULT_MULTISHOT_CAP);
        let key = allocate(&mut registry, 0x1);
        let mut foreign = key;
        foreign.worker_id = 4;
        assert!(!registry.is_live(foreign));
    }

    #[test]
    fn is_cancel_pending_tracks_the_flag() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert!(
            !registry.is_cancel_pending(key.slot, gen_low16),
            "a fresh slot is not cancel-pending",
        );
        registry.mark_cancel_pending(key);
        assert!(
            registry.is_cancel_pending(key.slot, gen_low16),
            "marking sets the cancel-pending flag",
        );
    }

    #[test]
    fn is_cancel_pending_rejects_stale_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.mark_cancel_pending(key);
        registry.free(key);
        assert!(
            !registry.is_cancel_pending(key.slot, gen_low16),
            "a freed slot reads not cancel-pending at the old generation",
        );
    }

    #[test]
    fn free_by_slot_frees_and_bumps_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.mark_cancel_pending(key);
        registry.free_by_slot(key.slot, gen_low16);
        assert!(!registry.is_live(key), "free_by_slot clears the occupancy");
        let next = allocate(&mut registry, 0x1);
        assert_eq!(next.slot, key.slot);
        assert_eq!(
            next.generation,
            key.generation + 1,
            "the generation bumped so the stale key is rejected",
        );
        assert!(
            !registry.is_cancel_pending(next.slot, (next.generation & 0xFFFF) as u16),
            "the reused slot starts without the cancel-pending flag",
        );
    }

    #[test]
    fn free_by_slot_ignores_stale_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.free_by_slot(key.slot, gen_low16.wrapping_add(1));
        assert!(
            registry.is_live(key),
            "a mismatched generation leaves the slot live",
        );
    }
}
