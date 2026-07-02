//! Per-worker registry for in-flight multishot recv operations.
//!
//! A multishot recv submits one SQE that posts many CQEs sharing one
//! `user_data`, each naming a kernel-selected provided buffer. Like the accept
//! registry, this owns a small FIFO per op that the completion drain pushes into
//! and the owning task drains on its next poll. Unlike accept, each queued
//! completion carries two values (the byte count and the provided-buffer id),
//! and the store is per-connection, so it scales past the inline accept path
//! with an mmap-backed FIFO payload.
//!
//! Zero-heap: one `MmapRegion` for the FIFO payload, allocated at construction
//! and never grown; the bitmaps and per-slot tables are inline. Pure safe Rust:
//! the payload is read and written as little-endian bytes through the region's
//! byte-slice views, the single raw-pointer step confined to `MmapRegion`.
//!
//! Generational: each slot carries a generation bumped on free, so a stale
//! sentinel naming a reused slot is rejected rather than routed to the new
//! occupant.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) restricts slab internals inside the now-pub multishot module"
)]
#![allow(dead_code, reason = "pending multishot recv wire-up")]

use std::io;

use crate::buffer::{mmap::MmapRegion, multishot::MULTISHOT_FIFO_DEPTH};

/// Default multishot recv slots per worker.
///
/// Recv is per-connection, so this is sized well above the accept path's handful
/// of listeners. It matches the provided-buffer ring depth that feeds every
/// stream on the worker: there is no reason to track more concurrent streams
/// than the shared buffer pool can serve. The FIFO payload is mmap-backed, so
/// this does not inflate the shard's stack frame the way the inline accept
/// registry would.
pub const DEFAULT_RECV_MULTISHOT_CAP: u16 = 256;

/// Bytes per queued completion in the FIFO payload: an `i32` byte count (or a
/// negative errno) followed by a `u16` provided-buffer id.
const ENTRY_LEN: usize = 6;

/// Slot-count ceiling that sizes the inline bitmaps and per-slot tables.
const MAX_RECV_MULTISHOT_SLOTS: usize = DEFAULT_RECV_MULTISHOT_CAP as usize;

/// Bitmap words covering [`MAX_RECV_MULTISHOT_SLOTS`].
const BITMAP_WORDS: usize = MAX_RECV_MULTISHOT_SLOTS / 64;

/// The `buf_id` value standing for "this completion consumed no buffer".
///
/// A real provided-buffer id never reaches `u16::MAX`: the buffer ring caps its
/// entry count at `1 << 15`. End of stream (a zero-length completion) and a
/// negative-result completion both queue this sentinel.
pub(crate) const NO_BUFFER: u16 = u16::MAX;

/// A Copy handle into the multishot recv registry.
///
/// Held by a recv stream. The `generation` guards slot reuse: once the slot is
/// freed its generation is bumped, so a stale stream or a stale sentinel that
/// still names the old generation is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvMultishotSlotKey {
    /// Slot index in the owning worker's registry.
    pub(crate) slot: u16,
    /// Generation captured at allocation.
    pub(crate) generation: u64,
    /// Worker whose registry owns the slot.
    pub(crate) worker_id: u8,
}

/// Outcome of pushing a completion into a slot's FIFO.
///
/// Unlike the accept registry, a dropped or overflowed recv completion owns a
/// provided-buffer id the caller must recycle to the buffer ring, never close:
/// recv completions carry no fd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecvMultishotPush {
    /// The result was queued for the consumer.
    Queued,
    /// The FIFO was full; the caller owns recycling the completion's buffer id.
    Overflowed,
    /// The sentinel named a freed or reused slot; not queued. Carries the same
    /// disposal contract as `Overflowed`: recycle the buffer id, or it leaks.
    Stale,
}

/// Per-worker fixed-capacity multishot recv registry.
pub struct RecvMultishotSlab {
    /// mmap-backed per-slot ring of `(i32 count, u16 buf_id)` entries.
    storage: MmapRegion,
    /// Per-slot ring read cursor.
    head: [u16; MAX_RECV_MULTISHOT_SLOTS],
    /// Per-slot count of queued results.
    len: [u16; MAX_RECV_MULTISHOT_SLOTS],
    occupied: [u64; BITMAP_WORDS],
    /// The op posted its final (no-`MORE`) CQE; no more results will arrive.
    terminated: [u64; BITMAP_WORDS],
    /// A cancel was submitted for the op; its final CQE frees the slot.
    cancel_pending: [u64; BITMAP_WORDS],
    /// Per-slot generation, bumped on free. A `u64` makes the ABA window
    /// effectively unbounded.
    generation: [u64; MAX_RECV_MULTISHOT_SLOTS],
    /// Per-slot owning task token, woken when a result lands in the FIFO.
    owner_token: [u64; MAX_RECV_MULTISHOT_SLOTS],
    worker_id: u8,
    cap: u16,
}

impl RecvMultishotSlab {
    /// Builds a registry of `cap` slots (clamped to
    /// [`DEFAULT_RECV_MULTISHOT_CAP`]) for `worker_id`.
    ///
    /// # Errors
    ///
    /// Returns the `mmap` error when backing allocation fails.
    pub fn new(worker_id: u8, cap: u16) -> io::Result<Self> {
        let cap = cap.min(DEFAULT_RECV_MULTISHOT_CAP);
        let depth = MULTISHOT_FIFO_DEPTH as usize;
        let storage = MmapRegion::new(MAX_RECV_MULTISHOT_SLOTS * depth * ENTRY_LEN)?;
        Ok(Self {
            storage,
            head: [0; MAX_RECV_MULTISHOT_SLOTS],
            len: [0; MAX_RECV_MULTISHOT_SLOTS],
            occupied: [0; BITMAP_WORDS],
            terminated: [0; BITMAP_WORDS],
            cancel_pending: [0; BITMAP_WORDS],
            generation: [0; MAX_RECV_MULTISHOT_SLOTS],
            owner_token: [0; MAX_RECV_MULTISHOT_SLOTS],
            worker_id,
            cap,
        })
    }

    /// Allocates the first free slot for a multishot recv op, returning its
    /// handle. `owner_token` is the task woken on each completion. Returns
    /// `None` when every slot is occupied.
    pub(crate) fn allocate(&mut self, owner_token: u64) -> Option<RecvMultishotSlotKey> {
        let slot = self.first_free()?;
        let (word, bit) = word_bit(slot);
        self.occupied[word] |= 1u64 << bit;
        self.terminated[word] &= !(1u64 << bit);
        self.cancel_pending[word] &= !(1u64 << bit);
        self.head[slot as usize] = 0;
        self.len[slot as usize] = 0;
        self.owner_token[slot as usize] = owner_token;
        Some(RecvMultishotSlotKey {
            slot,
            generation: self.generation[slot as usize],
            worker_id: self.worker_id,
        })
    }

    /// Queues a completion `(result, buf_id)` for `slot`, marking the slot
    /// terminated when `is_more` is clear. `generation_low16` rejects a stale
    /// sentinel; a freed or reused slot yields [`RecvMultishotPush::Stale`].
    pub(crate) fn push(
        &mut self,
        slot: u16,
        generation_low16: u16,
        result: i32,
        buf_id: u16,
        is_more: bool,
    ) -> RecvMultishotPush {
        if !self.is_slot_live(slot, generation_low16) {
            return RecvMultishotPush::Stale;
        }
        if !is_more {
            let (word, bit) = word_bit(slot);
            self.terminated[word] |= 1u64 << bit;
        }
        let slot_idx = slot as usize;
        if self.len[slot_idx] >= MULTISHOT_FIFO_DEPTH {
            return RecvMultishotPush::Overflowed;
        }
        let ring_index = (self.head[slot_idx] + self.len[slot_idx]) % MULTISHOT_FIFO_DEPTH;
        let offset = entry_offset(slot, ring_index);
        let bytes = self.storage.as_mut_slice();
        bytes[offset..offset + 4].copy_from_slice(&result.to_le_bytes());
        bytes[offset + 4..offset + ENTRY_LEN].copy_from_slice(&buf_id.to_le_bytes());
        self.len[slot_idx] += 1;
        RecvMultishotPush::Queued
    }

    /// Pops the oldest queued `(result, buf_id)` for `key`, or `None` when the
    /// FIFO is empty or `key` is stale. A `buf_id` of [`NO_BUFFER`] marks a
    /// completion that consumed no provided buffer.
    pub(crate) fn pop(&mut self, key: RecvMultishotSlotKey) -> Option<(i32, u16)> {
        if !self.is_live(key) || self.len[key.slot as usize] == 0 {
            return None;
        }
        let slot_idx = key.slot as usize;
        let head = self.head[slot_idx];
        let offset = entry_offset(key.slot, head);
        let bytes = self.storage.as_slice();
        let Ok(result_bytes) = <[u8; 4]>::try_from(&bytes[offset..offset + 4]) else {
            return None;
        };
        let Ok(buf_id_bytes) = <[u8; 2]>::try_from(&bytes[offset + 4..offset + ENTRY_LEN]) else {
            return None;
        };
        self.head[slot_idx] = (head + 1) % MULTISHOT_FIFO_DEPTH;
        self.len[slot_idx] -= 1;
        Some((
            i32::from_le_bytes(result_bytes),
            u16::from_le_bytes(buf_id_bytes),
        ))
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
    pub(crate) const fn is_terminated(&self, key: RecvMultishotSlotKey) -> bool {
        if !self.is_live(key) {
            return false;
        }
        let (word, bit) = word_bit(key.slot);
        self.terminated[word] & (1u64 << bit) != 0
    }

    /// Marks `key`'s slot cancel-pending. A stale handle is a no-op.
    pub(crate) const fn mark_cancel_pending(&mut self, key: RecvMultishotSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.cancel_pending[word] |= 1u64 << bit;
    }

    /// Frees `key`'s slot, bumping its generation. A stale handle is a no-op.
    pub(crate) const fn free(&mut self, key: RecvMultishotSlotKey) {
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
    pub(crate) const fn is_live(&self, key: RecvMultishotSlotKey) -> bool {
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

    /// First free slot within `cap`, by inverted-bitmap scan.
    fn first_free(&self) -> Option<u16> {
        let limit = self.cap as usize;
        let last_word = limit / 64;
        let last_bit = limit % 64;
        for (word_idx, word) in self.occupied.iter().enumerate() {
            let mask = if word_idx < last_word {
                u64::MAX
            } else if word_idx == last_word && last_bit > 0 {
                (1u64 << last_bit) - 1
            } else {
                break;
            };
            let available = !*word & mask;
            if available != 0 {
                let raw = word_idx * 64 + available.trailing_zeros() as usize;
                return u16::try_from(raw).ok();
            }
        }
        None
    }
}

/// Byte offset of a slot's ring entry in the FIFO payload.
const fn entry_offset(slot: u16, ring_index: u16) -> usize {
    (slot as usize * MULTISHOT_FIFO_DEPTH as usize + ring_index as usize) * ENTRY_LEN
}

/// Splits a slot index into its bitmap word and bit offset.
const fn word_bit(slot: u16) -> (usize, usize) {
    (slot as usize / 64, slot as usize % 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slab() -> RecvMultishotSlab {
        let Ok(slab) = RecvMultishotSlab::new(3, DEFAULT_RECV_MULTISHOT_CAP) else {
            panic!("mmap must succeed for the test registry");
        };
        slab
    }

    fn allocate(registry: &mut RecvMultishotSlab, owner: u64) -> RecvMultishotSlotKey {
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
    fn push_pop_carries_payload() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert_eq!(
            registry.push(key.slot, gen_low16, 128, 7, true),
            RecvMultishotPush::Queued
        );
        assert_eq!(
            registry.push(key.slot, gen_low16, 64, 9, true),
            RecvMultishotPush::Queued
        );
        assert_eq!(registry.pop(key), Some((128, 7)));
        assert_eq!(registry.pop(key), Some((64, 9)));
        assert_eq!(registry.pop(key), None);
    }

    #[test]
    fn pop_round_trips_payload() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.push(key.slot, gen_low16, 0, NO_BUFFER, true);
        registry.push(key.slot, gen_low16, -105, NO_BUFFER, false);
        assert_eq!(registry.pop(key), Some((0, NO_BUFFER)));
        assert_eq!(
            registry.pop(key),
            Some((-105, NO_BUFFER)),
            "a negative errno round-trips through the little-endian payload",
        );
    }

    #[test]
    fn push_marks_terminated() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert!(!registry.is_terminated(key));
        registry.push(key.slot, gen_low16, 7, 1, false);
        assert!(registry.is_terminated(key));
    }

    #[test]
    fn push_overflows_when_full() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(
                registry.push(key.slot, gen_low16, value, 0, true),
                RecvMultishotPush::Queued
            );
        }
        assert_eq!(
            registry.push(key.slot, gen_low16, 999, 0, true),
            RecvMultishotPush::Overflowed
        );
    }

    #[test]
    fn push_rejects_stale_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        registry.free(key);
        let stale = (key.generation & 0xFFFF) as u16;
        assert_eq!(
            registry.push(key.slot, stale, 5, 0, true),
            RecvMultishotPush::Stale
        );
    }

    #[test]
    fn ring_wraps_around() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        for value in 0..40u16 {
            registry.push(key.slot, gen_low16, i32::from(value), value, true);
            assert_eq!(registry.pop(key), Some((i32::from(value), value)));
        }
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(
                registry.push(key.slot, gen_low16, value, 0, true),
                RecvMultishotPush::Queued
            );
        }
        for value in 0..i32::from(MULTISHOT_FIFO_DEPTH) {
            assert_eq!(registry.pop(key), Some((value, 0)));
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
        let Ok(mut registry) = RecvMultishotSlab::new(0, 2) else {
            panic!("mmap must succeed");
        };
        assert!(registry.allocate(0x1).is_some());
        assert!(registry.allocate(0x2).is_some());
        assert!(registry.allocate(0x3).is_none());
    }

    #[test]
    fn slots_are_stride_independent() {
        let mut registry = slab();
        let first = allocate(&mut registry, 0x1);
        let second = allocate(&mut registry, 0x2);
        let first_gen = (first.generation & 0xFFFF) as u16;
        let second_gen = (second.generation & 0xFFFF) as u16;
        registry.push(first.slot, first_gen, 11, 1, true);
        registry.push(second.slot, second_gen, 22, 2, true);
        assert_eq!(registry.pop(first), Some((11, 1)));
        assert_eq!(
            registry.pop(second),
            Some((22, 2)),
            "each slot's FIFO payload is independent in the shared region",
        );
    }

    #[test]
    fn is_live_rejects_other_worker() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let mut foreign = key;
        foreign.worker_id = 4;
        assert!(!registry.is_live(foreign));
    }

    #[test]
    fn cancel_pending_tracks_flag() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        assert!(!registry.is_cancel_pending(key.slot, gen_low16));
        registry.mark_cancel_pending(key);
        assert!(registry.is_cancel_pending(key.slot, gen_low16));
    }

    #[test]
    fn free_by_slot_bumps_generation() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.mark_cancel_pending(key);
        registry.free_by_slot(key.slot, gen_low16);
        assert!(!registry.is_live(key));
        let next = allocate(&mut registry, 0x1);
        assert_eq!(next.slot, key.slot);
        assert_eq!(next.generation, key.generation + 1);
    }

    #[test]
    fn free_by_slot_ignores_stale() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        let gen_low16 = (key.generation & 0xFFFF) as u16;
        registry.free_by_slot(key.slot, gen_low16.wrapping_add(1));
        assert!(registry.is_live(key), "a mismatched generation is a no-op");
    }

    #[test]
    fn stale_key_reads_are_inert() {
        let mut registry = slab();
        let key = allocate(&mut registry, 0x1);
        registry.free(key);
        assert!(!registry.is_live(key));
        assert!(!registry.is_terminated(key));
        assert_eq!(registry.pop(key), None);
    }
}
