//! Per-worker in-flight buffer registry for drop-safe completion futures.
//!
//! Completion-based I/O hands the kernel a pointer into a buffer that must
//! outlive the submitting future. If the future is dropped before its
//! completion arrives, inline storage would free under an in-flight kernel
//! write -- undefined behavior. This registry owns the bytes instead: a
//! future holds a Copy [`InflightSlotKey`], and the bytes live in
//! fixed-capacity mmap storage freed only after the completion is harvested,
//! never on the drop.
//!
//! Zero-heap: the backing is one `MmapRegion` allocated at construction and
//! never grown.
//!
//! Generational: each slot carries a generation bumped on free, so a stale
//! handle for a reused slot is rejected. A retire-pending bit alone cannot
//! tell one occupant from the next.

#![allow(dead_code, reason = "pending drop-safe future wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io;

use crate::buffer::mmap::MmapRegion;

/// Per-slot byte capacity; a buffered future's `CAP` must not exceed this.
pub(crate) const INFLIGHT_BUF_STRIDE: u32 = 4096;

/// Default in-flight slots per worker.
pub(crate) const DEFAULT_INFLIGHT_CAP: u16 = 256;

/// Slot-count ceiling that sizes the inline bitmaps and generation table.
const MAX_INFLIGHT_SLOTS: usize = DEFAULT_INFLIGHT_CAP as usize;

/// Bitmap words covering [`MAX_INFLIGHT_SLOTS`].
const BITMAP_WORDS: usize = MAX_INFLIGHT_SLOTS / 64;

/// A Copy handle into the in-flight buffer registry.
///
/// Held by a completion future in place of inline byte storage. The
/// `generation` guards slot reuse: once the slot is freed its generation is
/// bumped, so a stale future or a stale cancel that still names the old
/// generation is rejected rather than touching a reallocated slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InflightSlotKey {
    /// Slot index in the owning worker's registry.
    pub(crate) slot: u16,
    /// Generation captured at allocation.
    pub(crate) generation: u64,
    /// Worker whose registry owns the slot.
    pub(crate) worker_id: u8,
    /// Submitted op `user_data`; the cancel target when the future drops.
    pub(crate) op_token: u64,
}

/// Per-worker fixed-capacity in-flight buffer registry.
///
/// Owns the bytes for every buffered op in flight on its worker. A slot is
/// freed only by the completion drain after the kernel signals it is done,
/// never by the future's drop, so the kernel never writes freed memory.
pub(crate) struct InflightBufSlab {
    storage: MmapRegion,
    occupied: [u64; BITMAP_WORDS],
    retire_pending: [u64; BITMAP_WORDS],
    /// Per-slot generation, bumped on free. A `u64` makes the ABA window
    /// effectively unbounded: a slot would need 2^64 reuses to wrap.
    generation: [u64; MAX_INFLIGHT_SLOTS],
    worker_id: u8,
    cap: u16,
    stride: u32,
}

impl InflightBufSlab {
    /// Builds a registry of `cap` slots (clamped to [`DEFAULT_INFLIGHT_CAP`])
    /// for `worker_id`, each [`INFLIGHT_BUF_STRIDE`] bytes wide.
    ///
    /// # Errors
    ///
    /// Returns the `mmap` error when backing allocation fails.
    pub(crate) fn new(worker_id: u8, cap: u16) -> io::Result<Self> {
        let cap = cap.min(DEFAULT_INFLIGHT_CAP);
        let stride = INFLIGHT_BUF_STRIDE;
        let storage = MmapRegion::new(cap as usize * stride as usize)?;
        Ok(Self {
            storage,
            occupied: [0; BITMAP_WORDS],
            retire_pending: [0; BITMAP_WORDS],
            generation: [0; MAX_INFLIGHT_SLOTS],
            worker_id,
            cap,
            stride,
        })
    }

    /// Allocates the first free slot for an op, returning its handle.
    ///
    /// Returns `None` when every slot is occupied; the caller surfaces that
    /// as an `io::Error` rather than blocking.
    pub(crate) fn allocate(&mut self, op_token: u64) -> Option<InflightSlotKey> {
        let slot = self.first_free()?;
        let (word, bit) = word_bit(slot);
        self.occupied[word] |= 1u64 << bit;
        Some(InflightSlotKey {
            slot,
            generation: self.generation[slot as usize],
            worker_id: self.worker_id,
            op_token,
        })
    }

    /// Frees `key`'s slot, bumping its generation so later stale handles are
    /// rejected. A stale handle (the slot was already freed and reused) is a
    /// no-op.
    pub(crate) const fn free(&mut self, key: InflightSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.occupied[word] &= !(1u64 << bit);
        self.retire_pending[word] &= !(1u64 << bit);
        let generation = &mut self.generation[key.slot as usize];
        *generation = generation.wrapping_add(1);
    }

    /// Marks `key`'s slot retire-pending. A stale handle is a no-op.
    pub(crate) const fn mark_retire_pending(&mut self, key: InflightSlotKey) {
        if !self.is_live(key) {
            return;
        }
        let (word, bit) = word_bit(key.slot);
        self.retire_pending[word] |= 1u64 << bit;
    }

    /// Returns whether `slot` is currently marked retire-pending.
    pub(crate) const fn is_retire_pending(&self, slot: u16) -> bool {
        if slot >= self.cap {
            return false;
        }
        let (word, bit) = word_bit(slot);
        self.retire_pending[word] & (1u64 << bit) != 0
    }

    /// Returns a writable pointer to `key`'s slot, or `None` if stale.
    ///
    /// The pointer is valid for [`INFLIGHT_BUF_STRIDE`] bytes while `self`
    /// lives and the slot stays occupied at `key`'s generation. This method
    /// performs no dereference: the future hands the pointer to the kernel
    /// through `InlineBuf`, and that unsafe contract lives at the call site.
    ///
    /// The caller must not also read the slot via
    /// [`slot_slice`](Self::slot_slice) while the pointer is submitted to the
    /// kernel: a shared read aliasing an in-flight kernel write is a data race.
    pub(crate) const fn slot_ptr(&self, key: InflightSlotKey) -> Option<*mut u8> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        Some(self.storage.as_ptr().cast_mut().wrapping_add(offset))
    }

    /// Returns `key`'s slot bytes truncated to `len`, or `None` if stale.
    ///
    /// Must be called only after the CQE for this slot's op is received;
    /// reading the slot while the kernel write is still in flight is a data
    /// race. `len` must not exceed the CQE-confirmed byte count -- bytes
    /// beyond it are zero (the initial mmap fill) or hold a prior op's data.
    /// The slice is clamped to the stride for safety.
    pub(crate) fn slot_slice(&self, key: InflightSlotKey, len: usize) -> Option<&[u8]> {
        if !self.is_live(key) {
            return None;
        }
        let offset = key.slot as usize * self.stride as usize;
        let end = offset + len.min(self.stride as usize);
        Some(&self.storage.as_slice()[offset..end])
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

    /// Whether `key` names a live slot this registry owns at the matching
    /// generation.
    ///
    /// Checks worker ownership and slot occupancy alongside the generation, so
    /// a cross-worker handle or a fabricated key for an unallocated slot is
    /// rejected, not just a stale one. This generation guard is what lets
    /// [`is_retire_pending`](Self::is_retire_pending) skip a generation
    /// parameter: [`free`](Self::free) clears the retire-pending bit and bumps
    /// the generation together, so any reuse of the slot begins with the bit
    /// clear and a stale mark cannot survive across a free.
    const fn is_live(&self, key: InflightSlotKey) -> bool {
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
}

/// Bitmap word index and bit offset for `slot`.
const fn word_bit(slot: u16) -> (usize, usize) {
    (slot as usize / 64, slot as usize % 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slab(cap: u16) -> InflightBufSlab {
        let Ok(slab) = InflightBufSlab::new(3, cap) else {
            panic!("mmap must succeed for the test registry");
        };
        slab
    }

    #[test]
    fn allocate_is_sequential() {
        let mut registry = slab(8);
        assert_eq!(registry.allocate(0).map(|key| key.slot), Some(0));
        assert_eq!(registry.allocate(0).map(|key| key.slot), Some(1));
    }

    #[test]
    fn allocate_carries_worker_token_and_generation() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0xABCD) else {
            panic!("allocate must succeed");
        };
        assert_eq!(key.worker_id, 3);
        assert_eq!(key.op_token, 0xABCD);
        assert_eq!(key.generation, 0);
    }

    #[test]
    fn exhaustion_returns_none() {
        let mut registry = slab(2);
        assert!(registry.allocate(0).is_some());
        assert!(registry.allocate(0).is_some());
        assert!(registry.allocate(0).is_none());
    }

    #[test]
    fn free_reuses_slot_and_bumps_generation() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        registry.free(first);
        let Some(second) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert_eq!(second.slot, 0, "the freed slot is reused");
        assert_eq!(second.generation, 1, "the generation bumped on free");
    }

    #[test]
    fn stale_handle_is_rejected_everywhere() {
        let mut registry = slab(8);
        let Some(stale) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        registry.free(stale);
        let Some(fresh) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert_eq!(fresh.slot, stale.slot, "the slot is reused");
        assert!(registry.slot_ptr(stale).is_none(), "stale ptr is rejected");
        assert!(
            registry.slot_slice(stale, 4).is_none(),
            "stale slice is rejected"
        );
        registry.free(stale);
        registry.mark_retire_pending(stale);
        assert!(
            !registry.is_retire_pending(fresh.slot),
            "the stale mark did not apply to the fresh occupant",
        );
        assert!(
            registry.slot_ptr(fresh).is_some(),
            "the fresh slot survives a stale free",
        );
    }

    #[test]
    fn retire_pending_marks_and_clears_on_free() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        assert!(!registry.is_retire_pending(key.slot));
        registry.mark_retire_pending(key);
        assert!(registry.is_retire_pending(key.slot));
        registry.free(key);
        assert!(!registry.is_retire_pending(key.slot), "free clears the bit");
    }

    #[test]
    fn slot_ptr_is_distinct_and_stride_apart() {
        let mut registry = slab(8);
        let Some(first) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(second) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let (Some(first_ptr), Some(second_ptr)) =
            (registry.slot_ptr(first), registry.slot_ptr(second))
        else {
            panic!("a live handle yields a pointer");
        };
        let gap = second_ptr as usize - first_ptr as usize;
        assert_eq!(gap, INFLIGHT_BUF_STRIDE as usize);
    }

    #[test]
    fn slot_slice_clamps_to_len() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(bytes) = registry.slot_slice(key, 4) else {
            panic!("a live handle yields a slice");
        };
        assert_eq!(bytes.len(), 4);
        assert!(bytes.iter().all(|&byte| byte == 0), "mmap is zero-filled");
    }

    #[test]
    fn slot_slice_len_is_capped_at_stride() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let Some(bytes) = registry.slot_slice(key, usize::MAX) else {
            panic!("a live handle yields a slice");
        };
        assert_eq!(bytes.len(), INFLIGHT_BUF_STRIDE as usize);
    }

    #[test]
    fn unallocated_slot_is_not_live() {
        let registry = slab(8);
        let fabricated = InflightSlotKey {
            slot: 0,
            generation: 0,
            worker_id: 3,
            op_token: 0,
        };
        assert!(
            registry.slot_ptr(fabricated).is_none(),
            "a fabricated key for an unallocated slot is rejected",
        );
    }

    #[test]
    fn foreign_worker_key_is_rejected() {
        let mut registry = slab(8);
        let Some(key) = registry.allocate(0) else {
            panic!("allocate must succeed");
        };
        let foreign = InflightSlotKey {
            worker_id: key.worker_id + 1,
            ..key
        };
        assert!(
            registry.slot_ptr(foreign).is_none(),
            "a key from another worker is rejected",
        );
    }
}
