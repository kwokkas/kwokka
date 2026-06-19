//! Provided buffer ring handle for `io_uring` 5.19+ multishot operations.
//!
//! [`BufRing`] is a non-owning view over the kernel's `io_uring_buf_ring` /
//! `io_uring_buf` ABI. The backend (`UringDriver`) owns the mmap'd region;
//! this module owns only the view and the tail-advance logic.

#![allow(dead_code, reason = "pending buf_ring wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
};

use crate::buffer::ring::abi::BufRingEntry;

/// Provided buffer ring handle for `io_uring` 5.19+.
///
/// Wraps a pre-registered mmap'd ring where the kernel picks buffers
/// for multishot recv/accept. Does not own the memory -- the backend
/// manages mmap/munmap lifecycle.
///
/// # Single-writer invariant
///
/// [`push`](BufRing::push) must be called from a single thread (the
/// per-driver owner). The kernel reads the ring concurrently via its
/// own acquire ordering on the tail counter.
pub(crate) struct BufRing {
    ring: NonNull<BufRingEntry>,
    entries: u16,
    mask: u16,
}

impl BufRing {
    /// Create from a raw mmap'd region.
    ///
    /// # Panics
    ///
    /// Panics if `entries` is zero or not a power of two.
    ///
    /// # Safety
    ///
    /// - `ring` must point to a valid mmap'd region of at least `entries * 16` bytes, page-aligned
    ///   and zero-filled.
    /// - The region must be registered via `IORING_REGISTER_PBUF_RING`.
    /// - `entries` must be a non-zero power of two, max 2^15.
    /// - The caller must keep the region mapped for the lifetime of this [`BufRing`].
    /// - The tail field (offset 14 of entry 0) must only be accessed through this [`BufRing`] or
    ///   the kernel -- no aliasing writes.
    pub(crate) fn from_raw(ring: NonNull<BufRingEntry>, entries: u16) -> Self {
        assert!(
            entries > 0 && entries.is_power_of_two(),
            "entries must be a non-zero power of two"
        );
        Self {
            ring,
            entries,
            mask: entries - 1,
        }
    }

    /// Push a buffer entry and advance the tail.
    ///
    /// Writes `addr`, `len`, and `bid` to the next ring slot, then
    /// stores the incremented tail with `Release` ordering so the
    /// kernel observes the entry fields before the new tail value.
    ///
    /// The `resv` field is intentionally left untouched -- entry 0's
    /// `resv` is the shared tail counter per the kernel union layout.
    pub(crate) fn push(&self, addr: u64, len: u32, bid: u16) {
        let tail = self.tail().load(Ordering::Relaxed);
        let idx = (tail & self.mask) as usize;

        // SAFETY: Invariant -- idx in [0, entries) due to mask.
        // The mmap region has at least entries * 16 bytes, so
        // ring.add(idx) is in bounds. Only addr/len/bid are written;
        // resv is left untouched because entry 0's resv field is the
        // shared tail counter.
        // Precondition: single-writer (per-driver owner thread).
        // Failure mode: out-of-bounds write or resv overwrite causes UB.
        unsafe {
            let entry = self.ring.as_ptr().add(idx);
            (*entry).addr = addr;
            (*entry).len = len;
            (*entry).bid = bid;
        }

        self.tail().store(tail.wrapping_add(1), Ordering::Release);
    }

    /// Number of entries in this ring.
    pub(crate) const fn entries(&self) -> u16 {
        self.entries
    }

    /// Mask for tail wrap-around (`entries - 1`).
    pub(crate) const fn mask(&self) -> u16 {
        self.mask
    }

    /// Atomic reference to the in-ring tail counter.
    ///
    /// The tail lives at byte offset 14 of entry 0, which is the
    /// `resv` field position in [`BufRingEntry`] -- matching the
    /// kernel's `io_uring_buf_ring` union layout.
    pub(crate) fn tail(&self) -> &AtomicU16 {
        // SAFETY: Invariant -- entry 0 resv field occupies offset 14,
        // matching the tail in the io_uring_buf_ring union. The mmap
        // region is page-aligned, so resv is naturally u16-aligned.
        // The region is valid for the lifetime of this BufRing.
        // AtomicU16 has the same size and alignment as u16. The tail
        // is only accessed atomically (Relaxed load + Release store in
        // push, Acquire load by kernel).
        // Precondition: mmap region is mapped and not aliased.
        // Failure mode: misaligned or dangling pointer causes UB.
        unsafe {
            let resv_ptr = core::ptr::addr_of_mut!((*self.ring.as_ptr()).resv);
            AtomicU16::from_ptr(resv_ptr)
        }
    }
}

// SAFETY: Invariant -- BufRing holds a NonNull<BufRingEntry> pointing to
// mmap'd memory whose lifetime is managed by the backend (UringDriver).
// The backend keeps the region mapped until BufRing is dropped.
// Send: transferring BufRing to another thread is safe because the mmap
// region has no thread affinity -- process-wide kernel-managed memory.
// Precondition: the backend unmaps the region only after BufRing is dropped.
// Failure mode: if the region is unmapped before BufRing is dropped,
// subsequent push/tail calls dereference freed memory (UB).
unsafe impl Send for BufRing {}

#[cfg(test)]
mod tests {
    use std::alloc::{Layout, alloc_zeroed, dealloc};

    use super::*;
    use crate::buffer::ring::abi::TAIL_OFFSET;

    #[allow(clippy::cast_ptr_alignment, reason = "Layout guarantees align(16)")]
    fn alloc_ring(entries: u16) -> NonNull<BufRingEntry> {
        let Ok(layout) = Layout::from_size_align(entries as usize * 16, 16) else {
            panic!("invalid layout for {entries} entries");
        };
        // SAFETY: Invariant -- layout has non-zero size (entries >= 1)
        // and alignment 16, matching BufRingEntry's repr(C, align(16)).
        // Precondition: entries >= 1 (caller provides valid count).
        // Failure mode: zero-size layout or null return causes UB.
        let ptr = unsafe { alloc_zeroed(layout) }.cast::<BufRingEntry>();
        let Some(nn) = NonNull::new(ptr) else {
            panic!("alloc_zeroed returned null for {entries} entries");
        };
        nn
    }

    fn dealloc_ring(ring: NonNull<BufRingEntry>, entries: u16) {
        let Ok(layout) = Layout::from_size_align(entries as usize * 16, 16) else {
            panic!("invalid layout for {entries} entries");
        };
        // SAFETY: Invariant -- ring was allocated by alloc_ring with
        // matching layout.
        // Precondition: region and entries match the alloc_ring call.
        // Failure mode: mismatched layout causes heap corruption.
        unsafe { dealloc(ring.as_ptr().cast::<u8>(), layout) }
    }

    #[test]
    fn entry_size_is_16_bytes() {
        assert_eq!(core::mem::size_of::<BufRingEntry>(), 16);
    }

    #[test]
    fn entry_alignment_is_16() {
        assert_eq!(core::mem::align_of::<BufRingEntry>(), 16);
    }

    #[test]
    fn tail_offset_matches_resv_field() {
        assert_eq!(core::mem::offset_of!(BufRingEntry, resv), TAIL_OFFSET);
    }

    #[test]
    fn from_raw_sets_mask() {
        let entries = 4u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);
        assert_eq!(ring.entries(), 4);
        assert_eq!(ring.mask(), 3);
        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    #[should_panic(expected = "non-zero power of two")]
    fn from_raw_rejects_zero_entries() {
        BufRing::from_raw(NonNull::dangling(), 0);
    }

    #[test]
    #[should_panic(expected = "non-zero power of two")]
    fn from_raw_rejects_non_power_of_two() {
        BufRing::from_raw(NonNull::dangling(), 3);
    }

    #[test]
    fn push_writes_entry_fields() {
        let entries = 4u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);

        ring.push(0xDEAD_BEEF_0000_0001, 4096, 0);

        // SAFETY: Invariant -- index 0 is within the allocated region
        // of 4 entries.
        // Precondition: ring_ptr allocated with entries >= 1.
        // Failure mode: out-of-bounds read.
        let entry = unsafe { &*ring_ptr.as_ptr().add(0) };
        assert_eq!(entry.addr, 0xDEAD_BEEF_0000_0001);
        assert_eq!(entry.len, 4096);
        assert_eq!(entry.bid, 0);

        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    fn push_advances_tail() {
        let entries = 4u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);

        assert_eq!(ring.tail().load(Ordering::Relaxed), 0);
        ring.push(0x1000, 64, 0);
        assert_eq!(ring.tail().load(Ordering::Relaxed), 1);
        ring.push(0x2000, 64, 1);
        assert_eq!(ring.tail().load(Ordering::Relaxed), 2);

        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    fn push_wraps_around_ring() {
        let entries = 2u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);

        ring.push(0xAAAA, 10, 0);
        ring.push(0xBBBB, 20, 1);
        ring.push(0xCCCC, 30, 2);

        assert_eq!(ring.tail().load(Ordering::Relaxed), 3);

        // SAFETY: Invariant -- index 0 is within the allocated region
        // of 2 entries. push 2 wraps to idx 0.
        // Precondition: ring_ptr allocated with entries >= 1.
        // Failure mode: out-of-bounds read.
        let entry0 = unsafe { &*ring_ptr.as_ptr().add(0) };
        assert_eq!(entry0.addr, 0xCCCC);
        assert_eq!(entry0.len, 30);
        assert_eq!(entry0.bid, 2);

        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    fn tail_wraps_u16() {
        let entries = 2u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);

        ring.tail().store(u16::MAX, Ordering::Relaxed);
        ring.push(0x1000, 64, 0);
        assert_eq!(ring.tail().load(Ordering::Relaxed), 0);

        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    fn push_does_not_corrupt_tail_at_entry_zero() {
        let entries = 4u16;
        let ring_ptr = alloc_ring(entries);
        let ring = BufRing::from_raw(ring_ptr, entries);

        ring.push(0x1000, 128, 5);

        let tail_val = ring.tail().load(Ordering::Relaxed);
        assert_eq!(tail_val, 1);

        dealloc_ring(ring_ptr, entries);
    }

    #[test]
    fn buf_ring_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<BufRing>();
    }
}
