//! Per-listener buffer ring pool for `io_uring` multishot operations.
//!
//! [`BufRingPool`] pairs mmap-backed contiguous storage with a
//! [`BufRing`](crate::buffer::ring::memory::BufRing) so the kernel can pick buffers
//! automatically during multishot recv/accept.

#![allow(dead_code, reason = "pending buf_ring wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::ptr::NonNull;
use std::io;

use crate::buffer::{
    mmap::MmapRegion,
    ring::{BufRing, BufRingEntry},
    slot::BufGroupId,
};

/// Per-listener buffer ring pool.
///
/// Allocates `entries * buf_size` bytes of mmap-backed contiguous
/// storage and registers every buffer with the [`BufRing`]. On
/// multishot recv/accept the kernel picks a buffer by `bid`; after
/// processing the CQE the driver calls [`recycle`](BufRingPool::recycle)
/// to return the buffer to the ring.
///
/// # Invariant
///
/// `bid` is in `[0, entries)` and maps to storage offset
/// `bid * buf_size`. This mapping is maintained across push and
/// recycle operations.
///
/// # Single-writer invariant
///
/// Inherited from [`BufRing`] -- [`recycle`](BufRingPool::recycle)
/// must be called from the per-driver owner thread only.
pub(crate) struct BufRingPool {
    storage: MmapRegion,
    ring: BufRing,
    buf_size: u32,
    entries: u16,
    group_id: BufGroupId,
}

impl BufRingPool {
    /// Allocate pool storage, create ring view, and push all entries.
    ///
    /// `ring_region` must be a valid mmap'd region satisfying the
    /// preconditions of [`BufRing::from_raw`].
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if `entries` is zero, not a power of two,
    /// or exceeds 32768; if `buf_size` is zero; or if the mmap
    /// allocation fails.
    pub(crate) fn new(
        entries: u16,
        buf_size: u32,
        group_id: BufGroupId,
        ring_region: NonNull<BufRingEntry>,
    ) -> io::Result<Self> {
        let entries_usize = entries as usize;
        let buf_size_usize = buf_size as usize;

        if buf_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buf_size must be non-zero",
            ));
        }
        if entries == 0 || !entries.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entries must be a non-zero power of two",
            ));
        }
        if entries > 1 << 15 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entries exceeds kernel maximum 32768",
            ));
        }

        let ring = BufRing::from_raw(ring_region, entries);
        let storage = MmapRegion::new(entries_usize * buf_size_usize)?;

        let base = storage.as_ptr() as u64;
        for bid in 0..entries {
            let addr = base + u64::from(bid) * u64::from(buf_size);
            ring.push(addr, buf_size, bid);
        }

        Ok(Self {
            storage,
            ring,
            buf_size,
            entries,
            group_id,
        })
    }

    /// Get buffer slice for a completed `bid` with `len` bytes written.
    ///
    /// `len` is the byte count from the CQE result -- the kernel may
    /// write fewer bytes than `buf_size`.
    ///
    /// # Panics
    ///
    /// Panics if `bid >= entries` or `len > buf_size`.
    pub(crate) fn get(&self, bid: u16, len: u32) -> &[u8] {
        let Self {
            entries, buf_size, ..
        } = *self;
        let Ok(()) = check_bid(bid, entries) else {
            unreachable!("bid {bid} out of range (entries: {entries})")
        };
        let Ok(()) = check_len(len, buf_size) else {
            unreachable!("len {len} exceeds buf_size {buf_size}")
        };
        let offset = bid as usize * buf_size as usize;
        &self.storage.as_slice()[offset..offset + len as usize]
    }

    /// Recycle a buffer back to the ring after processing.
    ///
    /// Must be called from the per-driver owner thread (single-writer
    /// invariant inherited from [`BufRing`]).
    ///
    /// # Panics
    ///
    /// Panics if `bid >= entries`.
    pub(crate) fn recycle(&self, bid: u16) {
        let Ok(()) = check_bid(bid, self.entries) else {
            unreachable!("bid {bid} out of range (entries: {})", self.entries)
        };
        let addr = self.storage.as_ptr() as u64 + u64::from(bid) * u64::from(self.buf_size);
        self.ring.push(addr, self.buf_size, bid);
    }

    /// Buffer group ID for this pool.
    pub(crate) const fn group_id(&self) -> BufGroupId {
        self.group_id
    }

    /// Number of buffer entries.
    pub(crate) const fn entries(&self) -> u16 {
        self.entries
    }

    /// Per-buffer size in bytes.
    pub(crate) const fn buf_size(&self) -> u32 {
        self.buf_size
    }
}

const fn check_bid(bid: u16, entries: u16) -> Result<(), ()> {
    if bid < entries { Ok(()) } else { Err(()) }
}

const fn check_len(len: u32, buf_size: u32) -> Result<(), ()> {
    if len <= buf_size { Ok(()) } else { Err(()) }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::Ordering;
    use std::alloc::{Layout, alloc_zeroed, dealloc};

    use super::*;

    #[allow(clippy::cast_ptr_alignment, reason = "Layout guarantees align(16)")]
    fn alloc_ring_region(entries: u16) -> NonNull<BufRingEntry> {
        let Ok(layout) = Layout::from_size_align(entries as usize * 16, 16) else {
            panic!("invalid layout for {entries} entries");
        };
        // SAFETY: Invariant -- layout has non-zero size (entries >= 1)
        // and alignment 16, matching BufRingEntry repr(C, align(16)).
        // Precondition: entries >= 1.
        // Failure mode: zero-size layout or null return causes UB.
        let ptr = unsafe { alloc_zeroed(layout) }.cast::<BufRingEntry>();
        let Some(nn) = NonNull::new(ptr) else {
            panic!("alloc_zeroed returned null");
        };
        nn
    }

    fn dealloc_ring_region(region: NonNull<BufRingEntry>, entries: u16) {
        let Ok(layout) = Layout::from_size_align(entries as usize * 16, 16) else {
            panic!("invalid layout for {entries} entries");
        };
        // SAFETY: Invariant -- region was allocated by alloc_ring_region
        // with matching layout.
        // Precondition: region and entries match the alloc call.
        // Failure mode: mismatched layout causes heap corruption.
        unsafe { dealloc(region.as_ptr().cast::<u8>(), layout) }
    }

    #[test]
    fn new_pushes_all_entries_to_ring() {
        let entries = 4u16;
        let region = alloc_ring_region(entries);
        let Ok(pool) = BufRingPool::new(entries, 64, BufGroupId::new(0), region) else {
            panic!("pool creation must succeed");
        };

        let ring = &pool.ring;
        assert_eq!(ring.tail().load(Ordering::Relaxed), entries);

        dealloc_ring_region(region, entries);
    }

    #[test]
    fn get_returns_correct_slice_length() {
        let entries = 4u16;
        let region = alloc_ring_region(entries);
        let Ok(pool) = BufRingPool::new(entries, 128, BufGroupId::new(1), region) else {
            panic!("pool creation must succeed");
        };

        let slice = pool.get(0, 64);
        assert_eq!(slice.len(), 64);

        let slice_full = pool.get(0, 128);
        assert_eq!(slice_full.len(), 128);

        dealloc_ring_region(region, entries);
    }

    #[test]
    fn get_returns_distinct_regions_per_bid() {
        let entries = 4u16;
        let buf_size = 32u32;
        let region = alloc_ring_region(entries);
        let Ok(pool) = BufRingPool::new(entries, buf_size, BufGroupId::new(0), region) else {
            panic!("pool creation must succeed");
        };

        let s0 = pool.get(0, buf_size).as_ptr();
        let s1 = pool.get(1, buf_size).as_ptr();
        let s2 = pool.get(2, buf_size).as_ptr();

        assert_eq!(s1 as usize - s0 as usize, buf_size as usize);
        assert_eq!(s2 as usize - s1 as usize, buf_size as usize);

        dealloc_ring_region(region, entries);
    }

    #[test]
    fn recycle_advances_tail() {
        let entries = 2u16;
        let region = alloc_ring_region(entries);
        let Ok(pool) = BufRingPool::new(entries, 64, BufGroupId::new(0), region) else {
            panic!("pool creation must succeed");
        };

        let tail_after_init = pool.ring.tail().load(Ordering::Relaxed);
        pool.recycle(0);
        let tail_after_recycle = pool.ring.tail().load(Ordering::Relaxed);

        assert_eq!(tail_after_recycle, tail_after_init.wrapping_add(1));

        dealloc_ring_region(region, entries);
    }

    #[test]
    fn accessors_return_construction_values() {
        let entries = 4u16;
        let region = alloc_ring_region(entries);
        let Ok(pool) = BufRingPool::new(entries, 256, BufGroupId::new(7), region) else {
            panic!("pool creation must succeed");
        };

        assert_eq!(pool.entries(), 4);
        assert_eq!(pool.buf_size(), 256);
        assert_eq!(pool.group_id(), BufGroupId::new(7));

        dealloc_ring_region(region, entries);
    }

    #[test]
    fn new_rejects_zero_buf_size() {
        let region = alloc_ring_region(2);
        let result = BufRingPool::new(2, 0, BufGroupId::new(0), region);
        assert!(result.is_err());
        dealloc_ring_region(region, 2);
    }

    #[test]
    fn new_rejects_entries_over_max() {
        let result = BufRingPool::new(u16::MAX, 64, BufGroupId::new(0), NonNull::dangling());
        assert!(result.is_err());
    }
}
