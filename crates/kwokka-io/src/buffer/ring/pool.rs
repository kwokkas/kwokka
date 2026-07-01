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
    /// View over `ring_region`; the kernel picks buffers through it.
    ring: BufRing,
    /// Kernel-shared ring metadata region backing `ring`. Owned here so it
    /// outlives both the view and the `IORING_REGISTER_PBUF_RING`
    /// registration (`io_uring_register_buf_ring.3`) -- the pool is the sole
    /// owner of this memory.
    ring_region: MmapRegion,
    /// Buffer storage the kernel writes recv payloads into.
    storage: MmapRegion,
    buf_size: u32,
    entries: u16,
    group_id: BufGroupId,
}

impl BufRingPool {
    /// Allocate the ring metadata region and buffer storage, then push all
    /// entries so the kernel can select them.
    ///
    /// The pool owns both mmap regions it allocates. The caller registers
    /// the ring with the kernel via [`ring_addr`](Self::ring_addr) after
    /// construction; on registration failure the pool drops, unmapping the
    /// never-registered region with no dangling kernel reference.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if `entries` is zero, not a power of two,
    /// or exceeds 32768; if `buf_size` is zero; or if either mmap
    /// allocation fails.
    pub(crate) fn new(entries: u16, buf_size: u32, group_id: BufGroupId) -> io::Result<Self> {
        check_params(entries, buf_size)?;

        let entries_usize = entries as usize;
        let buf_size_usize = buf_size as usize;

        let ring_region = MmapRegion::new(entries_usize * core::mem::size_of::<BufRingEntry>())?;
        let ring_ptr = ring_region.as_non_null().cast::<BufRingEntry>();
        let ring = BufRing::from_raw(ring_ptr, entries);
        let storage = MmapRegion::new(entries_usize * buf_size_usize)?;

        let base = storage.as_ptr() as u64;
        for bid in 0..entries {
            let addr = base + u64::from(bid) * u64::from(buf_size);
            ring.push(addr, buf_size, bid);
        }

        Ok(Self {
            ring,
            ring_region,
            storage,
            buf_size,
            entries,
            group_id,
        })
    }

    /// Kernel address of the ring metadata region.
    ///
    /// Passed as `ring_addr` to `IORING_REGISTER_PBUF_RING`
    /// (`io_uring_register_buf_ring.3`). The region stays mapped for the
    /// pool's lifetime, so the address is valid until the pool drops.
    pub(crate) fn ring_addr(&self) -> u64 {
        self.ring_region.as_ptr() as u64
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

fn check_params(entries: u16, buf_size: u32) -> io::Result<()> {
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
    Ok(())
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

    use super::*;

    #[test]
    fn new_pushes_every_entry() {
        let entries = 4u16;
        let Ok(pool) = BufRingPool::new(entries, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        assert_eq!(pool.ring.tail().load(Ordering::Relaxed), entries);
    }

    #[test]
    fn get_returns_correct_slice_length() {
        let Ok(pool) = BufRingPool::new(4, 128, BufGroupId::new(1)) else {
            panic!("pool creation must succeed");
        };
        assert_eq!(pool.get(0, 64).len(), 64);
        assert_eq!(pool.get(0, 128).len(), 128);
    }

    #[test]
    fn bids_map_to_distinct_regions() {
        let buf_size = 32u32;
        let Ok(pool) = BufRingPool::new(4, buf_size, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let s0 = pool.get(0, buf_size).as_ptr();
        let s1 = pool.get(1, buf_size).as_ptr();
        let s2 = pool.get(2, buf_size).as_ptr();

        assert_eq!(s1 as usize - s0 as usize, buf_size as usize);
        assert_eq!(s2 as usize - s1 as usize, buf_size as usize);
    }

    #[test]
    fn recycle_advances_tail() {
        let Ok(pool) = BufRingPool::new(2, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let before = pool.ring.tail().load(Ordering::Relaxed);
        pool.recycle(0);
        let after = pool.ring.tail().load(Ordering::Relaxed);

        assert_eq!(after, before.wrapping_add(1));
    }

    #[test]
    fn accessors_return_construction_values() {
        let Ok(pool) = BufRingPool::new(4, 256, BufGroupId::new(7)) else {
            panic!("pool creation must succeed");
        };
        assert_eq!(pool.entries(), 4);
        assert_eq!(pool.buf_size(), 256);
        assert_eq!(pool.group_id(), BufGroupId::new(7));
    }

    #[test]
    fn ring_addr_is_non_zero() {
        let Ok(pool) = BufRingPool::new(4, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        assert_ne!(pool.ring_addr(), 0);
    }

    #[test]
    fn new_rejects_zero_buf_size() {
        assert!(BufRingPool::new(2, 0, BufGroupId::new(0)).is_err());
    }

    #[test]
    fn new_rejects_zero_entries() {
        assert!(BufRingPool::new(0, 64, BufGroupId::new(0)).is_err());
    }

    #[test]
    fn new_rejects_non_pow2_entries() {
        assert!(BufRingPool::new(3, 64, BufGroupId::new(0)).is_err());
    }

    #[test]
    fn new_rejects_entries_over_max() {
        // u16::MAX is not a power of two, so the pow2 guard rejects it; no
        // power-of-two u16 exceeds the kernel's 32768 limit.
        assert!(BufRingPool::new(u16::MAX, 64, BufGroupId::new(0)).is_err());
    }
}
