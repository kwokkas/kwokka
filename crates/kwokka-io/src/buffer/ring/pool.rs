//! Per-worker buffer ring pool for `io_uring` provided-buffer operations.
//!
//! `BufRingPool` (crate-internal) pairs mmap-backed contiguous storage with a
//! `BufRing` so the kernel can pick buffers automatically for provided-buffer
//! recv. [`ProvidedBuf`] is the borrowed view a completed recv resolves into:
//! the pool owns the bytes, the handle owns the buffer id, and dropping the
//! handle recycles the id back to the ring.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{marker::PhantomData, ops::Deref};
use std::io;

use crate::{
    boundary,
    buffer::{
        mmap::MmapRegion,
        ring::{BufRing, BufRingEntry},
        slot::BufGroupId,
    },
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
    #[cfg_attr(not(test), expect(dead_code, reason = "driver sizing test reads it"))]
    pub(crate) const fn buf_size(&self) -> u32 {
        self.buf_size
    }
}

/// A borrowed view over one kernel-selected provided buffer.
///
/// A completed provided-buffer recv resolves into this handle: the worker's
/// provided-buffer pool owns the bytes, and the handle is the exclusive claim
/// on the buffer id the kernel picked, so nothing writes the region while the
/// handle is alive. Dropping it recycles the id back to the ring, making the
/// buffer kernel-selectable again -- consume the bytes first.
///
/// The handle is `!Send` and `!Sync`: byte access and the recycle on drop are
/// single-thread contracts with the pool-owning worker, where the submitting
/// task is already pinned. On a work-stealing runtime, read and drop the
/// handle without holding it across an `.await`, so the task future stays
/// `Send`.
///
/// A handle carried outside its originating runtime run (for example returned
/// out of `block_on`) refuses byte access by panicking rather than reading
/// through a torn-down pool, and its drop skips the recycle -- a bounded loss
/// of one pool entry.
pub struct ProvidedBuf {
    /// Kernel-selected buffer id, or `None` for the empty end-of-stream view.
    buf_id: Option<u16>,
    /// Kernel-confirmed byte count, at most the pool's per-buffer size.
    len: u32,
    /// Worker whose pool owns the bytes -- the run-loop registry key.
    worker_id: u8,
    /// Pool-registration epoch captured at construction; access through a
    /// slot a later registration re-claimed is refused on mismatch.
    epoch: u32,
    /// Keeps the handle off `Send`/`Sync`: the recycle push and the byte
    /// window are contracts with one worker thread.
    _local: PhantomData<*const ()>,
}

impl ProvidedBuf {
    /// Wraps the kernel-selected buffer `buf_id` holding `len` received bytes.
    pub(crate) const fn new(worker_id: u8, epoch: u32, buf_id: u16, len: u32) -> Self {
        Self {
            buf_id: Some(buf_id),
            len,
            worker_id,
            epoch,
            _local: PhantomData,
        }
    }

    /// The empty view an end-of-stream completion resolves into when the
    /// kernel consumed no buffer.
    pub(crate) const fn empty() -> Self {
        Self {
            buf_id: None,
            len: 0,
            worker_id: 0,
            epoch: 0,
            _local: PhantomData,
        }
    }

    /// Borrows the received bytes.
    ///
    /// # Panics
    ///
    /// Panics when accessed outside the runtime run that produced the handle:
    /// the worker's pool registration is gone (the run-loop exited) or was
    /// re-claimed by a later registration, so there is no pool to read
    /// through. Also panics if the completion named a buffer id or length
    /// outside the pool's registered range.
    pub fn as_slice(&self) -> &[u8] {
        let Some(buf_id) = self.buf_id else {
            return &[];
        };
        let Some(pool) = boundary::provided_pool(self.worker_id, self.epoch) else {
            panic!("ProvidedBuf accessed outside its runtime's run-loop");
        };
        // SAFETY: Invariant -- a `Some` from `provided_pool` names the live
        // `BufRingPool` the guard installed for this worker at this epoch;
        // the guard is declared after the shard at every run-loop entry, so
        // LIFO drop nulls the slot before the pool unmaps, and the epoch
        // check refuses a slot a later registration re-claimed.
        // Precondition: the handle is `!Send`/`!Sync`, so this access runs on
        // the installing worker thread, and a non-null epoch-matched slot
        // means that thread is inside the run-loop session that installed it
        // -- the shard, and the pool it owns, cannot be torn down under a
        // borrow this same thread is holding.
        // Failure mode: a dangling pool deref -- excluded by the null and
        // epoch refusals plus the single-thread bracket.
        let pool = unsafe { pool.as_ref() };
        pool.get(buf_id, self.len)
    }
}

impl Deref for ProvidedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for ProvidedBuf {
    fn drop(&mut self) {
        let Some(buf_id) = self.buf_id else {
            return;
        };
        // The run-loop exited or a later registration owns the slot: the pool
        // unmaps (or already did) with the id checked out -- a bounded loss of
        // one entry, never a push into a reclaimed ring.
        let Some(pool) = boundary::provided_pool(self.worker_id, self.epoch) else {
            return;
        };
        // SAFETY: Invariant -- a `Some` from `provided_pool` names the live
        // `BufRingPool` the guard installed for this worker at this epoch;
        // the guard is declared after the shard at every run-loop entry, so
        // LIFO drop nulls the slot before the pool unmaps, and the epoch
        // check refuses a slot a later registration re-claimed.
        // Precondition: the handle is `!Send`/`!Sync`, so this recycle runs
        // on the installing worker thread, inside the run-loop session that
        // installed the slot -- the pool outlives the call, and the ring
        // push stays single-writer on that thread.
        // Failure mode: a recycle into a torn-down or re-claimed ring --
        // excluded by the null and epoch refusals.
        let pool = unsafe { pool.as_ref() };
        pool.recycle(buf_id);
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

    #[test]
    fn provided_buf_empty_view_is_inert() {
        let view = ProvidedBuf::empty();
        assert!(view.is_empty(), "the end-of-stream view holds no bytes");
        assert_eq!(view.as_slice(), &[] as &[u8]);
        // Dropping it recycles nothing and touches no pool registration.
        drop(view);
    }

    #[test]
    fn provided_buf_reads_and_recycles_through_the_registration() {
        let worker_id = 220;
        let Ok(pool) = BufRingPool::new(4, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let _guard = boundary::ProvidedPoolGuard::install_pool(worker_id, Some(&pool));
        let epoch = boundary::provided_pool_epoch(worker_id);
        let view = ProvidedBuf::new(worker_id, epoch, 2, 16);
        assert_eq!(view.len(), 16, "the view spans the kernel-confirmed count");
        assert_eq!(
            view.as_slice().as_ptr(),
            pool.get(2, 16).as_ptr(),
            "the view reads the pool's own storage, no copy",
        );
        let before = pool.ring.tail().load(Ordering::Relaxed);
        drop(view);
        let after = pool.ring.tail().load(Ordering::Relaxed);
        assert_eq!(
            after,
            before.wrapping_add(1),
            "the drop recycled the buffer id back to the ring",
        );
    }

    #[test]
    #[should_panic(expected = "outside its runtime's run-loop")]
    fn provided_buf_refuses_access_outside_a_run() {
        let worker_id = 221;
        let view = ProvidedBuf::new(worker_id, boundary::provided_pool_epoch(worker_id), 0, 8);
        // No registration is installed for this worker, so the read must
        // refuse by panicking rather than dereference a torn-down pool.
        let _bytes = view.as_slice();
    }

    #[test]
    #[should_panic(expected = "outside its runtime's run-loop")]
    fn provided_buf_refuses_a_stale_epoch() {
        let worker_id = 222;
        let Ok(pool) = BufRingPool::new(4, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let _guard = boundary::ProvidedPoolGuard::install_pool(worker_id, Some(&pool));
        let stale = boundary::provided_pool_epoch(worker_id).wrapping_sub(1);
        let view = ProvidedBuf::new(worker_id, stale, 0, 8);
        // The slot is installed, but the handle predates this registration;
        // reading through the wrong pool must be refused.
        let _bytes = view.as_slice();
    }

    #[test]
    fn provided_buf_drop_without_a_registration_is_quiet() {
        let worker_id = 223;
        let view = ProvidedBuf::new(worker_id, boundary::provided_pool_epoch(worker_id), 1, 8);
        // No pool is installed: the recycle is skipped -- a bounded loss on a
        // pool that unmaps regardless -- and nothing panics.
        drop(view);
    }
}
