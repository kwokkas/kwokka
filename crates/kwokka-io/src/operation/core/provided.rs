//! Borrowed views over kernel-selected provided buffers.

use core::{marker::PhantomData, ops::Deref};

#[cfg(target_os = "linux")]
use crate::{boundary, buffer::ring::pool::check_bid};

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
    #[cfg(target_os = "linux")]
    buf_id: Option<u16>,
    /// Kernel-confirmed byte count, at most the pool's per-buffer size.
    #[cfg(target_os = "linux")]
    len: u32,
    /// Worker whose pool owns the bytes -- the run-loop registry key.
    #[cfg(target_os = "linux")]
    worker_id: u8,
    /// Pool-registration epoch captured at construction; access through a
    /// slot a later registration re-claimed is refused on mismatch.
    #[cfg(target_os = "linux")]
    epoch: u64,
    /// Keeps the handle off `Send`/`Sync`: the recycle push and the byte
    /// window are contracts with one worker thread.
    _local: PhantomData<*const ()>,
}

impl ProvidedBuf {
    /// Wraps the kernel-selected buffer `buf_id` holding `len` received bytes.
    #[cfg(target_os = "linux")]
    pub(crate) const fn new(worker_id: u8, epoch: u64, buf_id: u16, len: u32) -> Self {
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
    #[cfg(target_os = "linux")]
    pub(crate) const fn empty() -> Self {
        Self {
            buf_id: None,
            len: 0,
            worker_id: 0,
            epoch: 0,
            _local: PhantomData,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) const fn empty() -> Self {
        Self {
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
        self.bytes()
    }

    #[cfg(target_os = "linux")]
    fn bytes(&self) -> &[u8] {
        let Some(buf_id) = self.buf_id else {
            return &[];
        };
        let Some(pool) = boundary::seam::pool::provided_pool(self.worker_id, self.epoch) else {
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

    #[cfg(not(target_os = "linux"))]
    #[allow(
        clippy::missing_const_for_fn,
        reason = "the public wrapper must have the same non-const contract as the Linux pool view"
    )]
    fn bytes(&self) -> &[u8] {
        &[]
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
        #[cfg(target_os = "linux")]
        {
            let Some(buf_id) = self.buf_id else {
                return;
            };
            // The run-loop exited or a later registration owns the slot: the pool
            // unmaps (or already did) with the id checked out -- a bounded loss of
            // one entry, never a push into a reclaimed ring.
            let Some(pool) = boundary::seam::pool::provided_pool(self.worker_id, self.epoch) else {
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
            // A drop must never panic: an id past the ring (a malformed
            // completion whose byte access already panicked) is skipped rather
            // than recycled, so unwinding cannot double-panic here.
            if check_bid(buf_id, pool.entries()).is_err() {
                return;
            }
            pool.recycle(buf_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provided_buf_empty_view_is_inert() {
        let view = ProvidedBuf::empty();
        assert!(view.is_empty(), "the end-of-stream view holds no bytes");
        assert_eq!(view.as_slice(), &[] as &[u8]);
        drop(view);
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod linux_tests {
    use super::*;
    use crate::{
        boundary,
        buffer::{registration::slot::BufGroupId, ring::pool::BufRingPool},
    };

    #[test]
    fn provided_buf_reads_and_recycles_through_the_registration() {
        let worker_id = boundary::reserve_worker_id();
        let Ok(pool) = BufRingPool::new(4, 64, BufGroupId::new(0)) else {
            panic!("pool creation must succeed");
        };
        let _guard = boundary::ProvidedPoolGuard::install_pool(worker_id, Some(&pool));
        let epoch = boundary::seam::pool::provided_pool_epoch(worker_id);
        let view = ProvidedBuf::new(worker_id, epoch, 2, 16);
        assert_eq!(view.len(), 16, "the view spans the kernel-confirmed count");
        assert_eq!(view.as_slice().as_ptr(), pool.get(2, 16).as_ptr());
        let before = pool.ring_tail();
        drop(view);
        let after = pool.ring_tail();
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
        let view = ProvidedBuf::new(
            worker_id,
            boundary::seam::pool::provided_pool_epoch(worker_id),
            0,
            8,
        );
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
        let stale = boundary::seam::pool::provided_pool_epoch(worker_id).wrapping_sub(1);
        let view = ProvidedBuf::new(worker_id, stale, 0, 8);
        let _bytes = view.as_slice();
    }

    #[test]
    fn provided_buf_drop_without_a_registration_is_quiet() {
        let worker_id = 223;
        let view = ProvidedBuf::new(
            worker_id,
            boundary::seam::pool::provided_pool_epoch(worker_id),
            1,
            8,
        );
        drop(view);
    }
}
