//! The per-worker provided-buffer pool registry, and reading a completed
//! provided recv back through it.

use core::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicU64, Ordering},
};
use std::io;

use crate::{
    DriverType,
    buffer::ring::pool::{BufRingPool, ProvidedBuf},
};

/// Resolves a provided-buffer recv completion into the buffer view it names.
///
/// `result` is the CQE result and `buf_id` the kernel-selected provided buffer
/// (`io_uring_prep_recv.3`: a `BUFFER_SELECT` recv reports its chosen buffer in
/// the CQE flags). A negative result maps to the corresponding [`io::Error`]. A
/// nonnegative result with a buffer resolves into a [`ProvidedBuf`] borrowing the
/// worker pool's bytes, which recycles the buffer to the ring on drop, so the
/// caller owns that recycle by holding and dropping the view. End of stream
/// completes with a zero result and no buffer, resolving into the empty view;
/// data without a buffer id cannot name its bytes and surfaces as
/// [`io::ErrorKind::InvalidData`] rather than a panic.
///
/// The single-shot provided recv and the multishot recv stream both resolve
/// their completions through this one path, so a caller reading a buffer view
/// gets identical bytes and identical recycle semantics whichever recv shape
/// produced it.
///
/// # Errors
///
/// Returns the mapped `-errno` for a negative completion, or
/// [`io::ErrorKind::InvalidData`] when a positive result carries no buffer id.
pub fn resolve_provided_recv(
    worker_id: u8,
    result: i32,
    buf_id: Option<u16>,
) -> io::Result<ProvidedBuf> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    let len = u32::try_from(result).unwrap_or(0);
    match buf_id {
        Some(buf_id) => Ok(ProvidedBuf::new(
            worker_id,
            provided_pool_epoch(worker_id),
            buf_id,
            len,
        )),
        None if result == 0 => Ok(ProvidedBuf::empty()),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provided recv completed with data but no kernel-selected buffer",
        )),
    }
}

/// One provided-pool slot per possible worker id byte, like [`SEAM_SLOTS`].
const PROVIDED_POOL_SLOTS: usize = u8::MAX as usize + 1;

/// The installed provided-buffer pool for each worker, or null outside a
/// run-loop, indexed by worker id.
///
/// Run-loop scoped like [`CANCEL_INBOXES`], not poll-window scoped: a
/// `ProvidedBuf` reads its bytes and recycles its buffer id from arbitrary
/// task code and from a drop in task reap, both outside the poll bracket, yet
/// always on the owning worker thread. `AtomicPtr<BufRingPool>` is `Sync`
/// regardless of the pointee, so the array is a sound `static` with no
/// `unsafe impl`.
static PROVIDED_POOLS: [AtomicPtr<BufRingPool>; PROVIDED_POOL_SLOTS] =
    [const { AtomicPtr::new(ptr::null_mut()) }; PROVIDED_POOL_SLOTS];

/// Registration epoch per provided-pool slot, bumped on every
/// [`ProvidedPoolGuard`] install.
///
/// A `ProvidedBuf` captures the epoch at construction and every access
/// re-checks it, so a handle outliving its run-loop session can never read
/// through -- or recycle into -- a pool a later registration installed in the
/// same slot (a rebuilt runtime re-claiming the worker id). The counter is
/// 64-bit, so the wrap that would let a stale handle match again is
/// effectively unreachable -- the same headroom the buffer registries use
/// for their generations.
static PROVIDED_POOL_EPOCHS: [AtomicU64; PROVIDED_POOL_SLOTS] =
    [const { AtomicU64::new(0) }; PROVIDED_POOL_SLOTS];

/// RAII bracket that installs a worker's provided-buffer pool for its whole
/// run-loop and clears it on drop.
///
/// Declared after the `WorkerShard` local in each run-loop entry, so Rust
/// LIFO drop clears the static before the shard -- and the driver-owned pool
/// -- is reclaimed. A `ProvidedBuf` dropped during shard teardown then finds
/// a null slot and skips its recycle, a bounded pool-entry loss on a pool
/// that unmaps regardless.
///
/// Not re-entrant: one run-loop per worker installs one guard.
pub struct ProvidedPoolGuard {
    /// Worker slot to clear on drop.
    worker_id: u8,
}

impl ProvidedPoolGuard {
    /// Installs the driver's provided-buffer pool for `worker_id` for the
    /// run-loop, returning the guard that clears it.
    ///
    /// A backend without a pool (a fallback driver, or a uring driver whose
    /// registration failed or whose kernel lacks `buf_ring`) installs
    /// nothing; the guard's drop still clears the slot, a no-op.
    #[must_use]
    pub fn install(worker_id: u8, driver: &DriverType) -> Self {
        Self::install_pool(worker_id, driver.provided_recv_pool())
    }

    /// Installs `pool` for `worker_id`, bumping the slot's epoch first so a
    /// handle from an earlier session can never match this registration.
    ///
    /// Crate-visible so a test can bracket a pool without a live driver; the
    /// production path goes through [`install`](Self::install).
    pub(crate) fn install_pool(worker_id: u8, pool: Option<&BufRingPool>) -> Self {
        let Some(pool) = pool else {
            return Self { worker_id };
        };
        PROVIDED_POOL_EPOCHS[worker_id as usize].fetch_add(1, Ordering::AcqRel);
        PROVIDED_POOLS[worker_id as usize].store(ptr::from_ref(pool).cast_mut(), Ordering::Release);
        Self { worker_id }
    }
}

impl Drop for ProvidedPoolGuard {
    fn drop(&mut self) {
        PROVIDED_POOLS[self.worker_id as usize].store(ptr::null_mut(), Ordering::Release);
    }
}

/// The provided-buffer pool installed for `worker_id`, when `epoch` still
/// names the current registration.
///
/// Returns `None` outside a run-loop (the slot is null) and on an epoch
/// mismatch (a later registration re-claimed the slot), so a stale handle is
/// refused rather than read through the wrong pool. The pointee is live for
/// exactly the reasons the guard doc states; the caller performs the deref
/// under that contract.
pub(crate) fn provided_pool(worker_id: u8, epoch: u64) -> Option<NonNull<BufRingPool>> {
    let pool = NonNull::new(PROVIDED_POOLS[worker_id as usize].load(Ordering::Acquire))?;
    if PROVIDED_POOL_EPOCHS[worker_id as usize].load(Ordering::Acquire) != epoch {
        return None;
    }
    Some(pool)
}

/// The current pool-registration epoch for `worker_id`.
///
/// Captured into each `ProvidedBuf` at construction; [`provided_pool`]
/// re-checks it on every access.
pub(crate) fn provided_pool_epoch(worker_id: u8) -> u64 {
    PROVIDED_POOL_EPOCHS[worker_id as usize].load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provided_pool_guard_brackets_install_and_clear() {
        let Ok(pool) = crate::buffer::ring::pool::BufRingPool::new(
            4,
            64,
            crate::buffer::registration::slot::BufGroupId::new(0),
        ) else {
            panic!("pool creation must succeed");
        };
        let before = provided_pool_epoch(210);
        {
            let _guard = ProvidedPoolGuard::install_pool(210, Some(&pool));
            let epoch = provided_pool_epoch(210);
            assert_eq!(epoch, before.wrapping_add(1), "an install bumps the epoch");
            assert!(
                provided_pool(210, epoch).is_some(),
                "the current epoch resolves the installed pool",
            );
            assert!(
                provided_pool(210, epoch.wrapping_sub(1)).is_none(),
                "a stale epoch is refused",
            );
        }
        assert!(
            provided_pool(210, provided_pool_epoch(210)).is_none(),
            "the guard clears the slot on drop",
        );
    }

    #[test]
    fn provided_pool_guard_without_pool_installs_nothing() {
        let before = provided_pool_epoch(211);
        let _guard = ProvidedPoolGuard::install_pool(211, None);
        assert_eq!(
            provided_pool_epoch(211),
            before,
            "a poolless install does not bump the epoch",
        );
        assert!(provided_pool(211, before).is_none());
    }
}
