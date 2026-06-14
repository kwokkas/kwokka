//! [`Pip`] constructors - `root`, `root_on`, `issue`, `child`, `detached`.

use core::sync::atomic::{AtomicU64, Ordering};

use crate::id::{
    Pip,
    error::PipError,
    layout::{CURRENT_VERSION, DEPTH_SHIFT, SEQ_SHIFT, VERSION_SHIFT, WORKER_BITS, WORKER_SHIFT},
};

/// Process-wide monotonic sequence counter. Starts at 1 (0 reserved for "no id").
static GLOBAL_SEQ: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_seq() -> u64 {
    GLOBAL_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Pack the bit fields into a raw `u128`. `kind` and `flags` are reserved (currently zero).
#[inline]
const fn pack(seq: u64, depth: u16, worker: u64) -> u128 {
    let worker_masked = (worker as u128) & ((1u128 << WORKER_BITS) - 1);
    ((seq as u128) << SEQ_SHIFT)
        | ((depth as u128) << DEPTH_SHIFT)
        | ((CURRENT_VERSION as u128) << VERSION_SHIFT)
        | (worker_masked << WORKER_SHIFT)
}

impl Pip {
    /// Create a root-level `Pip` with depth 0 and no worker hint.
    #[inline]
    pub fn root() -> Self {
        Self(pack(next_seq(), 0, 0))
    }

    /// Create a root-level `Pip` pinned to a specific worker.
    ///
    /// Values exceeding `WORKER_BITS` (38 bits) are silently masked to that width.
    #[inline]
    pub fn root_on(worker_id: u64) -> Self {
        Self(pack(next_seq(), 0, worker_id))
    }

    /// Mint a depth-0 `Pip` from a caller-supplied sequence number.
    ///
    /// Unlike [`root`](Self::root) and [`root_on`](Self::root_on), this does
    /// not touch the process-global sequence counter -- the caller supplies
    /// `seq` from its own monotonic source (a per-worker counter), so issuance
    /// is contention-free. Uniqueness is `(worker_id, seq)`: a per-issuer
    /// counter plus the embedded issuer id.
    ///
    /// `worker_id` is the ISSUING worker, not the executing one. A task may
    /// migrate to another worker under work-stealing, but its `Pip` keeps the
    /// id of the worker that minted it -- pip is invariant, location is
    /// not. Values exceeding `WORKER_BITS` (38 bits) are silently masked.
    ///
    /// Callers must avoid `(worker_id, seq) == (0, 0)`: it packs to `Pip`(0),
    /// the reserved "no id". `CURRENT_VERSION` is 0, so the version field sets
    /// no distinguishing bits and the all-zero pair is the sentinel. Start
    /// per-issuer counters at 1 (as the per-worker issuer does) to satisfy
    /// this unconditionally.
    #[inline]
    pub const fn issue(worker_id: u64, seq: u64) -> Self {
        Self(pack(seq, 0, worker_id))
    }

    /// Create a child `Pip` nested one level below `self`.
    ///
    /// Inherits the parent's worker hint, increments depth by 1, and assigns
    /// a fresh sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`PipError::DepthOverflow`] if the depth counter would exceed
    /// `u16::MAX`.
    #[inline]
    pub fn child(self) -> Result<Self, PipError> {
        let child_depth = self.depth().checked_add(1).ok_or(PipError::DepthOverflow)?;
        Ok(Self(pack(next_seq(), child_depth, self.worker_id())))
    }

    /// Create a detached `Pip` with no parent relationship.
    ///
    /// Equivalent in effect to [`Self::root`]; the distinct name documents
    /// intent at the call site (e.g., a task spawned outside any conductor).
    #[inline]
    pub fn detached() -> Self {
        Self::root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_has_depth_zero_no_worker() {
        let id = Pip::root();
        assert_eq!(id.depth(), 0);
        assert_eq!(id.worker_id(), 0);
    }

    #[test]
    fn root_ids_have_unique_increasing_seq() {
        let a = Pip::root();
        let b = Pip::root();
        assert!(b.seq() > a.seq());
    }

    #[test]
    fn root_on_pins_worker() {
        let id = Pip::root_on(42);
        assert_eq!(id.worker_id(), 42);
        assert_eq!(id.depth(), 0);
    }

    #[test]
    fn root_on_masks_worker_overflow() {
        // Values beyond WORKER_BITS are silently masked; the call must succeed.
        let id = Pip::root_on(1u64 << WORKER_BITS);
        // (1 << WORKER_BITS) & ((1 << WORKER_BITS) - 1) == 0
        assert_eq!(id.worker_id(), 0);
    }

    #[test]
    fn child_increments_depth() {
        let root = Pip::root();
        let Ok(child) = root.child() else {
            unreachable!("root has depth 0, child cannot overflow")
        };
        assert_eq!(child.depth(), 1);
    }

    #[test]
    fn child_inherits_worker() {
        let root = Pip::root_on(7);
        let Ok(child) = root.child() else {
            unreachable!("root has depth 0, child cannot overflow")
        };
        assert_eq!(child.worker_id(), 7);
    }

    #[test]
    fn child_depth_overflow_at_u16_max() {
        let raw = (1u128 << SEQ_SHIFT) | (u128::from(u16::MAX) << DEPTH_SHIFT);
        let maxed = Pip(raw);
        assert!(matches!(maxed.child(), Err(PipError::DepthOverflow)));
    }

    #[test]
    fn detached_is_root_equivalent() {
        let d = Pip::detached();
        assert_eq!(d.depth(), 0);
        assert_eq!(d.worker_id(), 0);
    }

    #[test]
    fn issue_embeds_worker_and_seq() {
        let id = Pip::issue(7, 42);
        assert_eq!(id.worker_id(), 7);
        assert_eq!(id.seq(), 42);
        assert_eq!(id.depth(), 0);
    }

    #[test]
    fn issue_takes_the_supplied_seq() {
        // The seq is the caller's, not the process-global counter.
        assert_eq!(Pip::issue(3, 100).seq(), 100);
        assert_eq!(Pip::issue(3, 1).seq(), 1);
    }

    #[test]
    fn issue_workers_do_not_collide() {
        assert_ne!(Pip::issue(1, 5).as_u128(), Pip::issue(2, 5).as_u128());
        assert_ne!(Pip::issue(1, 5).as_u128(), Pip::issue(1, 6).as_u128());
    }

    #[test]
    fn issue_masks_worker_overflow() {
        assert_eq!(Pip::issue(1u64 << WORKER_BITS, 5).worker_id(), 0);
    }
}
