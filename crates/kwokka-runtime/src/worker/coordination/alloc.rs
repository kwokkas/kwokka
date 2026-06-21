//! Process-wide [`WorkerId`] allocation.
//!
//! A process-global occupancy bitmap hands out unique [`WorkerId`]s.
//! One bit per id over the 7-bit worker space; two words cover every slot
//! of the per-worker tables. Claims are compare-exchange loops on plain
//! atomics: no lock, no allocation, const-constructible.

use core::sync::atomic::{AtomicU64, Ordering};

use crate::worker::WorkerId;

/// Process-global occupancy bitmap handing out unique [`WorkerId`]s.
///
/// One bit per id over the 7-bit worker space, so two words cover every slot
/// of the per-worker tables. Claims are compare-exchange loops on plain
/// atomics: no lock, no allocation, const-constructible.
struct WorkerIdAllocator {
    /// Occupancy bits; bit `i` of word `w` covers id `w * 64 + i`.
    words: [AtomicU64; 2],
}

/// The single process-wide allocator. Two runtimes in one process claim
/// distinct ids here, so their per-worker table slots never collide.
static WORKER_ALLOC: WorkerIdAllocator = WorkerIdAllocator {
    words: [AtomicU64::new(0), AtomicU64::new(0)],
};

impl WorkerIdAllocator {
    /// Claims the lowest free id, or `None` when the space is exhausted.
    fn claim_one(&self) -> Option<WorkerId> {
        for (word_index, word) in self.words.iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            while bits != u64::MAX {
                let free = (!bits).trailing_zeros();
                match word.compare_exchange_weak(
                    bits,
                    bits | (1u64 << free),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let Ok(raw) = u8::try_from(word_index * 64 + free as usize) else {
                            return None;
                        };
                        return WorkerId::new(raw).ok();
                    }
                    Err(actual) => bits = actual,
                }
            }
        }
        None
    }

    /// Claims `count` contiguous ids, or `None` when no run fits.
    ///
    /// Bounded to one word (64 ids) so the claim stays a single
    /// compare-exchange; a spurious failure rescans the word from the start.
    fn claim_block(&self, count: usize) -> Option<WorkerId> {
        if count == 0 || count > 64 {
            return None;
        }
        let run = if count == 64 {
            u64::MAX
        } else {
            (1u64 << count) - 1
        };
        for (word_index, word) in self.words.iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            let mut shift = 0;
            while shift + count <= 64 {
                let mask = run << shift;
                if bits & mask != 0 {
                    shift += 1;
                    continue;
                }
                match word.compare_exchange_weak(
                    bits,
                    bits | mask,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let Ok(raw) = u8::try_from(word_index * 64 + shift) else {
                            return None;
                        };
                        return WorkerId::new(raw).ok();
                    }
                    Err(actual) => {
                        bits = actual;
                        shift = 0;
                    }
                }
            }
        }
        None
    }

    /// Releases one claimed id back to the pool.
    fn release(&self, id: WorkerId) {
        let raw = id.raw() as usize;
        let mask = 1u64 << (raw % 64);
        self.words[raw / 64].fetch_and(!mask, Ordering::Release);
    }

    /// Releases a block claimed by [`WorkerIdAllocator::claim_block`].
    fn release_block(&self, start: WorkerId, count: usize) {
        if count == 0 || count > 64 {
            return;
        }
        let run = if count == 64 {
            u64::MAX
        } else {
            (1u64 << count) - 1
        };
        let raw = start.raw() as usize;
        // A claimed block never crosses a word by construction.
        self.words[raw / 64].fetch_and(!(run << (raw % 64)), Ordering::Release);
    }
}

/// Claims the lowest free worker id for a new runtime.
pub(crate) fn claim_one() -> Option<WorkerId> {
    WORKER_ALLOC.claim_one()
}

/// Claims `count` contiguous worker ids for a multi-worker runtime.
pub(crate) fn claim_block(count: usize) -> Option<WorkerId> {
    WORKER_ALLOC.claim_block(count)
}

/// Releases one claimed worker id.
pub(crate) fn release(id: WorkerId) {
    WORKER_ALLOC.release(id);
}

/// Releases a contiguous block of worker ids.
pub(crate) fn release_block(start: WorkerId, count: usize) {
    WORKER_ALLOC.release_block(start, count);
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::{claim_block, claim_one, release, release_block};

    #[test]
    fn claims_are_distinct_while_held() {
        let Some(first) = claim_one() else {
            panic!("the id space must have a free slot");
        };
        let Some(second) = claim_one() else {
            panic!("the id space must have a second free slot");
        };
        assert_ne!(first, second, "two live claims never share an id");
        release(first);
        release(second);
    }

    #[test]
    fn released_id_is_claimable_again() {
        let Some(first) = claim_one() else {
            panic!("the id space must have a free slot");
        };
        release(first);
        let Some(second) = claim_one() else {
            panic!("a released id must leave the pool claimable");
        };
        release(second);
    }

    #[test]
    fn block_claims_do_not_overlap() {
        let Some(first) = claim_block(8) else {
            panic!("an 8-wide block must fit the id space");
        };
        let Some(second) = claim_block(8) else {
            panic!("a second 8-wide block must fit the id space");
        };
        let lower = first.raw().min(second.raw());
        let upper = first.raw().max(second.raw());
        assert!(lower + 8 <= upper, "blocks must be disjoint");
        release_block(first, 8);
        release_block(second, 8);
    }

    #[test]
    fn block_bounds_are_rejected() {
        assert_eq!(claim_block(0), None);
        assert_eq!(claim_block(65), None);
    }
}
