//! [`TimerEntry`] -- per-timer state stored in the wheel's slab.

use core::num::NonZeroU32;

use crate::task::TaskRef;

/// A single timer entry in the hierarchical wheel.
///
/// Stored in a `Slab<TimerEntry>` owned by the wheel. Intrusive
/// doubly-linked list links (`prev`/`next`) use [`NonZeroU32`] for
/// niche-optimized `Option<NonZeroU32>` (4 bytes).
pub(crate) struct TimerEntry {
    /// Absolute tick at which this timer expires.
    pub(crate) deadline_tick: u64,
    /// Task to wake on expiry.
    pub(crate) task_ref: TaskRef,
    /// Previous entry in the same wheel slot (intrusive link).
    pub(crate) prev: Option<NonZeroU32>,
    /// Next entry in the same wheel slot (intrusive link).
    pub(crate) next: Option<NonZeroU32>,
    /// Wheel level this entry currently resides in.
    pub(crate) level: u8,
    /// Slot index within the level.
    pub(crate) slot: u8,
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use kwokka_core::{Generation, slab::SlabKey};

    use super::*;

    fn dummy_task_ref() -> TaskRef {
        TaskRef::from_slab(0, SlabKey::new(0, Generation::ZERO))
    }

    #[test]
    fn entry_default_links_are_none() {
        let entry = TimerEntry {
            deadline_tick: 100,
            task_ref: dummy_task_ref(),
            prev: None,
            next: None,
            level: 0,
            slot: 0,
        };
        assert!(entry.prev.is_none());
        assert!(entry.next.is_none());
    }

    #[test]
    fn non_zero_u32_niche_optimization() {
        assert_eq!(
            core::mem::size_of::<Option<NonZeroU32>>(),
            core::mem::size_of::<u32>(),
        );
    }

    #[test]
    fn entry_stores_deadline_and_task_ref() {
        let task = dummy_task_ref();
        let entry = TimerEntry {
            deadline_tick: 42,
            task_ref: task,
            prev: None,
            next: None,
            level: 2,
            slot: 15,
        };
        assert_eq!(entry.deadline_tick, 42);
        assert_eq!(entry.task_ref, task);
        assert_eq!(entry.level, 2);
        assert_eq!(entry.slot, 15);
    }
}
