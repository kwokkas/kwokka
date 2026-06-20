//! Wheel slot and level types with intrusive linked list operations.

use core::num::NonZeroU32;

use crate::timer::{entry::TimerEntry, nz_to_slab};
use kwokka_core::slab::Slab;

/// Number of slots per wheel level.
pub(crate) const SLOTS_PER_LEVEL: usize = 64;

/// Single slot in one wheel level.
///
/// Holds the head of an intrusive doubly-linked list of timer entries.
pub(crate) struct WheelSlot {
    /// Head entry in this slot, or `None` if empty.
    pub(crate) head: Option<NonZeroU32>,
}

impl WheelSlot {
    /// Creates an empty slot.
    pub(crate) const fn new() -> Self {
        Self { head: None }
    }

    /// Returns `true` when no entries are linked in this slot.
    pub(crate) const fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// Prepend an entry to the front of this slot's list.
    ///
    /// O(1). Updates the entry's `prev`/`next` links and the old
    /// head's `prev` link.
    pub(crate) fn push_front(&mut self, entry_nz: NonZeroU32, entries: &mut Slab<TimerEntry>) {
        let idx = nz_to_slab(entry_nz);
        let Some(entry) = entries.get_mut_by_index(idx) else {
            return;
        };
        entry.prev = None;
        entry.next = self.head;

        if let Some(old_head) = self.head {
            let old_idx = nz_to_slab(old_head);
            if let Some(old_entry) = entries.get_mut_by_index(old_idx) {
                old_entry.prev = Some(entry_nz);
            }
        }

        self.head = Some(entry_nz);
    }

    /// Remove an entry from this slot's list.
    ///
    /// O(1). Patches the predecessor's `next` and successor's `prev`.
    /// Returns `true` if the slot is now empty.
    pub(crate) fn remove(&mut self, entry_nz: NonZeroU32, entries: &mut Slab<TimerEntry>) -> bool {
        let idx = nz_to_slab(entry_nz);
        let Some(entry) = entries.get_by_index(idx) else {
            return self.head.is_none();
        };
        let prev = entry.prev;
        let next = entry.next;

        if let Some(prev_nz) = prev {
            let prev_idx = nz_to_slab(prev_nz);
            if let Some(prev_entry) = entries.get_mut_by_index(prev_idx) {
                prev_entry.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next_nz) = next {
            let next_idx = nz_to_slab(next_nz);
            if let Some(next_entry) = entries.get_mut_by_index(next_idx) {
                next_entry.prev = prev;
            }
        }

        if let Some(entry) = entries.get_mut_by_index(idx) {
            entry.prev = None;
            entry.next = None;
        }

        self.head.is_none()
    }
}

/// One level of the hierarchical wheel.
///
/// Contains [`SLOTS_PER_LEVEL`] slots and a `populated_mask` bitmask.
/// The mask is maintained by the `TimerWheel` (not by `WheelSlot`
/// operations) to enable O(1) `next_expiry` via `trailing_zeros()`.
pub(crate) struct WheelLevel {
    /// Slots in this level.
    pub(crate) slots: [WheelSlot; SLOTS_PER_LEVEL],
    /// Bitmask of non-empty slots.
    pub(crate) populated_mask: u64,
}

impl WheelLevel {
    /// Creates an empty level.
    pub(crate) const fn new() -> Self {
        const EMPTY_SLOT: WheelSlot = WheelSlot::new();
        Self {
            slots: [EMPTY_SLOT; SLOTS_PER_LEVEL],
            populated_mask: 0,
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::task::TaskRef;
    use crate::timer::slab_to_nz;
    use kwokka_core::{Generation, slab::SlabKey};

    fn dummy_task_ref() -> TaskRef {
        TaskRef::from_slab(0, SlabKey::new(0, Generation::ZERO))
    }

    fn make_entry(deadline: u64) -> TimerEntry {
        TimerEntry {
            deadline_tick: deadline,
            task_ref: dummy_task_ref(),
            prev: None,
            next: None,
            level: 0,
            slot: 0,
        }
    }

    #[test]
    fn empty_slot() {
        let slot = WheelSlot::new();
        assert!(slot.is_empty());
    }

    #[test]
    fn push_front_single() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(key) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let nz = slab_to_nz(key.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz, &mut slab);

        assert!(!slot.is_empty());
        assert_eq!(slot.head, Some(nz));
    }

    #[test]
    fn push_front_two_entries() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(k1) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let Ok(k2) = slab.insert(make_entry(20)) else {
            panic!("insert must succeed");
        };
        let nz1 = slab_to_nz(k1.index());
        let nz2 = slab_to_nz(k2.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz1, &mut slab);
        slot.push_front(nz2, &mut slab);

        assert_eq!(slot.head, Some(nz2));

        let Some(entry2) = slab.get(k2) else {
            panic!("entry must exist");
        };
        assert_eq!(entry2.next, Some(nz1));

        let Some(entry1) = slab.get(k1) else {
            panic!("entry must exist");
        };
        assert_eq!(entry1.prev, Some(nz2));
    }

    #[test]
    fn remove_head() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(k1) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let Ok(k2) = slab.insert(make_entry(20)) else {
            panic!("insert must succeed");
        };
        let nz1 = slab_to_nz(k1.index());
        let nz2 = slab_to_nz(k2.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz1, &mut slab);
        slot.push_front(nz2, &mut slab);

        let is_empty = slot.remove(nz2, &mut slab);
        assert!(!is_empty);
        assert_eq!(slot.head, Some(nz1));
    }

    #[test]
    fn remove_tail() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(k1) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let Ok(k2) = slab.insert(make_entry(20)) else {
            panic!("insert must succeed");
        };
        let nz1 = slab_to_nz(k1.index());
        let nz2 = slab_to_nz(k2.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz1, &mut slab);
        slot.push_front(nz2, &mut slab);

        let is_empty = slot.remove(nz1, &mut slab);
        assert!(!is_empty);
        assert_eq!(slot.head, Some(nz2));

        let Some(entry2) = slab.get(k2) else {
            panic!("entry must exist");
        };
        assert_eq!(entry2.next, None);
    }

    #[test]
    fn remove_last_returns_empty() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(key) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let nz = slab_to_nz(key.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz, &mut slab);

        let is_empty = slot.remove(nz, &mut slab);
        assert!(is_empty);
        assert!(slot.is_empty());
    }

    #[test]
    fn remove_middle() {
        let mut slab: Slab<TimerEntry> = Slab::new(4);
        let Ok(k1) = slab.insert(make_entry(10)) else {
            panic!("insert must succeed");
        };
        let Ok(k2) = slab.insert(make_entry(20)) else {
            panic!("insert must succeed");
        };
        let Ok(k3) = slab.insert(make_entry(30)) else {
            panic!("insert must succeed");
        };
        let nz1 = slab_to_nz(k1.index());
        let nz2 = slab_to_nz(k2.index());
        let nz3 = slab_to_nz(k3.index());

        let mut slot = WheelSlot::new();
        slot.push_front(nz1, &mut slab);
        slot.push_front(nz2, &mut slab);
        slot.push_front(nz3, &mut slab);

        let is_empty = slot.remove(nz2, &mut slab);
        assert!(!is_empty);

        let Some(entry3) = slab.get(k3) else {
            panic!("entry must exist");
        };
        assert_eq!(entry3.next, Some(nz1));

        let Some(entry1) = slab.get(k1) else {
            panic!("entry must exist");
        };
        assert_eq!(entry1.prev, Some(nz3));
    }

    #[test]
    fn wheel_level_starts_empty() {
        let level = WheelLevel::new();
        assert_eq!(level.populated_mask, 0);
        for slot in &level.slots {
            assert!(slot.is_empty());
        }
    }

    #[test]
    fn populated_mask_size() {
        assert_eq!(SLOTS_PER_LEVEL, 64);
    }
}
