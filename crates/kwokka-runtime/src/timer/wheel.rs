//! [`TimerWheel`] -- hierarchical 6-level timing wheel.

use core::num::NonZeroU32;

use kwokka_core::slab::{Slab, SlabError};

use crate::{
    task::TaskRef,
    timer::{
        TimerHandle, clock::Clock, entry::TimerEntry, nz_to_slab, slab_to_nz, slot::WheelLevel,
    },
};

const LEVEL_COUNT: usize = 6;

/// Hierarchical timing wheel with 6 levels of 64 slots each.
///
/// Level coverage at 1ms tick: 64ms, ~4s, ~4.4min, ~4.7hr, ~12.5d,
/// ~2.2yr. The wheel does not call `waker.wake()` --
/// [`advance_to`](TimerWheel::advance_to) yields expired
/// [`TaskRef`] values for the caller to dispatch.
pub(crate) struct TimerWheel<C: Clock> {
    clock: C,
    current_tick: u64,
    levels: [WheelLevel; LEVEL_COUNT],
    entries: Slab<TimerEntry>,
}

const EMPTY_LEVEL: WheelLevel = WheelLevel::new();

impl<C: Clock> TimerWheel<C> {
    /// Create a new wheel anchored to the clock's current tick.
    ///
    /// `capacity` sets the maximum number of concurrent timer entries.
    pub(crate) fn new(clock: C, capacity: usize) -> Self {
        let current_tick = clock.now();
        Self {
            clock,
            current_tick,
            levels: [EMPTY_LEVEL; LEVEL_COUNT],
            entries: Slab::new(capacity),
        }
    }

    /// Register a timer that expires at `deadline_tick`.
    ///
    /// # Errors
    ///
    /// Returns [`SlabError::Full`] when the entry slab is at capacity.
    pub(crate) fn register(
        &mut self,
        task_ref: TaskRef,
        deadline_tick: u64,
    ) -> Result<TimerHandle, SlabError> {
        let entry = TimerEntry {
            deadline_tick,
            task_ref,
            prev: None,
            next: None,
            level: 0,
            slot: 0,
        };
        let key = self.entries.insert(entry)?;
        let nz = slab_to_nz(key.index());
        let (level, slot) = position(deadline_tick, self.current_tick);
        self.insert_at(nz, level, slot);
        Ok(TimerHandle::from_key(key))
    }

    /// Cancel a timer. Returns `true` if the entry was found and removed.
    #[must_use]
    #[allow(dead_code, reason = "drop-cancel deferred to 0.2.0")]
    pub(crate) fn cancel(&mut self, handle: TimerHandle) -> bool {
        if self.entries.get(handle.key()).is_none() {
            return false;
        }
        self.remove_from_slot(handle.nz());
        // IGNORE: get() above confirmed entry exists, remove always succeeds.
        let _ = self.entries.remove(handle.key());
        true
    }

    /// Advance the wheel to `target_tick`, yielding expired entries.
    #[must_use]
    #[allow(
        clippy::missing_const_for_fn,
        reason = "returns mutable borrow iterator"
    )]
    pub(crate) fn advance_to(&mut self, target_tick: u64) -> ExpiredIter<'_, C> {
        ExpiredIter {
            wheel: self,
            target_tick,
            current_drain: None,
        }
    }

    /// Tick count until the next expiry, or `None` if empty.
    pub(crate) fn next_expiry(&self) -> Option<u64> {
        for level in 0..LEVEL_COUNT {
            let mask = self.levels[level].populated_mask;
            if mask == 0 {
                continue;
            }
            let current_slot = ((self.current_tick >> (level as u64 * 6)) & 0x3F) as u32;
            let rotated = mask.rotate_right(current_slot);
            let distance = rotated.trailing_zeros();
            let slot_size = 1u64 << (level as u64 * 6);
            return Some(u64::from(distance) * slot_size + 1);
        }
        None
    }

    /// Current tick of the wheel.
    #[allow(dead_code, reason = "drop-cancel deferred to 0.2.0")]
    pub(crate) const fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Current time from the underlying clock source.
    #[inline]
    pub(crate) fn now_tick(&self) -> u64 {
        self.clock.now()
    }

    fn insert_at(&mut self, entry_nz: NonZeroU32, level: u8, slot: u8) {
        let idx = nz_to_slab(entry_nz);
        if let Some(entry) = self.entries.get_mut_by_index(idx) {
            entry.level = level;
            entry.slot = slot;
        }
        self.levels[level as usize].slots[slot as usize].push_front(entry_nz, &mut self.entries);
        self.levels[level as usize].populated_mask |= 1u64 << slot;
    }

    fn remove_from_slot(&mut self, entry_nz: NonZeroU32) {
        let idx = nz_to_slab(entry_nz);
        let Some(entry) = self.entries.get_by_index(idx) else {
            return;
        };
        let level = entry.level;
        let slot = entry.slot;

        let is_empty =
            self.levels[level as usize].slots[slot as usize].remove(entry_nz, &mut self.entries);

        if is_empty {
            self.levels[level as usize].populated_mask &= !(1u64 << slot);
        }
    }

    fn cascade_level(&mut self, level: u8) {
        let slot_idx = ((self.current_tick >> (u64::from(level) * 6)) & 0x3F) as usize;

        let mut entry_nz = self.levels[level as usize].slots[slot_idx].head;
        self.levels[level as usize].slots[slot_idx].head = None;
        self.levels[level as usize].populated_mask &= !(1u64 << slot_idx);

        while let Some(nz) = entry_nz {
            let idx = nz_to_slab(nz);
            let Some(entry) = self.entries.get_by_index(idx) else {
                break;
            };
            let next = entry.next;
            let deadline = entry.deadline_tick;

            if let Some(entry) = self.entries.get_mut_by_index(idx) {
                entry.prev = None;
                entry.next = None;
            }

            let (new_level, new_slot) = position(deadline, self.current_tick);
            self.insert_at(nz, new_level, new_slot);

            entry_nz = next;
        }
    }

    fn advance_one_tick(&mut self) {
        self.current_tick += 1;

        let tz = self.current_tick.trailing_zeros();
        if tz >= 6 {
            self.cascade_level(1);
            for lvl in 2..LEVEL_COUNT {
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "lvl bounded by LEVEL_COUNT (6)"
                )]
                let threshold = lvl as u32 * 6;
                if tz >= threshold {
                    #[allow(
                        clippy::cast_possible_truncation,
                        reason = "lvl bounded by LEVEL_COUNT (6)"
                    )]
                    self.cascade_level(lvl as u8);
                } else {
                    break;
                }
            }
        }
    }
}

/// Iterator yielding expired [`TaskRef`] values.
pub(crate) struct ExpiredIter<'a, C: Clock> {
    wheel: &'a mut TimerWheel<C>,
    target_tick: u64,
    current_drain: Option<NonZeroU32>,
}

impl<C: Clock> Iterator for ExpiredIter<'_, C> {
    type Item = TaskRef;

    fn next(&mut self) -> Option<TaskRef> {
        loop {
            if let Some(nz) = self.current_drain {
                let idx = nz_to_slab(nz);
                let Some(entry) = self.wheel.entries.get_by_index(idx) else {
                    self.current_drain = None;
                    continue;
                };
                let task_ref = entry.task_ref;
                let next = entry.next;
                self.current_drain = next;
                // IGNORE: entry is known occupied via get_by_index above.
                let _ = self.wheel.entries.remove_by_index(idx);
                return Some(task_ref);
            }

            if self.wheel.current_tick >= self.target_tick {
                return None;
            }

            if self.wheel.levels[0].populated_mask == 0 {
                let next_l0_wrap = (self.wheel.current_tick | 0x3F) + 1;
                if next_l0_wrap > self.target_tick {
                    self.wheel.current_tick = self.target_tick;
                    return None;
                }
                self.wheel.current_tick = next_l0_wrap - 1;
            }

            self.wheel.advance_one_tick();

            let slot_idx = (self.wheel.current_tick & 0x3F) as usize;
            self.current_drain = self.wheel.levels[0].slots[slot_idx].head;
            self.wheel.levels[0].slots[slot_idx].head = None;
            if self.current_drain.is_some() {
                self.wheel.levels[0].populated_mask &= !(1u64 << slot_idx);
            }
        }
    }
}

/// Compute the (level, slot) for a deadline relative to the current tick.
fn position(deadline_tick: u64, current_tick: u64) -> (u8, u8) {
    let delta = deadline_tick.saturating_sub(current_tick);

    if delta == 0 {
        return (0, (current_tick & 0x3F) as u8);
    }

    let leading_zeros = delta.leading_zeros();
    let level = ((63 - leading_zeros) / 6).min(5) as u8;
    let slot = ((deadline_tick >> (u64::from(level) * 6)) & 0x3F) as u8;

    (level, slot)
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use kwokka_core::{Generation, slab::SlabKey};

    use super::*;

    struct MockClock(AtomicU64);

    impl MockClock {
        fn new(initial: u64) -> Self {
            Self(AtomicU64::new(initial))
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn dummy_task_ref(id: u8) -> TaskRef {
        TaskRef::from_slab(id, SlabKey::new(0, Generation::ZERO))
    }

    fn wheel(capacity: usize) -> TimerWheel<MockClock> {
        TimerWheel::new(MockClock::new(0), capacity)
    }

    #[test]
    fn position_delta_zero() {
        let (level, slot) = position(100, 100);
        assert_eq!(level, 0);
        assert_eq!(slot, 36);
    }

    #[test]
    fn position_l0_range() {
        assert_eq!(position(10, 0).0, 0);
        assert_eq!(position(63, 0).0, 0);
    }

    #[test]
    fn position_l1_range() {
        assert_eq!(position(64, 0).0, 1);
    }

    #[test]
    fn register_and_expire_l0() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 5) else {
            panic!("register must succeed");
        };
        let mut count = 0;
        for task in tw.advance_to(5) {
            assert_eq!(task.worker_id(), 1);
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn cancel_before_expiry() {
        let mut tw = wheel(16);
        let Ok(handle) = tw.register(dummy_task_ref(2), 10) else {
            panic!("register must succeed");
        };
        assert!(tw.cancel(handle));
        assert_eq!(tw.advance_to(10).count(), 0);
    }

    #[test]
    fn cancel_stale_returns_false() {
        let mut tw = wheel(16);
        let Ok(handle) = tw.register(dummy_task_ref(3), 10) else {
            panic!("register must succeed");
        };
        assert!(tw.cancel(handle));
        assert!(!tw.cancel(handle));
    }

    #[test]
    fn next_expiry_empty_returns_none() {
        let tw = wheel(16);
        assert!(tw.next_expiry().is_none());
    }

    #[test]
    fn next_expiry_with_entry() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 10) else {
            panic!("register must succeed");
        };
        assert!(tw.next_expiry().is_some());
    }

    #[test]
    fn multiple_entries_same_deadline() {
        let mut tw = wheel(16);
        for i in 0..4u8 {
            let Ok(_) = tw.register(dummy_task_ref(i), 5) else {
                panic!("register must succeed");
            };
        }
        assert_eq!(tw.advance_to(5).count(), 4);
    }

    #[test]
    fn l1_cascade_and_expire() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 100) else {
            panic!("register must succeed");
        };
        assert_eq!(tw.advance_to(100).count(), 1);
    }

    #[test]
    fn large_jump_does_not_tick_by_tick() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 30_000) else {
            panic!("register must succeed");
        };
        assert_eq!(tw.advance_to(30_000).count(), 1);
    }

    #[test]
    fn populated_mask_set_on_insert() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 5) else {
            panic!("register must succeed");
        };
        assert_ne!(tw.levels[0].populated_mask, 0);
    }

    #[test]
    fn populated_mask_cleared_after_expiry() {
        let mut tw = wheel(16);
        let Ok(_) = tw.register(dummy_task_ref(1), 3) else {
            panic!("register must succeed");
        };
        tw.advance_to(3).for_each(drop);
        assert_eq!(tw.levels[0].populated_mask, 0);
    }

    #[test]
    fn populated_mask_cleared_on_cancel() {
        let mut tw = wheel(16);
        let Ok(handle) = tw.register(dummy_task_ref(1), 5) else {
            panic!("register must succeed");
        };
        assert!(tw.cancel(handle));
        assert_eq!(tw.levels[0].populated_mask, 0);
    }
}
