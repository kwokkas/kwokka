//! The stale-handle routing table: where a relocated task went.
//!
//! A [`TaskRef`] minted before a steal names a slot the task has left. The
//! victim records the route here in the same straight-line step that retires
//! the source, so a holder of a stale handle can re-route by slot index rather
//! than lose the wake. The move itself -- claiming, retiring, and copying the
//! cell -- lives in [`relocate`](crate::scheduler::stealing::relocate); this
//! table only remembers where the bytes went.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::{Generation, slab::SlabKey};

use crate::task::TaskRef;

/// One slot's forwarding record: the generation that was retired and the
/// task's new location.
#[derive(Clone, Copy)]
struct ForwardEntry {
    retired_generation: Generation,
    new_ref: TaskRef,
}

/// Forwarding table mapping a relocated task's old slot to its new
/// location, one entry per victim slab slot.
///
/// A stale [`TaskRef`] minted before a move resolves to the `Retired` husk
/// (or, once the husk is released, to nothing); the holder re-routes
/// through this table by slot index, gated on the generation the entry
/// retired. The victim records the route in the same straight-line serve
/// step that retires the source, so a husk observable through its live
/// generation always has its route recorded -- no half-entry window.
///
/// One entry per slot is the capacity guarantee: no eviction, no lost
/// route. Re-relocating a reused slot index overwrites the previous
/// record, which only keys whose generation already rolled could want -- a
/// collision degrades to a dropped wake, never a misroute. Precise
/// per-entry reclamation is deferred to the 0.2.0 track.
pub(crate) struct ForwardTable {
    entries: Vec<Option<ForwardEntry>>,
}

impl ForwardTable {
    /// Empty table sized to the victim slab's capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(None);
        }
        Self { entries }
    }

    /// Records that the task retired under `victim_key` now lives at `to`.
    ///
    /// # Panics
    ///
    /// Panics if `victim_key` indexes outside the table -- the victim
    /// records only keys retired from its own equally-sized slab.
    pub(crate) fn record(&mut self, victim_key: SlabKey, to: TaskRef) {
        let Some(entry) = self.entries.get_mut(victim_key.index() as usize) else {
            panic!("a forward record must name a slot inside the victim slab");
        };
        *entry = Some(ForwardEntry {
            retired_generation: victim_key.generation(),
            new_ref: to,
        });
    }

    /// Resolves a stale key to the location recorded for its generation.
    ///
    /// Returns `None` when the slot has no record or the record belongs to
    /// a different generation of the slot -- a collision after slot reuse
    /// drops the consumer's wake rather than misrouting it.
    pub(crate) fn lookup(&self, stale_key: SlabKey) -> Option<TaskRef> {
        let entry = (*self.entries.get(stale_key.index() as usize)?)?;
        (entry.retired_generation == stale_key.generation()).then_some(entry.new_ref)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    fn task_ref(worker: u8, index: u32) -> TaskRef {
        TaskRef::from_slab(worker, SlabKey::new(index, Generation::from_raw(1)))
    }

    #[test]
    fn a_recorded_route_resolves_for_the_retired_generation() {
        let mut table = ForwardTable::new(4);
        let retired = SlabKey::new(2, Generation::from_raw(1));
        table.record(retired, task_ref(1, 10));
        assert_eq!(table.lookup(retired), Some(task_ref(1, 10)));
        assert_eq!(
            table.lookup(SlabKey::new(2, Generation::from_raw(3))),
            None,
            "a later generation of the slot has no claim on the route",
        );
        assert_eq!(table.lookup(SlabKey::new(1, Generation::from_raw(1))), None);
    }

    #[test]
    fn a_generation_collision_drops_instead_of_misrouting() {
        let mut table = ForwardTable::new(2);
        let first = SlabKey::new(0, Generation::from_raw(1));
        let reused = SlabKey::new(0, Generation::from_raw(3));
        table.record(first, task_ref(1, 10));
        table.record(reused, task_ref(2, 20));
        assert_eq!(
            table.lookup(first),
            None,
            "the overwritten generation drops; it must never see the reused slot's route",
        );
        assert_eq!(table.lookup(reused), Some(task_ref(2, 20)));
    }

    #[test]
    fn an_out_of_range_lookup_resolves_to_nothing() {
        let table = ForwardTable::new(1);
        assert_eq!(table.lookup(SlabKey::new(9, Generation::from_raw(1))), None);
    }

    #[test]
    #[should_panic(expected = "a forward record must name a slot inside the victim slab")]
    fn a_record_outside_the_table_panics() {
        let mut table = ForwardTable::new(1);
        table.record(SlabKey::new(1, Generation::from_raw(1)), task_ref(1, 10));
    }
}
