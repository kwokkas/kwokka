//! Where a stolen task came from: the per-slot origin store a thief keeps.
//!
//! A thief that installs a relocated task records the victim it came from, so
//! it can later report the task settled and let the victim release the husk.
//! The store is indexed by the thief's own slot, one entry per slot, and it is
//! read by [`victim`](crate::scheduler::stealing::victim) to refuse a
//! second hop and written by [`thief`](crate::scheduler::stealing::thief).

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::slab::SlabKey;

/// Where a relocated resident came from: the victim worker and the husk
/// slot awaiting this worker's settled note.
#[derive(Clone, Copy)]
pub(crate) struct Origin {
    /// Worker whose slab holds the `Retired` husk.
    pub(crate) victim_id: u8,
    /// The husk slot in the victim's slab.
    pub(crate) victim_key: SlabKey,
}

/// Origin records for tasks relocated into this worker's slab, keyed by
/// destination slot index -- the thief-side mirror of the victim's
/// [`ForwardTable`].
///
/// An entry is recorded at install time and taken when the settled note
/// is pushed, so a slot index never carries a stale origin into its next
/// resident: the take precedes the slot's release on every settle path.
pub(crate) struct ForwardOrigin {
    entries: Vec<Option<Origin>>,
}

impl ForwardOrigin {
    /// Empty table sized to the owning slab's capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(None);
        }
        Self { entries }
    }

    /// Slots the table covers, matching the owning slab's capacity.
    ///
    /// A table too large to index with a `u32` reports zero, which walks no
    /// slots -- a slab that big cannot be addressed by a `SlabKey` anyway.
    pub(crate) fn capacity(&self) -> u32 {
        u32::try_from(self.entries.len()).unwrap_or(0)
    }

    /// The origin recorded for `index`, leaving it in place.
    ///
    /// The read half of [`take`](ForwardOrigin::take): a settle report reads
    /// the origin, decides whether the note landed, and only then takes it.
    pub(crate) fn peek(&self, index: u32) -> Option<Origin> {
        *self.entries.get(index as usize)?
    }

    /// Records that the resident installed at `index` came from `origin`.
    ///
    /// # Panics
    ///
    /// Panics if `index` lies outside the table -- the thief records only
    /// keys reserved from its own equally-sized slab.
    pub(crate) fn record(&mut self, index: u32, origin: Origin) {
        let Some(entry) = self.entries.get_mut(index as usize) else {
            panic!("an origin record must name a slot inside the thief slab");
        };
        *entry = Some(origin);
    }

    /// Takes the origin recorded for `index`, leaving the slot bare.
    pub(crate) fn take(&mut self, index: u32) -> Option<Origin> {
        self.entries.get_mut(index as usize)?.take()
    }

    /// Whether `index` currently hosts a relocated resident.
    pub(crate) fn is_relocated(&self, index: u32) -> bool {
        self.entries
            .get(index as usize)
            .is_some_and(Option::is_some)
    }
}
