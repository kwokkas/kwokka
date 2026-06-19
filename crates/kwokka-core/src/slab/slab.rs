//! Generational slab allocator and error type.

use core::{fmt, mem::MaybeUninit, ptr::NonNull};

use crate::generation::Generation;
use crate::slab::key::SlabKey;

const FREE_SENTINEL: u32 = u32::MAX;

/// Errors emitted by [`Slab`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlabError {
    /// All slots are occupied; `insert` cannot proceed.
    Full,
}

impl fmt::Display for SlabError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("slab is full"),
        }
    }
}

impl core::error::Error for SlabError {}

struct Slot<T> {
    generation: Generation,
    next_free: u32,
    data: MaybeUninit<T>,
}

impl<T> Slot<T> {
    #[inline]
    const fn data_ref(&self) -> &T {
        // SAFETY: callers (`get`, `iter`) verify generation match or
        // occupancy parity before reaching this point. Occupied generation
        // implies `data` was written by `Slab::insert` and not drained by
        // `Slab::remove`. Violation would read uninitialized memory.
        unsafe { self.data.assume_init_ref() }
    }
}

/// Generational slab with fixed capacity.
///
/// O(1) insert, get, and remove. Generational indices detect stale
/// handles when a slot is recycled.
///
/// Capacity is fixed at construction; `insert` returns
/// [`SlabError::Full`] when all slots are occupied. The backing storage
/// never reallocates.
///
/// The free list is in-band: empty slots store the next free index in
/// the slot itself, terminated by `u32::MAX`.
///
/// # Concurrency
///
/// `insert`, `remove`, and `get_mut` take `&mut self`, so only the
/// single owner can mutate. The `&mut self` discipline makes shared
/// mutation a compile-time error rather than a runtime check.
pub struct Slab<T> {
    slots: Vec<Slot<T>>,
    free_head: u32,
    len: usize,
}

impl<T> Slab<T> {
    /// Creates a new slab with fixed `capacity`.
    ///
    /// All slots start empty and linked into the free list in index order.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds `u32::MAX as usize`.
    pub fn new(capacity: usize) -> Self {
        let Ok(capacity_u32) = u32::try_from(capacity) else {
            panic!("capacity {capacity} exceeds u32::MAX");
        };
        let mut slots = Vec::with_capacity(capacity);
        for idx in 0..capacity_u32 {
            let next_free = if idx + 1 < capacity_u32 {
                idx + 1
            } else {
                FREE_SENTINEL
            };
            slots.push(Slot {
                generation: Generation::ZERO,
                next_free,
                data: MaybeUninit::uninit(),
            });
        }
        let free_head = if capacity_u32 == 0 { FREE_SENTINEL } else { 0 };
        Self {
            slots,
            free_head,
            len: 0,
        }
    }

    /// Inserts `value`, returning a [`SlabKey`] that locates it.
    ///
    /// # Errors
    ///
    /// Returns [`SlabError::Full`] when all slots are currently occupied.
    ///
    /// # Panics
    ///
    /// Panics if the internal free list is corrupt (points to an occupied
    /// slot). This indicates a bug in `Slab` itself and cannot be triggered
    /// by correct use of the public API.
    pub fn insert(&mut self, value: T) -> Result<SlabKey, SlabError> {
        let idx = self.free_head;
        if idx == FREE_SENTINEL {
            return Err(SlabError::Full);
        }
        let slot = &mut self.slots[idx as usize];
        if slot.generation.is_occupied() {
            unreachable!("free list points to occupied slot {idx}");
        }
        let next = slot.next_free;
        slot.next_free = FREE_SENTINEL;
        slot.generation = slot.generation.next();
        slot.data.write(value);
        let generation = slot.generation;
        self.free_head = next;
        self.len += 1;
        Ok(SlabKey::new(idx, generation))
    }

    /// Returns a shared reference to the value at `key`, if present.
    pub fn get(&self, key: SlabKey) -> Option<&T> {
        let slot = self.slots.get(key.index() as usize)?;
        if slot.generation != key.generation() {
            return None;
        }
        Some(slot.data_ref())
    }

    /// Returns an exclusive reference to the value at `key`, if present.
    pub fn get_mut(&mut self, key: SlabKey) -> Option<&mut T> {
        let slot = self.slots.get_mut(key.index() as usize)?;
        if slot.generation != key.generation() {
            return None;
        }
        // SAFETY: matching generation implies `data` is initialized. The
        // exclusive borrow is bounded by `&mut self`, so no aliasing
        // reference can coexist. Violation would read uninitialized memory.
        Some(unsafe { slot.data.assume_init_mut() })
    }

    /// Returns a raw, generation-checked pointer to the value at `key`.
    ///
    /// The pointer carries provenance over the whole `T`, so a slot-local
    /// reader (a consumer that steps past a header into the cell) retags at
    /// the use site. Two distinct keys resolve to distinct indices,
    /// hence distinct elements of the contiguous backing array, so pointers
    /// from distinct keys never alias -- the structural guarantee that lets a
    /// child-slot read coexist with a live borrow of a different slot.
    ///
    /// The pointer is read-oriented (formed from a shared borrow). A writer
    /// must establish exclusive provenance by other means. Returns `None`
    /// when the slot is empty or the generation no longer matches. Before
    /// dereferencing, the caller must ensure (1) the slot stays occupied (no
    /// `remove` / `retire_slot` / `remove_by_index` on the same key while the
    /// pointer is live) and (2) the pointer does not outlive this slab.
    pub fn slot_ptr(&self, key: SlabKey) -> Option<NonNull<T>> {
        let slot = self.slots.get(key.index() as usize)?;
        if slot.generation != key.generation() {
            return None;
        }
        Some(NonNull::from(&slot.data).cast::<T>())
    }

    /// Returns a raw, generation-checked pointer to `key` for an exclusive `&mut T`.
    ///
    /// Carries the caller's intent to form an exclusive `&mut T` at the use
    /// site. The exclusive (`&mut self`) twin of [`slot_ptr`](Slab::slot_ptr): the
    /// returned pointer carries provenance over the whole `T`, and the worker
    /// reconstitutes the `&mut T` at the narrowest scope. Two distinct keys
    /// resolve to distinct indices, hence distinct elements of the contiguous
    /// backing array, so a `&mut T` formed at one key never aliases a pointer
    /// formed at another -- the same `split_at_mut`-spirit structural guarantee
    /// that lets the worker poll one slot exclusively while a disjoint slot is
    /// read through [`slot_ptr`](Slab::slot_ptr). Returns `None` when the slot
    /// is empty or the generation no longer matches. The use-site validity
    /// contract (no `&mut self` reborrow while a derived reference is live, no
    /// outliving a slot reclaim) lives on the caller.
    pub fn slot_ptr_mut(&mut self, key: SlabKey) -> Option<NonNull<T>> {
        let slot = self.slots.get_mut(key.index() as usize)?;
        if slot.generation != key.generation() {
            return None;
        }
        Some(NonNull::from(&mut slot.data).cast::<T>())
    }

    /// Removes and returns the value at `key`, if present.
    pub fn remove(&mut self, key: SlabKey) -> Option<T> {
        let head = self.free_head;
        let idx = key.index();
        let slot = self.slots.get_mut(idx as usize)?;
        if slot.generation != key.generation() {
            return None;
        }
        // SAFETY: matching generation implies `data` is initialized.
        // `assume_init_read` transfers ownership out; advancing the
        // generation immediately after marks the slot empty so future
        // reads through `data` are forbidden. Violation would
        // double-free or read uninitialized memory.
        let value = unsafe { slot.data.assume_init_read() };
        slot.generation = slot.generation.next();
        slot.next_free = head;
        self.free_head = idx;
        self.len -= 1;
        Some(value)
    }

    /// Releases the slot at `key` without dropping or returning its value.
    ///
    /// The relocation counterpart of [`remove`](Slab::remove): the caller has
    /// already moved the value's bytes out of the slot, so only the slot
    /// bookkeeping remains -- the generation rolls to empty parity and the
    /// slot rejoins the free list. No drop glue runs here; ownership of the
    /// moved-out bytes lives with the caller. Calling this on a slot whose
    /// value was not moved out leaks that value (safe, but a caller bug).
    ///
    /// Returns `false` when the key is stale or the slot is already empty.
    pub fn retire_slot(&mut self, key: SlabKey) -> bool {
        let head = self.free_head;
        let idx = key.index();
        let Some(slot) = self.slots.get_mut(idx as usize) else {
            return false;
        };
        if slot.generation != key.generation() {
            return false;
        }
        slot.generation = slot.generation.next();
        slot.next_free = head;
        self.free_head = idx;
        self.len -= 1;
        true
    }

    /// Takes an empty slot off the free list for a value that arrives
    /// later, returning the key that will name it once installed.
    ///
    /// The slot keeps its empty-parity generation until
    /// [`install`](Slab::install) commits the value, so the returned key
    /// resolves to nothing in the meantime -- a consumer holding the key
    /// too early reads nothing by construction. While reserved, the slot
    /// marks itself by pointing `next_free` at its own index, a shape the
    /// free chain never produces, so no extra sentinel is needed.
    /// [`unreserve`](Slab::unreserve) withdraws the promise.
    ///
    /// # Errors
    ///
    /// Returns [`SlabError::Full`] when every slot is occupied or reserved.
    ///
    /// # Panics
    ///
    /// Panics if the internal free list is corrupt (points to an occupied
    /// slot). This indicates a bug in `Slab` itself and cannot be triggered
    /// by correct use of the public API.
    pub fn reserve(&mut self) -> Result<SlabKey, SlabError> {
        let idx = self.free_head;
        if idx == FREE_SENTINEL {
            return Err(SlabError::Full);
        }
        let slot = &mut self.slots[idx as usize];
        if slot.generation.is_occupied() {
            unreachable!("free list points to occupied slot {idx}");
        }
        self.free_head = slot.next_free;
        slot.next_free = idx;
        Ok(SlabKey::new(idx, slot.generation.next()))
    }

    /// Installs `value` into the slot reserved under `key`, making the
    /// promised key live.
    ///
    /// The committing half of [`reserve`](Slab::reserve): the value's bytes
    /// land in the reserved slot and the generation rolls to the promised
    /// occupied parity, so the key handed out at reservation time starts
    /// resolving exactly here.
    ///
    /// # Errors
    ///
    /// Returns `value` back when `key` does not name the promised key of a
    /// currently reserved slot.
    pub fn install(&mut self, key: SlabKey, value: T) -> Result<(), T> {
        let idx = key.index();
        let Some(slot) = self.slots.get_mut(idx as usize) else {
            return Err(value);
        };
        if slot.next_free != idx || slot.generation.next() != key.generation() {
            return Err(value);
        }
        slot.next_free = FREE_SENTINEL;
        slot.generation = slot.generation.next();
        slot.data.write(value);
        self.len += 1;
        Ok(())
    }

    /// Returns a reserved slot to the free list, withdrawing the promise.
    ///
    /// The generation never moved, so a later [`insert`](Slab::insert) or
    /// [`reserve`](Slab::reserve) re-promises the same key -- harmless on
    /// the owning thread, where reservations are sequenced.
    ///
    /// Returns `false` when `key` does not name the promised key of a
    /// currently reserved slot.
    pub fn unreserve(&mut self, key: SlabKey) -> bool {
        let idx = key.index();
        let Some(slot) = self.slots.get_mut(idx as usize) else {
            return false;
        };
        if slot.next_free != idx || slot.generation.next() != key.generation() {
            return false;
        }
        slot.next_free = self.free_head;
        self.free_head = idx;
        true
    }

    /// Returns the number of occupied slots.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the fixed capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Returns `true` when no slots are occupied.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a shared reference by raw slot index, if occupied.
    ///
    /// Skips generation matching -- checks occupancy parity only.
    /// Use when the caller's index invariant guarantees the slot is live
    /// and a `SlabKey` generation is not available at the call site.
    pub fn get_by_index(&self, idx: u32) -> Option<&T> {
        let slot = self.slots.get(idx as usize)?;
        if !slot.generation.is_occupied() {
            return None;
        }
        Some(slot.data_ref())
    }

    /// Returns an exclusive reference by raw slot index, if occupied.
    ///
    /// Same occupancy-only check as [`get_by_index`](Slab::get_by_index).
    pub fn get_mut_by_index(&mut self, idx: u32) -> Option<&mut T> {
        let slot = self.slots.get_mut(idx as usize)?;
        if !slot.generation.is_occupied() {
            return None;
        }
        // SAFETY: occupied parity implies data is initialized. The
        // exclusive borrow is bounded by &mut self. Violation would
        // read uninitialized memory.
        Some(unsafe { slot.data.assume_init_mut() })
    }

    /// Removes and returns the value by raw slot index, if occupied.
    ///
    /// Same occupancy-only check as [`get_by_index`](Slab::get_by_index).
    pub fn remove_by_index(&mut self, idx: u32) -> Option<T> {
        let head = self.free_head;
        let slot = self.slots.get_mut(idx as usize)?;
        if !slot.generation.is_occupied() {
            return None;
        }
        // SAFETY: occupied parity implies data is initialized.
        // assume_init_read transfers ownership out. Advancing the
        // generation marks the slot empty. Violation would
        // double-free or read uninitialized memory.
        let value = unsafe { slot.data.assume_init_read() };
        slot.generation = slot.generation.next();
        slot.next_free = head;
        self.free_head = idx;
        self.len -= 1;
        Some(value)
    }

    /// Returns an iterator over occupied slots as `(SlabKey, &T)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (SlabKey, &T)> + '_ {
        self.slots.iter().enumerate().filter_map(|(idx, slot)| {
            if !slot.generation.is_occupied() {
                return None;
            }
            let value_ref = slot.data_ref();
            #[allow(
                clippy::cast_possible_truncation,
                reason = "slots.len() is bounded by u32::MAX in Slab::new"
            )]
            let key = SlabKey::new(idx as u32, slot.generation);
            Some((key, value_ref))
        })
    }
}

impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if slot.generation.is_occupied() {
                // SAFETY: occupied parity implies `data` is initialized.
                // Dropping in place releases owned resources before the
                // backing Vec frees the slot storage. Violation would
                // drop uninitialized memory.
                unsafe { slot.data.assume_init_drop() };
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn insert_or_panic<T>(slab: &mut Slab<T>, value: T) -> SlabKey {
        match slab.insert(value) {
            Ok(key) => key,
            Err(SlabError::Full) => panic!("insert must succeed: slab unexpectedly full"),
        }
    }

    #[test]
    fn insert_get_roundtrip() {
        let mut slab: Slab<u32> = Slab::new(2);
        let key = insert_or_panic(&mut slab, 7);
        assert_eq!(slab.get(key), Some(&7));
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn remove_returns_value_and_reuses_slot() {
        let mut slab: Slab<u32> = Slab::new(2);
        let key = insert_or_panic(&mut slab, 11);
        assert_eq!(slab.remove(key), Some(11));
        assert!(slab.is_empty());
        let key2 = insert_or_panic(&mut slab, 22);
        assert_eq!(key2.index(), key.index());
        assert_ne!(key2.generation(), key.generation());
    }

    #[test]
    fn aba_stale_key_returns_none() {
        let mut slab: Slab<u32> = Slab::new(1);
        let key_a = insert_or_panic(&mut slab, 100);
        slab.remove(key_a);
        let key_b = insert_or_panic(&mut slab, 200);
        assert_eq!(key_a.index(), key_b.index());
        assert_eq!(slab.get(key_a), None);
        assert_eq!(slab.get(key_b), Some(&200));
    }

    #[test]
    fn full_capacity_returns_full_error() {
        let mut slab: Slab<u32> = Slab::new(2);
        insert_or_panic(&mut slab, 1);
        insert_or_panic(&mut slab, 2);
        assert_eq!(slab.insert(3), Err(SlabError::Full));
    }

    #[test]
    fn zero_capacity_immediately_full() {
        let mut slab: Slab<u32> = Slab::new(0);
        assert_eq!(slab.insert(1), Err(SlabError::Full));
        assert_eq!(slab.capacity(), 0);
    }

    #[test]
    fn get_mut_modifies_in_place() {
        let mut slab: Slab<u32> = Slab::new(1);
        let key = insert_or_panic(&mut slab, 0);
        let Some(slot) = slab.get_mut(key) else {
            panic!("just-inserted key must resolve");
        };
        *slot = 99;
        assert_eq!(slab.get(key), Some(&99));
    }

    #[test]
    fn iter_yields_only_occupied_in_index_order() {
        let mut slab: Slab<u32> = Slab::new(4);
        let k0 = insert_or_panic(&mut slab, 10);
        insert_or_panic(&mut slab, 20);
        insert_or_panic(&mut slab, 30);
        slab.remove(k0);
        let mut values = [0u32; 2];
        let mut count = 0;
        for (_, value) in slab.iter() {
            values[count] = *value;
            count += 1;
        }
        assert_eq!(count, 2);
        assert_eq!(values, [20, 30]);
    }

    #[test]
    fn iter_empty_after_full_drain() {
        let mut slab: Slab<u32> = Slab::new(2);
        let key = insert_or_panic(&mut slab, 1);
        slab.remove(key);
        assert!(slab.iter().next().is_none());
    }

    #[test]
    fn drop_runs_on_remaining_occupied_slots() {
        struct Bomb<'a>(&'a AtomicUsize);
        impl Drop for Bomb<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = AtomicUsize::new(0);
        {
            let mut slab: Slab<Bomb<'_>> = Slab::new(3);
            insert_or_panic(&mut slab, Bomb(&counter));
            let key = insert_or_panic(&mut slab, Bomb(&counter));
            insert_or_panic(&mut slab, Bomb(&counter));
            slab.remove(key);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn len_tracks_inserts_and_removals() {
        let mut slab: Slab<u32> = Slab::new(3);
        assert_eq!(slab.len(), 0);
        let k1 = insert_or_panic(&mut slab, 1);
        insert_or_panic(&mut slab, 2);
        assert_eq!(slab.len(), 2);
        slab.remove(k1);
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn out_of_range_index_returns_none() {
        let mut slab: Slab<u32> = Slab::new(2);
        insert_or_panic(&mut slab, 1);
        let stale = SlabKey::new(99, Generation(1));
        assert_eq!(slab.get(stale), None);
    }

    #[test]
    fn slaberror_display_message() {
        assert_eq!(SlabError::Full.to_string(), "slab is full");
    }

    #[test]
    fn retire_slot_rolls_generation_and_reuses_slot() {
        let mut slab: Slab<u32> = Slab::new(1);
        let key = insert_or_panic(&mut slab, 5);
        assert!(slab.retire_slot(key));
        assert_eq!(slab.get(key), None);
        assert!(slab.is_empty());
        let key2 = insert_or_panic(&mut slab, 6);
        assert_eq!(key2.index(), key.index());
        assert_ne!(key2.generation(), key.generation());
    }

    #[test]
    fn retire_slot_does_not_drop_value() {
        struct Bomb<'a>(&'a AtomicUsize);
        impl Drop for Bomb<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = AtomicUsize::new(0);
        {
            let mut slab: Slab<Bomb<'_>> = Slab::new(1);
            let key = insert_or_panic(&mut slab, Bomb(&counter));
            assert!(slab.retire_slot(key));
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "a retired slot must not run drop glue; the caller owns the moved-out value",
        );
    }

    #[test]
    fn retire_slot_stale_or_empty_returns_false() {
        let mut slab: Slab<u32> = Slab::new(1);
        let key = insert_or_panic(&mut slab, 9);
        assert!(slab.retire_slot(key));
        assert!(
            !slab.retire_slot(key),
            "a second retire of the same key must miss the rolled generation",
        );
        let out_of_range = SlabKey::new(99, Generation(1));
        assert!(!slab.retire_slot(out_of_range));
    }

    #[test]
    fn a_reserved_key_resolves_to_nothing_until_install() {
        let mut slab: Slab<u32> = Slab::new(2);
        let Ok(promised) = slab.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        assert_eq!(slab.get(promised), None);
        assert!(slab.get_mut(promised).is_none());
        assert_eq!(slab.remove(promised), None);
        assert!(!slab.retire_slot(promised));
        assert_eq!(slab.len(), 0);
        let Ok(()) = slab.install(promised, 7) else {
            panic!("install under the promised key must succeed");
        };
        assert_eq!(slab.get(promised), Some(&7));
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn unreserve_returns_the_slot_to_the_free_list() {
        let mut slab: Slab<u32> = Slab::new(1);
        let Ok(promised) = slab.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        assert!(slab.reserve().is_err(), "a reserved slot is off the list");
        assert!(slab.unreserve(promised));
        let key = insert_or_panic(&mut slab, 9);
        assert_eq!(key.index(), promised.index());
        assert_eq!(
            key.generation(),
            promised.generation(),
            "the unmoved generation re-promises the same key",
        );
        assert_eq!(slab.get(key), Some(&9));
    }

    #[test]
    fn install_rejects_a_key_that_was_never_reserved() {
        let mut slab: Slab<u32> = Slab::new(2);
        let occupied = insert_or_panic(&mut slab, 1);
        assert_eq!(slab.install(occupied, 5), Err(5));
        let forged = SlabKey::new(1, Generation::ZERO.next());
        assert_eq!(slab.install(forged, 6), Err(6));
        assert!(!slab.unreserve(forged));
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn a_full_slab_rejects_a_reservation() {
        let mut slab: Slab<u32> = Slab::new(1);
        let Ok(_promised) = slab.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        assert!(slab.reserve().is_err());
        assert!(
            slab.insert(3).is_err(),
            "a reserved slot is unreachable by insert",
        );
    }

    #[test]
    fn a_dropped_slab_skips_reserved_slots() {
        struct Bomb<'a>(&'a AtomicUsize);
        impl Drop for Bomb<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = AtomicUsize::new(0);
        {
            let mut slab: Slab<Bomb<'_>> = Slab::new(2);
            insert_or_panic(&mut slab, Bomb(&counter));
            let Ok(_promised) = slab.reserve() else {
                panic!("reserve beside an occupied slot must succeed");
            };
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "a reserved slot holds no value, so slab drop must skip it",
        );
    }

    #[test]
    fn install_then_remove_rolls_the_generation_forward() {
        let mut slab: Slab<u32> = Slab::new(1);
        let Ok(first) = slab.reserve() else {
            panic!("reserve on a fresh slab must succeed");
        };
        let Ok(()) = slab.install(first, 4) else {
            panic!("install under the promised key must succeed");
        };
        assert_eq!(slab.remove(first), Some(4));
        let Ok(second) = slab.reserve() else {
            panic!("a removed slot must be reservable again");
        };
        assert_eq!(second.index(), first.index());
        assert_eq!(
            second.generation(),
            first.generation().next().next(),
            "remove rolls the slot two promises past the first",
        );
        assert_eq!(slab.install(first, 8), Err(8), "a stale promise is dead");
    }
}
