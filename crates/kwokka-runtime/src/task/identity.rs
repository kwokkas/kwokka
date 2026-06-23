//! 64-bit packed task handle, path-agnostic over slab and arena.

use core::fmt;

use kwokka_core::{Generation, slab::SlabKey};

const TAG_SHIFT: u32 = 63;
const TAG_ARENA_BIT: u64 = 1 << TAG_SHIFT;

const WORKER_BITS: u32 = 7;
const WORKER_SHIFT: u32 = 56;
const WORKER_MASK: u64 = (1u64 << WORKER_BITS) - 1;

const GENERATION_SHIFT: u32 = 32;
const GENERATION_MASK: u64 = Generation::MAX as u64;

const INDEX_MASK: u64 = u32::MAX as u64;

/// 64-bit task handle. `Copy`, path-agnostic over slab and arena.
///
/// Bit layout:
///
/// ```text
/// [63]     tag        - 0 = slab path, 1 = arena path
/// [62..56] worker_id  - 7 bits, routing target (max 127)
/// [55..32] generation - 24 bits, matches `SlabKey` and arena `Generation`
/// [31.. 0] index      - 32 bits, slab slot index or arena offset
/// ```
///
/// Designed to be passed as the `*const ()` data pointer of a `RawWaker`.
/// The 64-bit packing assumes a 64-bit target.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TaskRef(u64);

impl TaskRef {
    /// Maximum routable worker id (`2^7 - 1`).
    pub const WORKER_ID_MAX: u8 = (1u8 << WORKER_BITS) - 1;

    /// Constructs a slab-path reference.
    ///
    /// The caller must ensure `worker_id` does not exceed
    /// [`Self::WORKER_ID_MAX`]; values beyond 127 are masked to 7 bits.
    #[inline]
    #[must_use]
    pub const fn from_slab(worker_id: u8, key: SlabKey) -> Self {
        let bits = (((worker_id & Self::WORKER_ID_MAX) as u64) << WORKER_SHIFT) | key.to_bits();
        Self(bits)
    }

    /// Constructs an arena-path reference.
    ///
    /// The caller must ensure `worker_id` does not exceed
    /// [`Self::WORKER_ID_MAX`]; values beyond 127 are masked to 7 bits.
    #[inline]
    #[must_use]
    pub const fn from_arena(worker_id: u8, offset: u32, generation: Generation) -> Self {
        let bits = TAG_ARENA_BIT
            | (((worker_id & Self::WORKER_ID_MAX) as u64) << WORKER_SHIFT)
            | ((generation.get() as u64) << GENERATION_SHIFT)
            | offset as u64;
        Self(bits)
    }

    /// Returns `true` for arena-path references, `false` for slab-path.
    #[inline]
    #[must_use]
    pub const fn is_arena(self) -> bool {
        self.0 >> TAG_SHIFT == 1
    }

    /// Returns the routing worker id.
    #[inline]
    #[must_use]
    pub const fn worker_id(self) -> u8 {
        ((self.0 >> WORKER_SHIFT) & WORKER_MASK) as u8
    }

    /// Returns the generation captured at construction.
    #[inline]
    #[must_use]
    pub const fn generation(self) -> Generation {
        Generation::from_raw(((self.0 >> GENERATION_SHIFT) & GENERATION_MASK) as u32)
    }

    /// Returns the slot index (slab path) or arena offset (arena path).
    #[inline]
    #[must_use]
    pub const fn index(self) -> u32 {
        (self.0 & INDEX_MASK) as u32
    }

    /// Returns the raw 64-bit representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Reconstructs a [`TaskRef`] from its raw 64-bit form.
    ///
    /// The caller must ensure `raw` originated from [`TaskRef::raw`] on a
    /// still-live handle.
    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = if self.is_arena() { "arena" } else { "slab" };
        f.debug_struct("TaskRef")
            .field("path", &path)
            .field("worker_id", &self.worker_id())
            .field("generation", &self.generation().get())
            .field("index", &self.index())
            .finish()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use kwokka_core::slab::Slab;

    use super::*;

    fn slab_key(worker_capacity: usize, value: u32) -> SlabKey {
        let mut slab: Slab<u32> = Slab::new(worker_capacity);
        let Ok(key) = slab.insert(value) else {
            panic!("insert into fresh slab must succeed");
        };
        key
    }

    #[test]
    fn from_slab_round_trip_preserves_components() {
        let key = slab_key(4, 42);
        let task = TaskRef::from_slab(7, key);
        assert!(!task.is_arena());
        assert_eq!(task.worker_id(), 7);
        assert_eq!(task.index(), key.index());
        assert_eq!(task.generation(), key.generation());
    }

    #[test]
    fn from_slab_overlay_is_pure_or() {
        let key = slab_key(2, 1);
        let task = TaskRef::from_slab(3, key);
        assert_eq!(task.raw(), ((3u64) << 56) | key.to_bits());
    }

    #[test]
    fn from_arena_round_trip_at_max_generation() {
        let task = TaskRef::from_arena(5, 0xDEAD_BEEF, Generation::from_raw(Generation::MAX));
        assert!(task.is_arena());
        assert_eq!(task.worker_id(), 5);
        assert_eq!(task.index(), 0xDEAD_BEEF);
        assert_eq!(task.generation().get(), Generation::MAX);
    }

    #[test]
    fn from_arena_round_trip_at_zero_generation() {
        let task = TaskRef::from_arena(0, 0, Generation::ZERO);
        assert!(task.is_arena());
        assert_eq!(task.worker_id(), 0);
        assert_eq!(task.index(), 0);
        assert_eq!(task.generation(), Generation::ZERO);
    }

    #[test]
    fn tag_bit_distinguishes_paths() {
        let key = slab_key(1, 0);
        let slab_task = TaskRef::from_slab(0, key);
        let arena_task = TaskRef::from_arena(0, 0, Generation::ZERO);
        assert_eq!(slab_task.raw() >> 63, 0);
        assert_eq!(arena_task.raw() >> 63, 1);
    }

    #[test]
    fn worker_id_max_boundary_round_trips() {
        let task = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, Generation::ZERO);
        assert_eq!(task.worker_id(), TaskRef::WORKER_ID_MAX);
        assert_eq!(TaskRef::WORKER_ID_MAX, 127);
    }

    #[test]
    fn worker_id_overflow_is_masked() {
        let task = TaskRef::from_arena(255, 0, Generation::ZERO);
        assert_eq!(task.worker_id(), 127);
    }

    #[test]
    fn raw_from_raw_is_identity() {
        let original = TaskRef::from_arena(42, u32::MAX, Generation::from_raw(Generation::MAX));
        let restored = TaskRef::from_raw(original.raw());
        assert_eq!(original, restored);
    }

    #[test]
    fn repr_size_is_u64() {
        assert_eq!(core::mem::size_of::<TaskRef>(), core::mem::size_of::<u64>());
    }

    #[test]
    fn worker_id_field_does_not_leak_tag_bit() {
        let task = TaskRef::from_arena(TaskRef::WORKER_ID_MAX, 0, Generation::ZERO);
        assert_eq!(task.worker_id(), TaskRef::WORKER_ID_MAX);
        assert!(task.is_arena());
    }
}
