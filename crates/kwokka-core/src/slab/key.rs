//! 64-bit packed slot reference for the generational slab.

use core::fmt;

use crate::generation::Generation;

const INDEX_BITS: u32 = 32;
const INDEX_MASK: u64 = (1u64 << INDEX_BITS) - 1;
const GENERATION_MASK: u64 = Generation::MAX as u64;
const GENERATION_SHIFT: u32 = INDEX_BITS;

/// 64-bit handle returned by `Slab::insert`.
///
/// Bit layout:
///
/// ```text
/// [63..56] reserved   - 8 bits, zero (identity tag/routing overlay)
/// [55..32] generation - 24 bits, insert-time generation
/// [31.. 0] index      - 32 bits, slot index
/// ```
///
/// `Copy`. The reserved high bits are guaranteed zero so a consumer's
/// identity handle may overlay routing metadata on them.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SlabKey(u64);

impl SlabKey {
    /// Packs an index and generation into the 64-bit key.
    #[inline]
    pub const fn new(index: u32, generation: Generation) -> Self {
        let bits = ((generation.get() as u64) << GENERATION_SHIFT) | index as u64;
        Self(bits)
    }

    /// Returns the slot index.
    #[inline]
    pub const fn index(self) -> u32 {
        (self.0 & INDEX_MASK) as u32
    }

    /// Returns the generation captured at insert time.
    #[inline]
    pub const fn generation(self) -> Generation {
        Generation(((self.0 >> GENERATION_SHIFT) & GENERATION_MASK) as u32)
    }

    /// Returns the raw 64-bit representation.
    #[inline]
    pub const fn to_bits(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for SlabKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlabKey")
            .field("index", &self.index())
            .field("generation", &self.generation().get())
            .finish()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn layout_index_generation_split() {
        let key = SlabKey::new(0xDEAD_BEEF, Generation(0x123_456));
        assert_eq!(key.index(), 0xDEAD_BEEF);
        assert_eq!(key.generation().get(), 0x123_456);
    }

    #[test]
    fn reserved_bits_are_zero() {
        let key = SlabKey::new(u32::MAX, Generation(Generation::MAX));
        assert_eq!(key.to_bits() >> 56, 0);
    }

    #[test]
    fn repr_size_is_u64() {
        assert_eq!(core::mem::size_of::<SlabKey>(), core::mem::size_of::<u64>());
    }
}
