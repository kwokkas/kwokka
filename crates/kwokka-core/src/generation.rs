//! 24-bit wrapping counter shared by slab and arena.
//!
//! Both data structures use a generational index to detect stale handles
//! after a slot or arena is recycled. The two consumers interpret the
//! counter slightly differently:
//!
//! - **slab** treats parity as occupancy (odd = occupied, even = empty) and bumps the generation on
//!   both insert and remove. See [`Generation::is_occupied`].
//! - **arena** uses the counter as a plain version tag, bumped once per `reset()`. Parity is
//!   ignored.

use core::fmt;

const GENERATION_BITS: u32 = 24;

/// 24-bit wrapping generation counter.
///
/// Wrapping at `2^24` is acceptable: a slot or arena must be recycled
/// `2^24` times before a stale handle could collide.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct Generation(pub(crate) u32);

impl Generation {
    /// Empty initial state - even, never inserted into.
    pub const ZERO: Self = Self(0);

    /// Maximum 24-bit value (`2^24 - 1`).
    pub const MAX: u32 = (1 << GENERATION_BITS) - 1;

    /// Returns the raw value masked to 24 bits.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0 & Self::MAX
    }

    /// Returns `true` when the generation parity marks a slot as occupied.
    ///
    /// Slab semantics only - arena ignores parity.
    #[inline]
    pub const fn is_occupied(self) -> bool {
        self.0 & 1 == 1
    }

    /// Increments the generation, wrapping at `2^24`.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1) & Self::MAX)
    }
}

impl fmt::Debug for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Generation({})", self.get())
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn zero_is_even_and_empty() {
        assert_eq!(Generation::ZERO.get(), 0);
        assert!(!Generation::ZERO.is_occupied());
    }

    #[test]
    fn next_alternates_parity() {
        let g0 = Generation::ZERO;
        let g1 = g0.next();
        let g2 = g1.next();
        assert!(g1.is_occupied());
        assert!(!g2.is_occupied());
        assert_eq!(g1.get(), 1);
        assert_eq!(g2.get(), 2);
    }

    #[test]
    fn wraps_at_24_bit_max() {
        let max = Generation(Generation::MAX);
        assert_eq!(max.get(), Generation::MAX);
        assert!(max.is_occupied());
        assert_eq!(max.next().get(), 0);
    }

    #[test]
    fn pre_max_boundary() {
        let pre_max = Generation(Generation::MAX - 1);
        assert_eq!(pre_max.next().get(), Generation::MAX);
    }

    #[test]
    fn repr_size_is_u32() {
        assert_eq!(
            core::mem::size_of::<Generation>(),
            core::mem::size_of::<u32>()
        );
    }
}
