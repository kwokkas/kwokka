//! [`Pip`] core type and accessors.

use crate::id::layout::{DEPTH_MASK, DEPTH_SHIFT, SEQ_MASK, SEQ_SHIFT, WORKER_MASK, WORKER_SHIFT};

/// A generational, bit-packed 128-bit task identifier.
///
/// `Pip` is the observability primitive for tasks, conductor stages, chain
/// steps, and other first-class entities. It is `Copy`, zero-allocation, and
/// carries no heap state.
///
/// Distinct from `TaskRef`, which is a separate 64-bit slab handle for
/// runtime task lookup (defined in `kwokka-runtime`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Pip(pub(crate) u128);

impl Pip {
    /// Returns the raw `u128` representation.
    #[inline]
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// Returns the parent-child nesting depth (0 for root).
    #[inline]
    pub const fn depth(self) -> u16 {
        ((self.0 & DEPTH_MASK) >> DEPTH_SHIFT) as u16
    }

    /// Returns the monotonic sequence number assigned at creation.
    #[inline]
    pub const fn seq(self) -> u64 {
        ((self.0 & SEQ_MASK) >> SEQ_SHIFT) as u64
    }

    /// Returns the originating worker id encoded in this id.
    #[inline]
    pub const fn worker_id(self) -> u64 {
        ((self.0 & WORKER_MASK) >> WORKER_SHIFT) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repr_transparent_size_align() {
        assert_eq!(core::mem::size_of::<Pip>(), core::mem::size_of::<u128>());
        assert_eq!(core::mem::align_of::<Pip>(), core::mem::align_of::<u128>());
    }

    #[test]
    fn as_u128_roundtrip() {
        let raw = (0xdead_beef_1234_5678u128 << 64) | 0xabcd_ef01_2345_6789u128;
        assert_eq!(Pip(raw).as_u128(), raw);
    }

    #[test]
    fn zero_id_accessors_are_zero() {
        let id = Pip(0);
        assert_eq!(id.depth(), 0);
        assert_eq!(id.seq(), 0);
        assert_eq!(id.worker_id(), 0);
    }
}
