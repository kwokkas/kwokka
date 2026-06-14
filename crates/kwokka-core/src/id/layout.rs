//! Bit layout constants for [`crate::id::Pip`].
//!
//! ```text
//! [127..80] seq      - 48 bits, monotonic sequence number
//! [ 79..64] depth    - 16 bits, parent-child nesting depth
//! [ 63..48] kind     - 16 bits, reserved for future use (task/conductor/stage tag)
//! [ 47..40] version  -  8 bits, layout version
//! [ 39.. 2] worker   - 38 bits, originating worker / routing hint
//! [  1.. 0] flags    -  2 bits, reserved for future use
//! ```

/// Bit offset of the `seq` field within a `Pip`.
pub(super) const SEQ_SHIFT: u32 = 80;
/// Width of the `seq` field in bits.
pub(super) const SEQ_BITS: u32 = 48;
/// Mask isolating the `seq` field of a `Pip`.
pub(super) const SEQ_MASK: u128 = ((1u128 << SEQ_BITS) - 1) << SEQ_SHIFT;

/// Bit offset of the `depth` field within a `Pip`.
pub(super) const DEPTH_SHIFT: u32 = 64;
/// Width of the `depth` field in bits.
pub(super) const DEPTH_BITS: u32 = 16;
/// Mask isolating the `depth` field of a `Pip`.
pub(super) const DEPTH_MASK: u128 = ((1u128 << DEPTH_BITS) - 1) << DEPTH_SHIFT;

/// Bit offset of the `version` field within a `Pip`.
pub(super) const VERSION_SHIFT: u32 = 40;

/// Current layout version stored in every `Pip`.
pub(super) const CURRENT_VERSION: u8 = 0;

/// Bit offset of the `worker` field within a `Pip`.
pub(super) const WORKER_SHIFT: u32 = 2;
/// Width of the `worker` field in bits.
pub(super) const WORKER_BITS: u32 = 38;
/// Mask isolating the `worker` field of a `Pip`.
pub(super) const WORKER_MASK: u128 = ((1u128 << WORKER_BITS) - 1) << WORKER_SHIFT;

#[cfg(test)]
mod tests {
    use super::*;

    /// Layout invariant: bit field sizes must sum to 128.
    #[test]
    fn total_bits_sum_to_128() {
        const KIND_BITS: u32 = 16;
        const VERSION_BITS: u32 = 8;
        const FLAGS_BITS: u32 = 2;
        let total = SEQ_BITS + DEPTH_BITS + KIND_BITS + VERSION_BITS + WORKER_BITS + FLAGS_BITS;
        assert_eq!(total, 128);
    }

    /// Sanity check: `WORKER_SHIFT` + `WORKER_BITS` does not overlap `VERSION` region.
    #[test]
    fn worker_does_not_overlap_version() {
        const VERSION_SHIFT: u32 = 40;
        assert_eq!(WORKER_SHIFT + WORKER_BITS, VERSION_SHIFT);
    }

    /// Active masks (those used by current accessors) must not overlap.
    #[test]
    fn active_masks_non_overlapping() {
        let or = SEQ_MASK | DEPTH_MASK | WORKER_MASK;
        let xor = SEQ_MASK ^ DEPTH_MASK ^ WORKER_MASK;
        assert_eq!(or, xor);
    }
}
