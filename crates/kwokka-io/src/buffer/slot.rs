//! Buffer group and fd slot identifier types.

/// Group ID for a registered buffer pool.
///
/// Obtained from `IoDriver::register_buffers` and placed in the SQE `buf_group`
/// field for `IORING_OP_PROVIDE_BUFFERS` and `buf_ring` multishot ops.
/// Valid until the matching `IoDriver::unregister_buffers` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufGroupId(pub(crate) u16);

/// Slot index in the `io_uring` registered file-descriptor table.
///
/// Obtained from `IoDriver::register_files` and used with `IOSQE_FIXED_FILE`
/// to reference a pre-registered fd without a per-submission table lookup.
/// Valid until the matching `IoDriver::unregister_files` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FdSlot(pub(crate) u32);

#[cfg(test)]
impl BufGroupId {
    pub(crate) const fn new(slot: u16) -> Self {
        Self(slot)
    }

    pub(crate) const fn raw(self) -> u16 {
        self.0
    }
}

#[cfg(test)]
impl FdSlot {
    pub(crate) const fn new(slot: u32) -> Self {
        Self(slot)
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::*;

    fn hash_of<T: Hash>(value: T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn buf_group_id_round_trip() {
        assert_eq!(BufGroupId::new(0).raw(), 0);
        assert_eq!(BufGroupId::new(u16::MAX).raw(), u16::MAX);
    }

    #[test]
    fn buf_group_id_partial_eq() {
        assert_eq!(BufGroupId::new(7), BufGroupId::new(7));
        assert_ne!(BufGroupId::new(7), BufGroupId::new(8));
    }

    #[test]
    fn buf_group_id_hash_eq() {
        assert_eq!(hash_of(BufGroupId::new(7)), hash_of(BufGroupId::new(7)));
    }

    #[test]
    fn fd_slot_round_trip() {
        assert_eq!(FdSlot::new(0).raw(), 0);
        assert_eq!(FdSlot::new(u32::MAX).raw(), u32::MAX);
    }

    #[test]
    fn fd_slot_partial_eq() {
        assert_eq!(FdSlot::new(42), FdSlot::new(42));
        assert_ne!(FdSlot::new(42), FdSlot::new(43));
    }

    #[test]
    fn fd_slot_hash_eq() {
        assert_eq!(hash_of(FdSlot::new(42)), hash_of(FdSlot::new(42)));
    }
}
