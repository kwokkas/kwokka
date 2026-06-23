//! Userspace registry for `IORING_REGISTER_BUFFERS` pool slots.
//!
//! Tracks allocation state per slot via an inline bitmap. The actual
//! `IORING_REGISTER_BUFFERS` syscall lives in the `UringDriver` backend;
//! this registry manages only the userspace bookkeeping.
//!
//! No heap allocation -- the registry is a fixed-size `[u64; N]` bitmap
//! covering the maximum kernel slot count, with a runtime `capacity` cap.

#![allow(dead_code, reason = "pending registered-buffer wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::{RegisterError, buffer::slot::BufGroupId};

/// Maximum registered buffer slots. Covers `io_uring` kernel limit.
pub(crate) const MAX_REGISTERED_BUFFERS: usize = 32768;

const BUFFER_BITMAP_WORDS: usize = MAX_REGISTERED_BUFFERS / 64;

/// Userspace registry for registered buffer pool slots.
///
/// Tracks allocation state per slot via an inline bitmap. Capacity is
/// clamped to [`MAX_REGISTERED_BUFFERS`] at construction from
/// [`CapabilityMatrix`](crate::capability::CapabilityMatrix) kernel limits.
/// 4 KB inline, no heap allocation.
pub(crate) struct RegisteredBuffers {
    used: [u64; BUFFER_BITMAP_WORDS],
    capacity: u16,
}

impl RegisteredBuffers {
    /// Create a registry with `capacity` slots, all initially free.
    pub(crate) fn new(capacity: u16) -> Self {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "clamped to MAX_REGISTERED_BUFFERS which fits u16"
        )]
        let cap = (capacity as usize).min(MAX_REGISTERED_BUFFERS) as u16;
        Self {
            used: [0u64; BUFFER_BITMAP_WORDS],
            capacity: cap,
        }
    }

    /// Allocate the first free slot.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::SlotExhausted`] when all slots are in use.
    pub(crate) fn allocate(&mut self) -> Result<BufGroupId, RegisterError> {
        let limit = self.capacity as usize;
        let last_word = limit / 64;
        let last_bit = limit % 64;

        for (word_idx, word) in self.used.iter_mut().enumerate() {
            let mask = if word_idx < last_word {
                u64::MAX
            } else if word_idx == last_word && last_bit > 0 {
                (1u64 << last_bit) - 1
            } else {
                break;
            };

            let available = !*word & mask;
            if available != 0 {
                let bit = available.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;
                *word |= 1u64 << bit;
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "slot < capacity which is u16"
                )]
                return Ok(BufGroupId(slot as u16));
            }
        }
        Err(RegisterError::SlotExhausted)
    }

    /// Release a previously allocated slot.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::InvalidArgument`] if `group` is out of bounds
    /// or was not currently allocated (a logic bug at the call site).
    pub(crate) const fn release(&mut self, group: BufGroupId) -> Result<(), RegisterError> {
        let idx = group.0 as usize;
        if idx >= self.capacity as usize {
            return Err(RegisterError::InvalidArgument);
        }

        let word_idx = idx / 64;
        let bit = idx % 64;
        let mask = 1u64 << bit;

        if self.used[word_idx] & mask == 0 {
            return Err(RegisterError::InvalidArgument);
        }

        self.used[word_idx] &= !mask;
        Ok(())
    }

    /// Returns `true` if the slot is currently allocated.
    pub(crate) const fn is_allocated(&self, group: BufGroupId) -> bool {
        let idx = group.0 as usize;
        if idx >= self.capacity as usize {
            return false;
        }
        let word_idx = idx / 64;
        let bit = idx % 64;
        (self.used[word_idx] & (1u64 << bit)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_allocate_sequential() {
        let mut reg = RegisteredBuffers::new(4);
        let s0 = reg.allocate().map(|g| g.0);
        let s1 = reg.allocate().map(|g| g.0);
        let s2 = reg.allocate().map(|g| g.0);
        assert_eq!(s0, Ok(0));
        assert_eq!(s1, Ok(1));
        assert_eq!(s2, Ok(2));
    }

    #[test]
    fn buffer_exhausted() {
        let mut reg = RegisteredBuffers::new(2);
        assert!(reg.allocate().is_ok());
        assert!(reg.allocate().is_ok());
        assert_eq!(reg.allocate(), Err(RegisterError::SlotExhausted));
    }

    #[test]
    fn buffer_release_and_reallocate() {
        let mut reg = RegisteredBuffers::new(2);
        let s0 = reg.allocate().map(|g| g.0);
        let s1 = reg.allocate().map(|g| g.0);
        assert_eq!(s0, Ok(0));
        assert_eq!(s1, Ok(1));
        let _ = reg.release(BufGroupId(0));
        let s2 = reg.allocate().map(|g| g.0);
        assert_eq!(s2, Ok(0));
    }

    #[test]
    fn buffer_is_allocated() {
        let mut reg = RegisteredBuffers::new(4);
        let Ok(group) = reg.allocate() else {
            panic!("allocate must succeed");
        };
        assert!(reg.is_allocated(group));
        let _ = reg.release(group);
        assert!(!reg.is_allocated(group));
    }

    #[test]
    fn buffer_is_allocated_out_of_range() {
        let reg = RegisteredBuffers::new(2);
        assert!(!reg.is_allocated(BufGroupId(99)));
    }

    #[test]
    fn buffer_double_release_errors() {
        let mut reg = RegisteredBuffers::new(2);
        let Ok(group) = reg.allocate() else {
            panic!("allocate must succeed");
        };
        assert!(reg.release(group).is_ok());
        assert_eq!(reg.release(group), Err(RegisterError::InvalidArgument));
    }

    #[test]
    fn buffer_release_out_of_bounds_errors() {
        let mut reg = RegisteredBuffers::new(2);
        assert_eq!(
            reg.release(BufGroupId(10)),
            Err(RegisterError::InvalidArgument)
        );
    }

    #[test]
    fn buffer_capacity_boundary_partial_word() {
        let mut reg = RegisteredBuffers::new(100);
        for _ in 0..100 {
            assert!(reg.allocate().is_ok());
        }
        assert_eq!(reg.allocate(), Err(RegisterError::SlotExhausted));
    }

    #[test]
    fn zero_capacity_immediately_exhausted() {
        let mut reg = RegisteredBuffers::new(0);
        assert_eq!(reg.allocate(), Err(RegisterError::SlotExhausted));
    }
}
