//! Userspace registry for `IORING_REGISTER_FILES` descriptor slots.
//!
//! Tracks allocation state per slot via an inline bitmap. The actual
//! `IORING_REGISTER_FILES` syscall lives in the `UringDriver` backend;
//! this registry manages only the userspace bookkeeping.
//!
//! No heap allocation -- the registry is a fixed-size `[u64; N]` bitmap
//! covering the maximum kernel slot count, with a runtime `capacity` cap.

#![allow(dead_code, reason = "pending registered-buffer wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use crate::{RegisterError, buffer::registration::slot::FdSlot};

/// Maximum registered fd slots.
pub(crate) const MAX_REGISTERED_FDS: usize = 65536;

const FD_BITMAP_WORDS: usize = MAX_REGISTERED_FDS / 64;

/// Userspace registry for registered file-descriptor slots.
///
/// Same allocation model as
/// [`RegisteredBuffers`](crate::buffer::registration::buffers::RegisteredBuffers) but indexed by
/// [`FdSlot`] with `u32` capacity, clamped to [`MAX_REGISTERED_FDS`]. 8 KB inline, no heap
/// allocation.
pub(crate) struct RegisteredFds {
    used: [u64; FD_BITMAP_WORDS],
    capacity: u32,
}

impl RegisteredFds {
    /// Create a registry with `capacity` slots, all initially free.
    pub(crate) fn new(capacity: u32) -> Self {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "clamped to MAX_REGISTERED_FDS which fits u32"
        )]
        let cap = (capacity as usize).min(MAX_REGISTERED_FDS) as u32;
        Self {
            used: [0u64; FD_BITMAP_WORDS],
            capacity: cap,
        }
    }

    /// Allocate the first free slot.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::SlotExhausted`] when all slots are in use.
    pub(crate) fn allocate(&mut self) -> Result<FdSlot, RegisterError> {
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
                    reason = "slot < capacity which is u32"
                )]
                return Ok(FdSlot(slot as u32));
            }
        }
        Err(RegisterError::SlotExhausted)
    }

    /// Release a previously allocated slot.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::InvalidArgument`] if `slot` is out of bounds
    /// or was not currently allocated (a logic bug at the call site).
    pub(crate) const fn release(&mut self, fd_slot: FdSlot) -> Result<(), RegisterError> {
        let idx = fd_slot.0 as usize;
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
    pub(crate) const fn is_allocated(&self, fd_slot: FdSlot) -> bool {
        let idx = fd_slot.0 as usize;
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
    fn fd_allocate_sequential() {
        let mut reg = RegisteredFds::new(4);
        let s0 = reg.allocate().map(|f| f.0);
        let s1 = reg.allocate().map(|f| f.0);
        assert_eq!(s0, Ok(0));
        assert_eq!(s1, Ok(1));
    }

    #[test]
    fn fd_exhausted() {
        let mut reg = RegisteredFds::new(2);
        assert!(reg.allocate().is_ok());
        assert!(reg.allocate().is_ok());
        assert_eq!(reg.allocate(), Err(RegisterError::SlotExhausted));
    }

    #[test]
    fn fd_release_and_reallocate() {
        let mut reg = RegisteredFds::new(2);
        assert!(reg.allocate().is_ok());
        let Ok(fd_slot) = reg.allocate() else {
            panic!("allocate must succeed");
        };
        let _ = reg.release(fd_slot);
        let s2 = reg.allocate().map(|f| f.0);
        assert_eq!(s2, Ok(1));
    }

    #[test]
    fn fd_is_allocated() {
        let mut reg = RegisteredFds::new(4);
        let Ok(fd_slot) = reg.allocate() else {
            panic!("allocate must succeed");
        };
        assert!(reg.is_allocated(fd_slot));
        let _ = reg.release(fd_slot);
        assert!(!reg.is_allocated(fd_slot));
    }

    #[test]
    fn fd_double_release_errors() {
        let mut reg = RegisteredFds::new(2);
        let Ok(fd_slot) = reg.allocate() else {
            panic!("allocate must succeed");
        };
        assert!(reg.release(fd_slot).is_ok());
        assert_eq!(reg.release(fd_slot), Err(RegisterError::InvalidArgument));
    }

    #[test]
    fn fd_release_out_of_bounds_errors() {
        let mut reg = RegisteredFds::new(2);
        assert_eq!(reg.release(FdSlot(10)), Err(RegisterError::InvalidArgument));
    }

    #[test]
    fn zero_capacity_immediately_exhausted() {
        let mut fd_reg = RegisteredFds::new(0);
        assert_eq!(fd_reg.allocate(), Err(RegisterError::SlotExhausted));
    }
}
