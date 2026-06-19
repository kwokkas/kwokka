//! [`BumpAllocatorBuilder`] -- fluent constructor for [`BumpAllocator`].
//!
//! [`BumpAllocator`]: crate::arena::BumpAllocator

use crate::arena::bump::{ArenaError, BumpAllocator};

/// Default backing capacity in bytes when `bytes` is not set.
pub const DEFAULT_BYTES: usize = 64 * 1024;

/// Default drop-slot capacity when `drop_slots` is not set. Zero
/// forbids `alloc_with_drop` entirely.
pub const DEFAULT_DROP_SLOTS: usize = 0;

/// Default backing buffer alignment in bytes. 4 KiB satisfies the
/// `O_DIRECT` requirement for registered `io_uring` buffers.
pub(super) const DEFAULT_ALIGNMENT: usize = 4096;

/// Builder for [`BumpAllocator`]. Obtain via [`BumpAllocator::builder`].
///
/// [`BumpAllocator`]: crate::arena::BumpAllocator
/// [`BumpAllocator::builder`]: crate::arena::BumpAllocator::builder
#[derive(Clone, Copy, Debug)]
pub struct BumpAllocatorBuilder {
    bytes: usize,
    drop_slots: usize,
    alignment: usize,
}

impl Default for BumpAllocatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BumpAllocatorBuilder {
    #[inline]
    pub(super) const fn new() -> Self {
        Self {
            bytes: DEFAULT_BYTES,
            drop_slots: DEFAULT_DROP_SLOTS,
            alignment: DEFAULT_ALIGNMENT,
        }
    }

    /// Sets the backing buffer capacity in bytes.
    #[inline]
    #[must_use]
    pub const fn bytes(mut self, bytes: usize) -> Self {
        self.bytes = bytes;
        self
    }

    /// Sets the backing buffer alignment in bytes.
    ///
    /// Must be a power of two. Defaults to 4096 (4 KiB), which
    /// satisfies the `O_DIRECT` requirement for registered `io_uring`
    /// buffers.
    ///
    /// # Errors
    ///
    /// [`build`] returns [`ArenaError::InvalidAlignment`] when
    /// `alignment` is not a power of two.
    ///
    /// [`build`]: BumpAllocatorBuilder::build
    /// [`ArenaError::InvalidAlignment`]: crate::arena::ArenaError::InvalidAlignment
    #[inline]
    #[must_use]
    pub const fn alignment(mut self, alignment: usize) -> Self {
        self.alignment = alignment;
        self
    }

    /// Sets the maximum number of `alloc_with_drop` registrations.
    ///
    /// Each registration consumes one slot of a fixed-size LIFO drop
    /// registry. Set to zero (the default) to forbid `alloc_with_drop`.
    #[inline]
    #[must_use]
    pub const fn drop_slots(mut self, drop_slots: usize) -> Self {
        self.drop_slots = drop_slots;
        self
    }

    /// Constructs the [`BumpAllocator`].
    ///
    /// # Errors
    ///
    /// - [`ArenaError::ZeroCapacity`] when `bytes` is zero.
    /// - [`ArenaError::InvalidAlignment`] when `alignment` is not a power of two.
    ///
    /// [`BumpAllocator`]: crate::arena::BumpAllocator
    /// [`ArenaError::InvalidAlignment`]: crate::arena::ArenaError::InvalidAlignment
    pub fn build(self) -> Result<BumpAllocator, ArenaError> {
        BumpAllocator::from_builder(self.bytes, self.drop_slots, self.alignment)
    }
}
