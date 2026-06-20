//! Timer entry handle over a slab key, with slab-index to `NonZeroU32`
//! conversion for the intrusive timer lists.

#![allow(
    dead_code,
    reason = "timer registration and cancellation are pending scheduler wire-up"
)]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::num::NonZeroU32;

use kwokka_core::slab::SlabKey;

/// Handle to a registered timer entry.
///
/// Wraps a [`SlabKey`] for direct use with the slab API. Use
/// [`nz`](TimerHandle::nz) to extract the `NonZeroU32` index for
/// intrusive list operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerHandle {
    key: SlabKey,
}

impl TimerHandle {
    /// Wrap a [`SlabKey`] as a timer handle.
    pub(crate) const fn from_key(key: SlabKey) -> Self {
        Self { key }
    }

    /// Returns the underlying [`SlabKey`].
    pub(crate) const fn key(self) -> SlabKey {
        self.key
    }

    /// Slab index as [`NonZeroU32`] for intrusive link operations.
    pub(crate) const fn nz(self) -> NonZeroU32 {
        slab_to_nz(self.key.index())
    }
}

/// Convert a slab index to [`NonZeroU32`] by adding 1.
///
/// # Panics
///
/// Panics if `index` is `u32::MAX` (overflow on +1).
#[inline]
pub(crate) const fn slab_to_nz(index: u32) -> NonZeroU32 {
    let Some(nz) = NonZeroU32::new(index + 1) else {
        panic!("slab index must not overflow u32::MAX")
    };
    nz
}

/// Convert a [`NonZeroU32`] back to a slab index.
#[inline]
pub(crate) const fn nz_to_slab(nz: NonZeroU32) -> u32 {
    nz.get() - 1
}
