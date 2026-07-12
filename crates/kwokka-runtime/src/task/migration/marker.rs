//! Phantom-type [`Mode`] markers - `Affine` (`!Send`) vs `Stealing` (`Send + Sync`).
//!
//! `TaskHandle<T, M: Mode>` parameterizes over the mode to encode
//! at the type level whether a task may cross worker boundaries. The trait is
//! sealed so external crates cannot extend the axis.

use core::marker::PhantomData;

mod sealed {
    /// Sealing trait preventing external [`crate::task::Mode`] implementations.
    pub trait Sealed {}
}

/// `!Send` marker - task is pinned to the worker that spawned it.
///
/// The raw-pointer `PhantomData` blocks the auto-derived `Send` impl, which is
/// the desired behavior: `Affine` tasks must never migrate.
pub struct Affine(PhantomData<*const ()>);

/// `Send + Sync` marker - task may be stolen by another worker.
pub struct Stealing(PhantomData<()>);

/// Sealed marker trait selecting between [`Affine`] and [`Stealing`].
///
/// External crates cannot add a third axis; the only inhabitants are the two
/// markers defined alongside this trait.
pub trait Mode: sealed::Sealed {}

impl sealed::Sealed for Affine {}
impl Mode for Affine {}

impl sealed::Sealed for Stealing {}
impl Mode for Stealing {}

// Compile-time proof that `Stealing` is `Send + Sync`. `Affine` is `!Send` by
// construction: its `PhantomData<*const ()>` field blocks the auto impl.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Stealing>();
};

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn markers_are_zero_sized() {
        assert_eq!(core::mem::size_of::<Affine>(), 0);
        assert_eq!(core::mem::size_of::<Stealing>(), 0);
    }
}
