//! [`FlatLayout`] - marker trait for stable `repr(C)` byte-layout types.

/// Marker trait for types with stable, `repr(C)` byte layout.
///
/// The stable layout lets a value be treated as raw bytes. A bump
/// allocator can use the bound to skip drop on reset.
///
/// # Safety
///
/// Implementors must guarantee:
///
/// - `SIZE == core::mem::size_of::<Self>()`
/// - `ALIGN == core::mem::align_of::<Self>()`
/// - The type has stable layout - either a primitive or `#[repr(C)]`
///
/// A consumer that reads `SIZE` bytes at an `ALIGN`-aligned address as
/// `Self` has undefined behavior if these values do not match the real
/// layout.
pub unsafe trait FlatLayout {
    /// Size in bytes.
    const SIZE: usize;
    /// Alignment in bytes.
    const ALIGN: usize;
}
