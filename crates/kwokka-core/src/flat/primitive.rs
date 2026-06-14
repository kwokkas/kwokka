//! `FlatLayout` impls for primitive types.
//!
//! `usize` and `isize` are intentionally excluded - their size is
//! platform-dependent, so a byte layout that must agree across platforms
//! cannot rely on them. Use a fixed-size integer instead.
//!
//! `bool`, `char`, `u128`, `i128`, `Option<T>`, and tuples are also excluded.
//! `bool` and `char` have invalid bit patterns; the others may be added
//! later if call sites emerge.

use core::mem::{align_of, size_of};

use crate::flat::FlatLayout;

macro_rules! impl_flat_primitive {
    ($($t:ty),* $(,)?) => {
        $(
            // SAFETY: `$t` is a Rust primitive with stable layout; SIZE and
            // ALIGN are derived from `size_of` / `align_of` of `Self`.
            // Incorrect values would cause misaligned reads or buffer
            // overruns at sites relying on the layout contract.
            unsafe impl FlatLayout for $t {
                const SIZE: usize = size_of::<Self>();
                const ALIGN: usize = align_of::<Self>();
            }
        )*
    };
}

// SAFETY (applies to every type below): each is a Rust primitive with a
// stable, platform-consistent layout, so the `size_of` / `align_of`-derived
// SIZE and ALIGN match the real layout. The macro emits one `unsafe impl` per
// type; wrong values would cause misaligned reads or buffer overruns.
impl_flat_primitive!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

// SAFETY: an array `[T; N]` of `FlatLayout` elements has size `T::SIZE * N`
// and alignment `T::ALIGN`, guaranteed by Rust's array layout rules. SIZE is
// a checked multiply: an overflowing `T::SIZE * N` is a compile-time panic,
// not a wrapped-small constant. Wrong values would cause misaligned reads or
// buffer overruns at sites relying on the layout contract.
unsafe impl<T: FlatLayout, const N: usize> FlatLayout for [T; N] {
    const SIZE: usize = match T::SIZE.checked_mul(N) {
        Some(value) => value,
        None => panic!("FlatLayout SIZE overflow: T::SIZE * N exceeds usize"),
    };
    const ALIGN: usize = T::ALIGN;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_size_is_n_times_t() {
        assert_eq!(<[u32; 4] as FlatLayout>::SIZE, 16);
        assert_eq!(<[u32; 4] as FlatLayout>::ALIGN, 4);
    }

    #[test]
    fn nested_array_size() {
        assert_eq!(<[[u8; 3]; 2] as FlatLayout>::SIZE, 6);
        assert_eq!(<[[u8; 3]; 2] as FlatLayout>::ALIGN, 1);
    }

    #[test]
    fn zero_size_array() {
        assert_eq!(<[u8; 0] as FlatLayout>::SIZE, 0);
        assert_eq!(<[u8; 0] as FlatLayout>::ALIGN, 1);
    }
}
