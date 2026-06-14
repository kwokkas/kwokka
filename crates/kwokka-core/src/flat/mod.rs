//! Marker trait for types with stable, `repr(C)` byte layout.

mod layout;
mod primitive;

pub use layout::FlatLayout;
