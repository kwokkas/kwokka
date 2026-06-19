//! Fixed-capacity bump allocator.

mod builder;
mod bump;
mod phase;

pub use builder::{BumpAllocatorBuilder, DEFAULT_BYTES, DEFAULT_DROP_SLOTS};
pub use bump::{ArenaError, BumpAllocator};
pub use phase::ArenaPhase;
