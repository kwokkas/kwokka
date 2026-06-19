//! Foundational types shared by all kwokka crates.
//!
//! Defines the leaf vocabulary on which the rest of the workspace builds:
//!
//! - [`CancellationKind`], [`CancellationPolicy`], [`CancellationContext`] - three-axis
//!   cancellation model
//! - [`FlatLayout`] - marker trait for types with stable byte layout
//! - [`SchedulingHint`] - scheduler selection hint
//! - [`AffinityHint`] - CPU affinity hint
//! - [`Pip`] - 128-bit tree-structured task identifier
//! - [`PipError`] - errors from [`Pip`] operations
//! - [`Namespace`] - logical task scope
//! - [`Slab`], [`SlabKey`], [`SlabError`] - generational slab allocator
//! - [`Generation`] - wrapping generation counter shared by slab and arena
//! - [`BumpAllocator`], [`BumpAllocatorBuilder`], [`ArenaError`], [`ArenaPhase`] - fixed-capacity
//!   bump allocator

pub mod arena;
pub mod cancellation;
pub mod flat;
pub mod generation;
pub mod hint;
pub mod id;
pub mod namespace;
pub mod slab;

pub use arena::{
    ArenaError, ArenaPhase, BumpAllocator, BumpAllocatorBuilder, DEFAULT_BYTES, DEFAULT_DROP_SLOTS,
};
pub use cancellation::{
    AlreadyCancelledBehavior, CancellationContext, CancellationKind, CancellationPolicy,
};
pub use flat::FlatLayout;
pub use generation::Generation;
pub use hint::{AffinityHint, SchedulingHint};
pub use id::{Pip, PipError};
pub use namespace::Namespace;
pub use slab::{Slab, SlabError, SlabKey};
