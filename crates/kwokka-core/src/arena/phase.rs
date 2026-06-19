//! [`ArenaPhase`] -- lifecycle phase of a [`BumpAllocator`].
//!
//! [`BumpAllocator`]: crate::arena::BumpAllocator

/// Lifecycle phase of a [`BumpAllocator`].
///
/// The arena moves Build -> Frozen on `freeze()`, and Frozen -> Build on
/// `reset()`. Allocation is permitted only in Build; Frozen guarantees
/// pointer stability for in-flight reads. The set is closed: `Build` and
/// `Frozen` are the only phases.
///
/// [`BumpAllocator`]: crate::arena::BumpAllocator
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ArenaPhase {
    /// Allocation phase. `alloc*` succeed; `freeze()` transitions to `Frozen`.
    Build,
    /// Frozen phase. `alloc*` returns `WrongPhase`; pointers are stable
    /// for reads; `reset()` transitions back to `Build`.
    Frozen,
}
