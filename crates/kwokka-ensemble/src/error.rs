//! Ensemble error type.

/// Errors from building or executing a kwokka orchestration.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnsembleError {
    /// A Conductor arena could not satisfy an allocation during the Build
    /// phase: the request exceeded the region's remaining capacity.
    ArenaExhausted {
        /// Bytes the allocation requested.
        requested: usize,
        /// Bytes still available in the arena.
        available: usize,
    },
}
