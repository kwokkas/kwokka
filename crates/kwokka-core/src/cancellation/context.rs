//! [`CancellationContext`] - full metadata for a cancellation event.

use crate::{
    cancellation::{CancellationKind, CancellationPolicy},
    id::Pip,
};

/// Full state of a cancellation event - observability-friendly metadata.
///
/// Carries the originating [`Pip`] across propagation so a consumer can
/// reconstruct cancel chains by source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationContext {
    /// The [`Pip`] that triggered this cancellation chain. Preserved across
    /// propagation - children receiving propagated cancel keep the original
    /// chain origin, not their immediate parent.
    pub source: Pip,

    /// Why this cancel happened.
    pub kind: CancellationKind,

    /// Policy applied to this cancel.
    pub policy: CancellationPolicy,

    /// Optional human-readable reason for logs and traces.
    pub reason: Option<&'static str>,

    /// Wall-clock timestamp (milliseconds since epoch) at cancel trigger.
    ///
    /// Caller-injected: core keeps no clock, so the value is supplied by
    /// whichever layer observes the cancellation.
    pub timestamp_ms: u64,
}

impl CancellationContext {
    /// Derive a context for a child task receiving propagated cancellation.
    ///
    /// All fields are preserved - `source` keeps the original chain origin
    /// for tracing, not the immediate parent. The child's own [`Pip`] is not
    /// recorded here: the context tracks the chain origin, and identity lives
    /// on the task itself.
    #[inline]
    #[must_use]
    pub const fn derive_for_child(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_context() -> CancellationContext {
        CancellationContext {
            source: Pip::root(),
            kind: CancellationKind::Hard,
            policy: CancellationPolicy::default(),
            reason: Some("test"),
            timestamp_ms: 12_345,
        }
    }

    #[test]
    fn derive_for_child_preserves_all_fields() {
        let parent = sample_context();
        let derived = parent.derive_for_child();
        assert_eq!(derived, parent);
    }

    #[test]
    fn derive_for_child_keeps_source_unchanged() {
        let parent = sample_context();
        let original_source = parent.source;
        let derived = parent.derive_for_child();
        assert_eq!(derived.source, original_source);
    }
}
