//! The caller-supplied description of a conductor, before it is encoded.
//!
//! These are the write-side twins of the read-side views next door: they
//! describe a conductor, not a wire encoding of one. Nothing here knows a
//! byte offset, a record tag, or an alignment rule -- that is
//! [`crate::flat::writer`]'s job, and it takes these as its input.

use crate::{
    conductor::EdgeView,
    config::ScalarValue,
    policy::{BreakerView, LimiterView, RetryView, TimeoutView},
};

/// The four policy slots of a stage in `guard()` order.
///
/// A `None` slot means the stage carries no policy of that kind. This is
/// the per-stage writer input, paired with the edge list passed via
/// [`ConductorBlob`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StageSpec {
    /// Rate limiter slot; `None` if this stage carries no rate limit.
    pub limiter: Option<LimiterView>,
    /// Overall timeout slot; `None` if this stage has no wall-clock cap.
    pub timeout: Option<TimeoutView>,
    /// Retry slot; `None` if this stage does not retry on failure.
    pub retry: Option<RetryView>,
    /// Circuit breaker slot; `None` if this stage has no breaker.
    pub breaker: Option<BreakerView>,
}

/// The stage-name registry input: one name per stage plus a name-sorted
/// ordinal index.
///
/// `names[i]` is stage `i`'s name; `sorted` lists every stage ordinal in
/// ascending name order for the reader's binary search. Both must have one
/// entry per stage. Sorting is the caller's responsibility: the writer is
/// allocation-free and copies `sorted` verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegistrySpec<'a> {
    /// Stage names in ordinal order; `names.len()` must equal the stage count.
    pub names: &'a [&'a [u8]],
    /// Stage ordinals in ascending name order; one per stage.
    pub sorted: &'a [u16],
}

/// One config binding input: which policy field of which stage to bind, the
/// external config key, and a default value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigBindingSpec<'a> {
    /// The target stage ordinal.
    pub node_ordinal: u16,
    /// The bound policy field; see the `ConfigBindingView::FIELD_*` constants.
    pub field_tag: u16,
    /// The external config key bytes.
    pub key: &'a [u8],
    /// The default value applied when the config source omits the key.
    pub default_value: ScalarValue,
}

/// The full input to [`write_conductor`](crate::write_conductor): stages, edges, and the optional
/// registry and config sections.
///
/// Named `ConductorBlob` rather than `ConductorSpec` to avoid colliding with
/// the read-side wire record of that name.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConductorBlob<'a> {
    /// The stage table; slot `i` carries stage `i`'s policies.
    pub stages: &'a [StageSpec],
    /// The DAG edges connecting stage ordinals.
    pub edges: &'a [EdgeView],
    /// The optional stage-name registry; `None` emits no registry section.
    pub registry: Option<RegistrySpec<'a>>,
    /// The config bindings; an empty slice emits no config section.
    pub config: &'a [ConfigBindingSpec<'a>],
}
