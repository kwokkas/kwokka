//! Policy slot kinds in `guard()` composition order.

use crate::{
    error::IrError,
    node::NodeTag,
    policy::{BreakerView, LimiterView, RetryView, TimeoutView},
};

/// A policy slot in a stage's guard composition.
///
/// Slots are stored positionally in `guard()` execution order, so the
/// discriminant doubles as the stage-table slot index:
/// `Limiter -> Timeout -> Retry -> Breaker -> execute`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    /// Rate limiter (outermost): shapes the call rate.
    Limiter = 0,
    /// Overall timeout: bounds total wall-clock around the retry loop.
    Timeout = 1,
    /// Retry: re-attempts on classified failures.
    Retry = 2,
    /// Circuit breaker (innermost): short-circuits on the failure rate.
    Breaker = 3,
}

impl PolicyKind {
    /// The number of policy slots in a stage-table entry.
    pub(crate) const COUNT: usize = 4;

    /// Every policy slot in `guard()` order, for slot iteration.
    pub(crate) const ALL: [Self; Self::COUNT] =
        [Self::Limiter, Self::Timeout, Self::Retry, Self::Breaker];

    /// The record tag a slot of this kind must carry.
    pub(crate) const fn node_tag(self) -> NodeTag {
        match self {
            Self::Limiter => NodeTag::PolicyLimiter,
            Self::Timeout => NodeTag::PolicyTimeout,
            Self::Retry => NodeTag::PolicyRetry,
            Self::Breaker => NodeTag::PolicyBreaker,
        }
    }

    /// Byte offset of this slot's record-offset field within a stage entry.
    pub(crate) const fn slot_field(self) -> usize {
        match self {
            Self::Limiter => 0,
            Self::Timeout => 4,
            Self::Retry => 8,
            Self::Breaker => 12,
        }
    }

    /// Checks that `body` decodes as this kind's policy view.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body is shorter than the
    /// view's fixed length.
    pub(crate) fn validate_body(self, body: &[u8]) -> Result<(), IrError> {
        match self {
            Self::Limiter => LimiterView::parse(body).map(drop),
            Self::Timeout => TimeoutView::parse(body).map(drop),
            Self::Retry => RetryView::parse(body).map(drop),
            Self::Breaker => BreakerView::parse(body).map(drop),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_index_matches_guard_order() {
        assert_eq!(PolicyKind::Limiter as usize, 0);
        assert_eq!(PolicyKind::Timeout as usize, 1);
        assert_eq!(PolicyKind::Retry as usize, 2);
        assert_eq!(PolicyKind::Breaker as usize, 3);
    }

    #[test]
    fn node_tag_maps_each_kind() {
        assert_eq!(PolicyKind::Limiter.node_tag(), NodeTag::PolicyLimiter);
        assert_eq!(PolicyKind::Timeout.node_tag(), NodeTag::PolicyTimeout);
        assert_eq!(PolicyKind::Retry.node_tag(), NodeTag::PolicyRetry);
        assert_eq!(PolicyKind::Breaker.node_tag(), NodeTag::PolicyBreaker);
    }
}
