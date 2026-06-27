//! A single stage node: its policy slots in `guard()` order.

use crate::{
    flat::reader::{read_record, read_u32},
    policy::{BreakerView, LimiterView, PolicyKind, RetryView, TimeoutView},
};

/// A stage in the conductor DAG, addressed by ordinal.
///
/// Carries the stage-table entry: four policy slots in `guard()` order,
/// where a zero offset means the slot is empty. Obtained from
/// [`ConductorView::stage`]. The parent [`ConductorView`] validated every
/// slot's record framing and tag at parse time, so each accessor decodes
/// within the already-checked payload.
///
/// [`ConductorView::stage`]: crate::ConductorView::stage
/// [`ConductorView`]: crate::ConductorView
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageView<'a> {
    body: &'a [u8],
    entry: &'a [u8],
}

impl<'a> StageView<'a> {
    /// Wraps a validated stage-table entry over the spec body.
    pub(crate) const fn new(body: &'a [u8], entry: &'a [u8]) -> Self {
        Self { body, entry }
    }

    /// Resolves the record body for a policy slot, or `None` if the slot
    /// is empty or its record does not match the slot kind.
    fn policy_body(&self, kind: PolicyKind) -> Option<&'a [u8]> {
        let Ok(slot_offset) = read_u32(self.entry, kind.slot_field()) else {
            return None;
        };
        if slot_offset == 0 {
            return None;
        }
        read_record(self.body, slot_offset as usize)
            .ok()
            .filter(|record| record.tag == kind.node_tag())
            .map(|record| record.body)
    }

    /// The rate limiter applied to this stage, if any.
    #[must_use]
    pub fn limiter(&self) -> Option<LimiterView> {
        self.policy_body(PolicyKind::Limiter)
            .and_then(|body| LimiterView::parse(body).ok())
    }

    /// The overall timeout applied to this stage, if any.
    #[must_use]
    pub fn timeout(&self) -> Option<TimeoutView> {
        self.policy_body(PolicyKind::Timeout)
            .and_then(|body| TimeoutView::parse(body).ok())
    }

    /// The retry policy applied to this stage, if any.
    #[must_use]
    pub fn retry(&self) -> Option<RetryView> {
        self.policy_body(PolicyKind::Retry)
            .and_then(|body| RetryView::parse(body).ok())
    }

    /// The circuit breaker applied to this stage, if any.
    #[must_use]
    pub fn breaker(&self) -> Option<BreakerView> {
        self.policy_body(PolicyKind::Breaker)
            .and_then(|body| BreakerView::parse(body).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncated_entry_yields_no_policy() {
        // An entry shorter than the slot table makes a later slot read fail,
        // which resolves to no policy rather than panicking.
        let body = [0u8; 8];
        let stage = StageView::new(&body, &[0u8; 4]);
        assert!(stage.timeout().is_none());
    }
}
