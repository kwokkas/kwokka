//! Circuit breaker policy view: failure-rate trip over a sliding window.

use crate::{
    error::IrError,
    flat::reader::{read_u8, read_u32, read_u64},
};

/// A circuit breaker that trips on the failure rate over a sliding window.
///
/// Decoded from a [`NodeTag::PolicyBreaker`] record body. The breaker
/// trips open when, after at least `minimum_calls` in the window, the
/// failure rate reaches `failure_rate_percent`; it stays open for
/// `open_duration_ns`, then admits up to `half_open_max_calls` trial
/// calls and closes once `half_open_success_threshold` of them succeed.
/// The rate is a whole-number percent (0-100) with no fractional
/// precision; rejecting an out-of-range rate is the consumer's job.
///
/// [`NodeTag::PolicyBreaker`]: crate::NodeTag::PolicyBreaker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerView {
    window_kind: u8,
    failure_rate_percent: u8,
    minimum_calls: u32,
    window_span: u64,
    half_open_max_calls: u32,
    half_open_success_threshold: u32,
    open_duration_ns: u64,
}

impl BreakerView {
    /// Window-kind discriminant: count-based sliding window.
    pub const WINDOW_COUNT: u8 = 0;
    /// Window-kind discriminant: time-based sliding window.
    pub const WINDOW_TIME: u8 = 1;

    /// Byte length of a breaker policy body on the wire.
    pub(crate) const LEN: usize = 32;

    /// Constructs a breaker policy from its sliding-window trip parameters.
    ///
    /// Parameters in order: `window_kind` (see [`BreakerView::WINDOW_COUNT`]
    /// and [`BreakerView::WINDOW_TIME`]), `failure_rate_percent` (0-100),
    /// `minimum_calls`, `window_span`, `half_open_max_calls`,
    /// `half_open_success_threshold`, and `open_duration_ns`. Intended for
    /// the conductor lowering and the imperative builder.
    #[must_use]
    pub const fn new(
        window_kind: u8,
        failure_rate_percent: u8,
        minimum_calls: u32,
        window_span: u64,
        half_open_max_calls: u32,
        half_open_success_threshold: u32,
        open_duration_ns: u64,
    ) -> Self {
        Self {
            window_kind,
            failure_rate_percent,
            minimum_calls,
            window_span,
            half_open_max_calls,
            half_open_success_threshold,
            open_duration_ns,
        }
    }

    /// Decodes a breaker policy body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body length is not exactly
    /// [`BreakerView::LEN`].
    pub(crate) fn parse(body: &[u8]) -> Result<Self, IrError> {
        if body.len() != Self::LEN {
            return Err(IrError::Truncated);
        }
        Ok(Self {
            window_kind: read_u8(body, 0)?,
            failure_rate_percent: read_u8(body, 1)?,
            minimum_calls: read_u32(body, 4)?,
            window_span: read_u64(body, 8)?,
            half_open_max_calls: read_u32(body, 16)?,
            half_open_success_threshold: read_u32(body, 20)?,
            open_duration_ns: read_u64(body, 24)?,
        })
    }

    /// The sliding-window discriminant; see [`BreakerView::WINDOW_COUNT`]
    /// and [`BreakerView::WINDOW_TIME`].
    #[must_use]
    pub const fn window_kind(&self) -> u8 {
        self.window_kind
    }

    /// The trip threshold as a whole-number failure percent (0-100).
    #[must_use]
    pub const fn failure_rate_percent(&self) -> u8 {
        self.failure_rate_percent
    }

    /// The minimum calls in the window before the rate is evaluated.
    #[must_use]
    pub const fn minimum_calls(&self) -> u32 {
        self.minimum_calls
    }

    /// The window span: call count when count-based, nanoseconds when
    /// time-based.
    #[must_use]
    pub const fn window_span(&self) -> u64 {
        self.window_span
    }

    /// The trial calls admitted while half-open.
    #[must_use]
    pub const fn half_open_max_calls(&self) -> u32 {
        self.half_open_max_calls
    }

    /// The successes among trial calls required to close.
    #[must_use]
    pub const fn half_open_success_threshold(&self) -> u32 {
        self.half_open_success_threshold
    }

    /// The duration the breaker stays open in nanoseconds.
    #[must_use]
    pub const fn open_duration_ns(&self) -> u64 {
        self.open_duration_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> [u8; 32] {
        let mut body = [0u8; 32];
        body[0] = 1;
        body[1] = 50;
        body[4..8].copy_from_slice(&20u32.to_le_bytes());
        body[8..16].copy_from_slice(&10_000u64.to_le_bytes());
        body[16..20].copy_from_slice(&3u32.to_le_bytes());
        body[20..24].copy_from_slice(&2u32.to_le_bytes());
        body[24..32].copy_from_slice(&30_000u64.to_le_bytes());
        body
    }

    #[test]
    fn decodes_rate_window_fields() {
        let view = BreakerView::parse(&sample());
        assert_eq!(
            view.map(|v| (
                v.window_kind(),
                v.failure_rate_percent(),
                v.minimum_calls(),
                v.window_span(),
                v.half_open_max_calls(),
                v.half_open_success_threshold(),
                v.open_duration_ns()
            )),
            Ok((1, 50, 20, 10_000, 3, 2, 30_000))
        );
    }

    #[test]
    fn rejects_a_short_body() {
        assert_eq!(BreakerView::parse(&[0u8; 16]), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_long_body() {
        assert_eq!(BreakerView::parse(&[0u8; 40]), Err(IrError::Truncated));
    }

    #[test]
    fn window_constants_match_wire_values() {
        assert_eq!(BreakerView::WINDOW_COUNT, 0);
        assert_eq!(BreakerView::WINDOW_TIME, 1);
    }
}
