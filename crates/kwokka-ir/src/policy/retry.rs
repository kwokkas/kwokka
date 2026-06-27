//! Retry policy view: attempt budget and backoff schedule.

use crate::{
    error::IrError,
    flat::reader::{read_u8, read_u32, read_u64},
};

/// A retry policy: the attempt budget and the backoff schedule.
///
/// Decoded from a [`NodeTag::PolicyRetry`] record body. Error
/// classification (which failures are retryable) is a code predicate, not
/// wire data, so the consumer resolves it rather than this view carrying
/// it.
///
/// [`NodeTag::PolicyRetry`]: crate::NodeTag::PolicyRetry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryView {
    max_attempts: u32,
    backoff_kind: u8,
    jitter_kind: u8,
    base_delay_ns: u64,
    max_delay_ns: u64,
}

impl RetryView {
    /// Backoff-schedule discriminant: fixed delay.
    pub const BACKOFF_FIXED: u8 = 0;
    /// Backoff-schedule discriminant: linear growth.
    pub const BACKOFF_LINEAR: u8 = 1;
    /// Backoff-schedule discriminant: exponential growth.
    pub const BACKOFF_EXPONENTIAL: u8 = 2;
    /// Jitter discriminant: no jitter.
    pub const JITTER_NONE: u8 = 0;
    /// Jitter discriminant: full randomized jitter.
    pub const JITTER_FULL: u8 = 1;

    /// Byte length of a retry policy body on the wire.
    pub(crate) const LEN: usize = 24;

    /// Decodes a retry policy body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body length is not exactly
    /// [`RetryView::LEN`].
    pub(crate) fn parse(body: &[u8]) -> Result<Self, IrError> {
        if body.len() != Self::LEN {
            return Err(IrError::Truncated);
        }
        Ok(Self {
            max_attempts: read_u32(body, 0)?,
            backoff_kind: read_u8(body, 4)?,
            jitter_kind: read_u8(body, 5)?,
            base_delay_ns: read_u64(body, 8)?,
            max_delay_ns: read_u64(body, 16)?,
        })
    }

    /// The maximum number of attempts before the retry gives up.
    #[must_use]
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The backoff-schedule discriminant; see [`RetryView::BACKOFF_FIXED`],
    /// [`RetryView::BACKOFF_LINEAR`], [`RetryView::BACKOFF_EXPONENTIAL`].
    #[must_use]
    pub const fn backoff_kind(&self) -> u8 {
        self.backoff_kind
    }

    /// The jitter discriminant; see [`RetryView::JITTER_NONE`],
    /// [`RetryView::JITTER_FULL`].
    #[must_use]
    pub const fn jitter_kind(&self) -> u8 {
        self.jitter_kind
    }

    /// The base delay between attempts in nanoseconds.
    #[must_use]
    pub const fn base_delay_ns(&self) -> u64 {
        self.base_delay_ns
    }

    /// The delay cap in nanoseconds.
    #[must_use]
    pub const fn max_delay_ns(&self) -> u64 {
        self.max_delay_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> [u8; 24] {
        let mut body = [0u8; 24];
        body[0..4].copy_from_slice(&5u32.to_le_bytes());
        body[4] = 2;
        body[5] = 1;
        body[8..16].copy_from_slice(&1_000u64.to_le_bytes());
        body[16..24].copy_from_slice(&60_000u64.to_le_bytes());
        body
    }

    #[test]
    fn decodes_attempt_and_backoff_fields() {
        let view = RetryView::parse(&sample());
        assert_eq!(
            view.map(|v| (
                v.max_attempts(),
                v.backoff_kind(),
                v.jitter_kind(),
                v.base_delay_ns(),
                v.max_delay_ns()
            )),
            Ok((5, 2, 1, 1_000, 60_000))
        );
    }

    #[test]
    fn rejects_a_short_body() {
        assert_eq!(RetryView::parse(&[0u8; 16]), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_long_body() {
        assert_eq!(RetryView::parse(&[0u8; 32]), Err(IrError::Truncated));
    }

    #[test]
    fn discriminant_constants_match_wire_values() {
        assert_eq!(RetryView::BACKOFF_FIXED, 0);
        assert_eq!(RetryView::BACKOFF_LINEAR, 1);
        assert_eq!(RetryView::BACKOFF_EXPONENTIAL, 2);
        assert_eq!(RetryView::JITTER_NONE, 0);
        assert_eq!(RetryView::JITTER_FULL, 1);
    }
}
