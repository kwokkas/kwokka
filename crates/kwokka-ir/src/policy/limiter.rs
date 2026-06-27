//! Rate limiter policy view: a token bucket.

use crate::{
    error::IrError,
    flat::reader::{read_u32, read_u64},
};

/// A token-bucket rate limiter applied to a stage.
///
/// Decoded from a [`NodeTag::PolicyLimiter`] record body. The bucket
/// holds up to `capacity` tokens and adds `refill_tokens` every
/// `refill_period_ns`.
///
/// [`NodeTag::PolicyLimiter`]: crate::NodeTag::PolicyLimiter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimiterView {
    capacity: u32,
    refill_tokens: u32,
    refill_period_ns: u64,
}

impl LimiterView {
    /// Byte length of a limiter policy body on the wire.
    pub(crate) const LEN: usize = 16;

    /// Decodes a limiter policy body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body length is not exactly
    /// [`LimiterView::LEN`].
    pub(crate) fn parse(body: &[u8]) -> Result<Self, IrError> {
        if body.len() != Self::LEN {
            return Err(IrError::Truncated);
        }
        Ok(Self {
            capacity: read_u32(body, 0)?,
            refill_tokens: read_u32(body, 4)?,
            refill_period_ns: read_u64(body, 8)?,
        })
    }

    /// The maximum number of tokens the bucket holds.
    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// The number of tokens added each refill period.
    #[must_use]
    pub const fn refill_tokens(&self) -> u32 {
        self.refill_tokens
    }

    /// The refill period in nanoseconds.
    #[must_use]
    pub const fn refill_period_ns(&self) -> u64 {
        self.refill_period_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> [u8; 16] {
        let mut body = [0u8; 16];
        body[0..4].copy_from_slice(&100u32.to_le_bytes());
        body[4..8].copy_from_slice(&10u32.to_le_bytes());
        body[8..16].copy_from_slice(&1_000u64.to_le_bytes());
        body
    }

    #[test]
    fn decodes_token_bucket_fields() {
        let view = LimiterView::parse(&sample());
        assert_eq!(
            view.map(|v| (v.capacity(), v.refill_tokens(), v.refill_period_ns())),
            Ok((100, 10, 1_000))
        );
    }

    #[test]
    fn rejects_a_short_body() {
        assert_eq!(LimiterView::parse(&[0u8; 8]), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_long_body() {
        assert_eq!(LimiterView::parse(&[0u8; 24]), Err(IrError::Truncated));
    }
}
