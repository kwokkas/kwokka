//! Scalar default value for a config binding.

use crate::{
    error::IrError,
    flat::reader::{read_u8, read_u64},
};

/// A scalar default value for a config binding.
///
/// Decoded from the 16-byte scalar tail of a config-table entry: a `u8`
/// kind discriminant, padding, then a `u64` value. The kind is a raw
/// discriminant with named constants ([`ScalarValue::KIND_U64`] and peers);
/// an unrecognized kind passes the codec and is the consumer's to reject,
/// matching the two-tier trust model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarValue {
    kind: u8,
    value: u64,
}

impl ScalarValue {
    /// Kind discriminant: an unsigned integer held in `value`.
    pub const KIND_U64: u8 = 0;
    /// Kind discriminant: a boolean, `false` when `value` is zero.
    pub const KIND_BOOL: u8 = 1;
    /// Kind discriminant: a duration in nanoseconds.
    pub const KIND_DURATION_NS: u8 = 2;

    /// Byte length of a scalar value on the wire.
    pub(crate) const LEN: usize = 16;

    /// Constructs a scalar from a kind discriminant and its raw value.
    #[must_use]
    pub const fn new(kind: u8, value: u64) -> Self {
        Self { kind, value }
    }

    /// Decodes a scalar value body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if the body length is not exactly
    /// [`ScalarValue::LEN`].
    pub(crate) fn parse(body: &[u8]) -> Result<Self, IrError> {
        if body.len() != Self::LEN {
            return Err(IrError::Truncated);
        }
        Ok(Self {
            kind: read_u8(body, 0)?,
            value: read_u64(body, 8)?,
        })
    }

    /// The raw kind discriminant; see [`ScalarValue::KIND_U64`] and peers.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        self.kind
    }

    /// The raw 64-bit value, interpreted per [`ScalarValue::kind`].
    #[must_use]
    pub const fn raw_value(&self) -> u64 {
        self.value
    }

    /// The value as a boolean: `true` when the raw value is nonzero.
    #[must_use]
    pub const fn as_bool(&self) -> bool {
        self.value != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> [u8; 16] {
        let mut body = [0u8; 16];
        body[0] = ScalarValue::KIND_DURATION_NS;
        body[8..16].copy_from_slice(&5_000u64.to_le_bytes());
        body
    }

    #[test]
    fn decodes_kind_and_value() {
        let view = ScalarValue::parse(&sample());
        assert_eq!(
            view.map(|v| (v.kind(), v.raw_value())),
            Ok((ScalarValue::KIND_DURATION_NS, 5_000))
        );
    }

    #[test]
    fn as_bool_reads_nonzero() {
        assert!(ScalarValue::new(ScalarValue::KIND_BOOL, 1).as_bool());
        assert!(!ScalarValue::new(ScalarValue::KIND_BOOL, 0).as_bool());
    }

    #[test]
    fn rejects_a_short_body() {
        assert_eq!(ScalarValue::parse(&[0u8; 8]), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_long_body() {
        assert_eq!(ScalarValue::parse(&[0u8; 24]), Err(IrError::Truncated));
    }
}
