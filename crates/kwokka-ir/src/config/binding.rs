//! Config binding view: a late-bind descriptor for one policy field.

use crate::{
    config::ScalarValue,
    error::IrError,
    flat::reader::{read_u16, read_u32},
};

/// A late-bind descriptor: which policy field of which stage to bind, the
/// external config key, and a default value.
///
/// Decoded from a 32-byte config-table entry: the target `node_ordinal`, a
/// `field_tag` selecting the bindable policy field (see the `FIELD_*`
/// constants), a string-ref to the external config key, and a
/// [`ScalarValue`] default. The IR carries only the descriptor; resolving
/// the key against a live config source is the consumer's job, not the
/// codec's.
///
/// A `field_tag` encodes the bound field as `(kind << 8) | index`: the high
/// byte is the [`PolicyKind`] discriminant (0 Limiter, 1 Timeout, 2 Retry,
/// 3 Breaker) and the low byte is the field's position within that policy.
/// The policy-discriminant sub-fields (`backoff_kind`, `jitter_kind`,
/// `window_kind`) occupy reserved low-byte indices and are not bindable, so
/// the `FIELD_*` constants skip them.
///
/// [`PolicyKind`]: crate::PolicyKind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigBindingView<'a> {
    node_ordinal: u16,
    field_tag: u16,
    key: &'a [u8],
    default: ScalarValue,
}

impl<'a> ConfigBindingView<'a> {
    /// Field tag: the limiter token capacity.
    pub const FIELD_LIMITER_CAPACITY: u16 = 0x0000;
    /// Field tag: the limiter refill token count.
    pub const FIELD_LIMITER_REFILL_TOKENS: u16 = 0x0001;
    /// Field tag: the limiter refill period in nanoseconds.
    pub const FIELD_LIMITER_REFILL_PERIOD_NS: u16 = 0x0002;
    /// Field tag: the overall timeout duration in nanoseconds.
    pub const FIELD_TIMEOUT_DURATION_NS: u16 = 0x0100;
    /// Field tag: the retry attempt budget.
    pub const FIELD_RETRY_MAX_ATTEMPTS: u16 = 0x0200;
    /// Field tag: the retry base delay in nanoseconds.
    pub const FIELD_RETRY_BASE_DELAY_NS: u16 = 0x0203;
    /// Field tag: the retry delay cap in nanoseconds.
    pub const FIELD_RETRY_MAX_DELAY_NS: u16 = 0x0204;
    /// Field tag: the breaker failure-rate trip threshold (whole percent).
    pub const FIELD_BREAKER_FAILURE_RATE_PERCENT: u16 = 0x0301;
    /// Field tag: the breaker minimum call count.
    pub const FIELD_BREAKER_MINIMUM_CALLS: u16 = 0x0302;
    /// Field tag: the breaker sliding-window span.
    pub const FIELD_BREAKER_WINDOW_SPAN: u16 = 0x0303;
    /// Field tag: the breaker half-open trial-call cap.
    pub const FIELD_BREAKER_HALF_OPEN_MAX_CALLS: u16 = 0x0304;
    /// Field tag: the breaker half-open success threshold.
    pub const FIELD_BREAKER_HALF_OPEN_SUCCESS_THRESHOLD: u16 = 0x0305;
    /// Field tag: the breaker open-state duration in nanoseconds.
    pub const FIELD_BREAKER_OPEN_DURATION_NS: u16 = 0x0306;

    /// Byte length of one config-table entry on the wire.
    pub(crate) const LEN: usize = 32;

    /// Byte offset of the scalar default within an entry.
    const SCALAR_OFFSET: usize = 16;

    /// Decodes one config-table entry, resolving its key against `body`.
    ///
    /// Crate-internal: [`ConductorView::parse`] slices the config table and
    /// the string heap before constructing each binding.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if `entry` is not
    /// [`ConfigBindingView::LEN`] bytes, and [`IrError::OutOfBounds`] if the
    /// key string-ref falls outside `body`.
    ///
    /// [`ConductorView::parse`]: crate::ConductorView
    pub(crate) fn parse(body: &'a [u8], entry: &[u8]) -> Result<Self, IrError> {
        if entry.len() != Self::LEN {
            return Err(IrError::Truncated);
        }
        let node_ordinal = read_u16(entry, 0)?;
        let field_tag = read_u16(entry, 2)?;
        let key_len = read_u32(entry, 4)? as usize;
        let key_offset = read_u32(entry, 8)? as usize;
        let key_end = key_offset
            .checked_add(key_len)
            .ok_or(IrError::OutOfBounds)?;
        let key = body.get(key_offset..key_end).ok_or(IrError::OutOfBounds)?;
        let scalar = entry
            .get(Self::SCALAR_OFFSET..Self::LEN)
            .ok_or(IrError::OutOfBounds)?;
        let default = ScalarValue::parse(scalar)?;
        Ok(Self {
            node_ordinal,
            field_tag,
            key,
            default,
        })
    }

    /// The target stage ordinal this binding configures.
    #[must_use]
    pub const fn node_ordinal(&self) -> u16 {
        self.node_ordinal
    }

    /// The field tag selecting the bound policy field; see the `FIELD_*`
    /// constants.
    #[must_use]
    pub const fn field_tag(&self) -> u16 {
        self.field_tag
    }

    /// The external config key bytes; UTF-8 validation is the consumer's.
    #[must_use]
    pub const fn key(&self) -> &'a [u8] {
        self.key
    }

    /// The default value applied when the config source omits the key.
    #[must_use]
    pub const fn default_value(&self) -> ScalarValue {
        self.default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 32-byte entry binding stage 1's timeout duration to key "ttl",
    /// default 5000 ns, with the key at body offset 0.
    fn binding() -> ([u8; 3], [u8; 32]) {
        let body = *b"ttl";
        let mut entry = [0u8; 32];
        entry[0..2].copy_from_slice(&1u16.to_le_bytes());
        entry[2..4].copy_from_slice(&ConfigBindingView::FIELD_TIMEOUT_DURATION_NS.to_le_bytes());
        entry[4..8].copy_from_slice(&3u32.to_le_bytes());
        entry[8..12].copy_from_slice(&0u32.to_le_bytes());
        entry[16] = ScalarValue::KIND_DURATION_NS;
        entry[24..32].copy_from_slice(&5_000u64.to_le_bytes());
        (body, entry)
    }

    #[test]
    fn reads_descriptor_fields() {
        let (body, entry) = binding();
        let view = ConfigBindingView::parse(&body, &entry);
        assert_eq!(
            view.map(|v| (v.node_ordinal(), v.field_tag(), v.key())),
            Ok((1, ConfigBindingView::FIELD_TIMEOUT_DURATION_NS, &b"ttl"[..]))
        );
    }

    #[test]
    fn reads_the_default_scalar() {
        let (body, entry) = binding();
        let value = ConfigBindingView::parse(&body, &entry).map(|v| v.default_value());
        assert_eq!(value.map(|s| (s.kind(), s.raw_value())), Ok((2, 5_000)));
    }

    #[test]
    fn rejects_a_short_entry() {
        assert_eq!(
            ConfigBindingView::parse(&[], &[0u8; 16]),
            Err(IrError::Truncated)
        );
    }

    #[test]
    fn rejects_a_key_overrun() {
        let (body, mut entry) = binding();
        entry[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(
            ConfigBindingView::parse(&body, &entry),
            Err(IrError::OutOfBounds)
        );
    }
}
