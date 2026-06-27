//! The IR root view, a validated handle over the raw blob bytes.

/// A validated kwokka IR blob.
///
/// Wraps the raw bytes of an IR blob. The only safe public way to obtain
/// one is [`crate::validate`]; the in-process construction path is
/// `pub(crate)` so an untrusted caller cannot fabricate a `KwokkaIr` that
/// skips validation. Accessors return already-bounds-checked views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KwokkaIr<'a> {
    bytes: &'a [u8],
}

impl<'a> KwokkaIr<'a> {
    /// Wraps trusted in-process bytes, skipping validation.
    ///
    /// Crate-internal: the validating reader is the only safe public
    /// entry point. The caller guarantees `bytes` is a blob this crate's
    /// writer produced in the same process, so its structure is sound.
    #[must_use]
    pub(crate) const fn from_trusted(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Returns the raw blob bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_trusted_round_trips() {
        let bytes = [0x4b, 0x57, 0x49, 0x52];
        let ir = KwokkaIr::from_trusted(&bytes);
        assert_eq!(ir.as_bytes(), &bytes);
    }
}
