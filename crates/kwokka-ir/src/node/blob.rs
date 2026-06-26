//! The IR root view, a validated handle over the raw blob bytes.

/// A validated kwokka IR blob.
///
/// Wraps the raw bytes of an IR blob. Construct via
/// [`KwokkaIr::from_trusted`] for in-arena bytes the writer produced in
/// this process, or via the validating reader for untrusted wire input
/// (added in a later step). Accessors return already-bounds-checked
/// views.
#[derive(Debug, Clone, Copy)]
pub struct KwokkaIr<'a> {
    bytes: &'a [u8],
}

impl<'a> KwokkaIr<'a> {
    /// Wraps trusted in-process bytes, skipping validation.
    ///
    /// The caller guarantees `bytes` is a blob produced by this crate's
    /// writer in the same process, so its structure is already sound.
    /// Bytes from outside the process must go through the validating
    /// reader instead.
    #[must_use]
    pub const fn from_trusted(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Returns the raw blob bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}
