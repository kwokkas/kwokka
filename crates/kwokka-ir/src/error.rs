//! IR error type.

/// Errors from validating or reading a kwokka IR blob.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IrError {
    /// The blob does not begin with the `KWIR` magic bytes.
    BadMagic,
    /// The wire format version is newer than this reader understands.
    UnsupportedVersion {
        /// The version value read from the header.
        found: u16,
    },
    /// The blob ends before a required field could be read.
    Truncated,
    /// An offset or length points outside the blob bounds.
    OutOfBounds,
    /// A record does not begin on an 8-byte boundary.
    Misaligned,
    /// A record carries a node tag this reader does not recognize.
    BadTag {
        /// The unrecognized tag discriminant.
        tag: u16,
    },
    /// A graph edge names a stage ordinal past the end of the stage table.
    OrdinalOutOfRange,
}
