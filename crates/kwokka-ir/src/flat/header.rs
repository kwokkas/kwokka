//! The 16-byte IR blob header.
//!
//! Layout (all multi-byte integers little-endian):
//!
//! ```text
//! offset size field        meaning
//!   0     4   magic         b"KWIR"
//!   4     2   version       wire format version
//!   6     2   flags         reserved (0)
//!   8     4   total_len     total blob length including header
//!  12     4   root_offset   byte offset to the root ConductorSpec record
//! ```

/// Magic bytes at offset 0 of every IR blob.
pub const MAGIC: [u8; 4] = *b"KWIR";

/// Current wire format version.
pub const VERSION: u16 = 1;

/// Total byte length of the header.
pub const HEADER_LEN: usize = 16;

/// Byte offset of the `root_offset` field within the header.
pub(crate) const ROOT_OFFSET_FIELD: usize = 12;
