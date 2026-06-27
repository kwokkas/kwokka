//! Validating reader for untrusted IR blobs.
//!
//! [`validate`] is the only safe public way to obtain a [`KwokkaIr`] from
//! wire-loaded bytes. It bounds-checks the header and the root record
//! frame before any structural read, upholding the two-tier trust model:
//! in-process bytes are trusted (`KwokkaIr::from_trusted`), wire bytes
//! are not.

use crate::{
    error::IrError,
    flat::{HEADER_LEN, MAGIC, VERSION, header::ROOT_OFFSET_FIELD},
    node::{KwokkaIr, NodeTag},
};

/// Byte length of a record frame header (`tag u16 | _pad u16 | len u32`).
const RECORD_HEADER_LEN: usize = 8;

/// Required alignment of a record offset and of a record length.
const RECORD_ALIGN: usize = 8;

/// A bounds-checked view of one record frame.
pub(crate) struct RecordView<'a> {
    /// The decoded record tag.
    pub(crate) tag: NodeTag,
    /// The record body: the bytes after the frame header, up to the
    /// record length.
    pub(crate) body: &'a [u8],
}

/// Reads a little-endian `u16` at `offset`.
///
/// # Errors
///
/// Returns [`IrError::Truncated`] if `offset..offset + 2` is out of range.
fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, IrError> {
    let end = offset.checked_add(2).ok_or(IrError::Truncated)?;
    match bytes.get(offset..end) {
        Some(&[a, b]) => Ok(u16::from_le_bytes([a, b])),
        _ => Err(IrError::Truncated),
    }
}

/// Reads a `u8` at `offset`.
///
/// # Errors
///
/// Returns [`IrError::Truncated`] if `offset` is out of range.
pub(crate) fn read_u8(bytes: &[u8], offset: usize) -> Result<u8, IrError> {
    bytes.get(offset).copied().ok_or(IrError::Truncated)
}

/// Reads a little-endian `u32` at `offset`.
///
/// # Errors
///
/// Returns [`IrError::Truncated`] if `offset..offset + 4` is out of range.
pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IrError> {
    let end = offset.checked_add(4).ok_or(IrError::Truncated)?;
    match bytes.get(offset..end) {
        Some(&[a, b, c, d]) => Ok(u32::from_le_bytes([a, b, c, d])),
        _ => Err(IrError::Truncated),
    }
}

/// Reads a little-endian `u64` at `offset`.
///
/// # Errors
///
/// Returns [`IrError::Truncated`] if `offset..offset + 8` is out of range.
pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IrError> {
    let end = offset.checked_add(8).ok_or(IrError::Truncated)?;
    let chunk: [u8; 8] = bytes
        .get(offset..end)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(IrError::Truncated)?;
    Ok(u64::from_le_bytes(chunk))
}

/// Reads and bounds-checks the record frame at `offset`.
///
/// # Errors
///
/// Returns [`IrError::Misaligned`] if `offset` or the record length is not
/// 8-byte aligned, [`IrError::OutOfBounds`] if the record extends past the
/// blob or its length cannot hold the frame header, [`IrError::Truncated`]
/// if the frame header itself is out of range, and [`IrError::BadTag`] if
/// the tag is not a recognized record kind.
pub(crate) fn read_record(bytes: &[u8], offset: usize) -> Result<RecordView<'_>, IrError> {
    if offset % RECORD_ALIGN != 0 {
        return Err(IrError::Misaligned);
    }
    let tag_raw = read_u16(bytes, offset)?;
    let len_offset = offset.checked_add(4).ok_or(IrError::Truncated)?;
    let len = read_u32(bytes, len_offset)? as usize;
    if len < RECORD_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    if len % RECORD_ALIGN != 0 {
        return Err(IrError::Misaligned);
    }
    let end = offset.checked_add(len).ok_or(IrError::OutOfBounds)?;
    let body_start = offset
        .checked_add(RECORD_HEADER_LEN)
        .ok_or(IrError::OutOfBounds)?;
    let body = bytes.get(body_start..end).ok_or(IrError::OutOfBounds)?;
    let tag = NodeTag::from_u16(tag_raw).ok_or(IrError::BadTag { tag: tag_raw })?;
    Ok(RecordView { tag, body })
}

/// Validates wire-loaded bytes and returns a [`KwokkaIr`] view.
///
/// This is the only safe public way to obtain a [`KwokkaIr`] from
/// untrusted bytes. It checks the 16-byte header and the root record
/// frame so every later accessor reads within bounds. Graph
/// well-formedness (cycles, edge arity, topological order) is the
/// consumer's responsibility, not the codec's.
///
/// # Errors
///
/// Returns [`IrError::Truncated`] if the blob is shorter than the header
/// or its `total_len` disagrees with the buffer, [`IrError::BadMagic`] if
/// the leading bytes are not `KWIR`, [`IrError::UnsupportedVersion`] if
/// the wire version is newer than this reader, and [`IrError::Misaligned`],
/// [`IrError::OutOfBounds`], or [`IrError::BadTag`] if the root record
/// frame is malformed or is not a [`NodeTag::ConductorSpec`].
pub fn validate(bytes: &[u8]) -> Result<KwokkaIr<'_>, IrError> {
    if bytes.len() < HEADER_LEN {
        return Err(IrError::Truncated);
    }
    match bytes.get(..MAGIC.len()) {
        Some(leading) if *leading == MAGIC => {}
        _ => return Err(IrError::BadMagic),
    }
    let version = read_u16(bytes, 4)?;
    if version > VERSION {
        return Err(IrError::UnsupportedVersion { found: version });
    }
    let total_len = read_u32(bytes, 8)? as usize;
    if total_len != bytes.len() {
        return Err(IrError::Truncated);
    }
    let root_offset = read_u32(bytes, ROOT_OFFSET_FIELD)? as usize;
    let root = read_record(bytes, root_offset)?;
    if root.tag != NodeTag::ConductorSpec {
        return Err(IrError::BadTag {
            tag: root.tag as u16,
        });
    }
    Ok(KwokkaIr::from_trusted(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT_OFFSET: usize = HEADER_LEN;
    const TOTAL_LEN: usize = HEADER_LEN + RECORD_HEADER_LEN;

    fn le32(value: usize) -> [u8; 4] {
        let [a, b, c, d, ..] = value.to_le_bytes();
        [a, b, c, d]
    }

    fn valid_blob() -> [u8; TOTAL_LEN] {
        let mut blob = [0u8; TOTAL_LEN];
        blob[..MAGIC.len()].copy_from_slice(&MAGIC);
        blob[4..6].copy_from_slice(&VERSION.to_le_bytes());
        blob[8..12].copy_from_slice(&le32(TOTAL_LEN));
        blob[12..16].copy_from_slice(&le32(ROOT_OFFSET));
        blob[16..18].copy_from_slice(&(NodeTag::ConductorSpec as u16).to_le_bytes());
        blob[20..24].copy_from_slice(&le32(RECORD_HEADER_LEN));
        blob
    }

    #[test]
    fn accepts_a_minimal_valid_blob() {
        let blob = valid_blob();
        assert_eq!(validate(&blob).map(|ir| ir.as_bytes()), Ok(&blob[..]));
    }

    #[test]
    fn reads_the_root_record_body() {
        let blob = valid_blob();
        assert!(matches!(
            read_record(&blob, ROOT_OFFSET),
            Ok(record) if record.tag == NodeTag::ConductorSpec && record.body.is_empty()
        ));
    }

    #[test]
    fn rejects_a_short_blob() {
        let blob = [0u8; HEADER_LEN - 1];
        assert_eq!(validate(&blob), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut blob = valid_blob();
        blob[0] = 0;
        assert_eq!(validate(&blob), Err(IrError::BadMagic));
    }

    #[test]
    fn rejects_a_newer_version() {
        let mut blob = valid_blob();
        blob[4..6].copy_from_slice(&(VERSION + 1).to_le_bytes());
        assert_eq!(
            validate(&blob),
            Err(IrError::UnsupportedVersion { found: VERSION + 1 })
        );
    }

    #[test]
    fn rejects_a_total_len_mismatch() {
        let mut blob = valid_blob();
        blob[8..12].copy_from_slice(&le32(TOTAL_LEN + RECORD_ALIGN));
        assert_eq!(validate(&blob), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_misaligned_root_offset() {
        let mut blob = valid_blob();
        blob[12..16].copy_from_slice(&le32(ROOT_OFFSET + 1));
        assert_eq!(validate(&blob), Err(IrError::Misaligned));
    }

    #[test]
    fn rejects_an_overrun_record() {
        let mut blob = valid_blob();
        blob[20..24].copy_from_slice(&le32(RECORD_HEADER_LEN + RECORD_ALIGN));
        assert_eq!(validate(&blob), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_an_unknown_root_tag() {
        let mut blob = valid_blob();
        blob[16..18].copy_from_slice(&999u16.to_le_bytes());
        assert_eq!(validate(&blob), Err(IrError::BadTag { tag: 999 }));
    }

    #[test]
    fn rejects_a_non_conductor_root() {
        let mut blob = valid_blob();
        blob[16..18].copy_from_slice(&(NodeTag::StageNode as u16).to_le_bytes());
        assert_eq!(
            validate(&blob),
            Err(IrError::BadTag {
                tag: NodeTag::StageNode as u16,
            })
        );
    }

    #[test]
    fn rejects_a_short_record_len() {
        let mut blob = valid_blob();
        blob[20..24].copy_from_slice(&le32(RECORD_HEADER_LEN - 1));
        assert_eq!(validate(&blob), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_a_misaligned_record_len() {
        let mut blob = valid_blob();
        blob[20..24].copy_from_slice(&le32(RECORD_HEADER_LEN + 1));
        assert_eq!(validate(&blob), Err(IrError::Misaligned));
    }

    #[test]
    fn rejects_a_header_root_offset() {
        let mut blob = valid_blob();
        blob[12..16].copy_from_slice(&le32(0));
        assert_eq!(validate(&blob), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_an_oversized_record_len() {
        let mut blob = valid_blob();
        blob[20..24].copy_from_slice(&0xFFFF_FFF8_u32.to_le_bytes());
        assert_eq!(validate(&blob), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_a_truncated_root_frame() {
        let mut blob = valid_blob();
        blob[12..16].copy_from_slice(&le32(TOTAL_LEN));
        assert_eq!(validate(&blob), Err(IrError::Truncated));
    }

    #[test]
    fn rejects_a_len_field_overrun() {
        let mut blob = [0u8; 20];
        let total = blob.len();
        blob[..MAGIC.len()].copy_from_slice(&MAGIC);
        blob[4..6].copy_from_slice(&VERSION.to_le_bytes());
        blob[8..12].copy_from_slice(&le32(total));
        blob[12..16].copy_from_slice(&le32(HEADER_LEN));
        blob[16..18].copy_from_slice(&(NodeTag::ConductorSpec as u16).to_le_bytes());
        assert_eq!(validate(&blob), Err(IrError::Truncated));
    }
}
