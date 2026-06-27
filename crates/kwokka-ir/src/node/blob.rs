//! The IR root view, a validated handle over the raw blob bytes.

use crate::{
    conductor::ConductorView,
    error::IrError,
    flat::{
        header::ROOT_OFFSET_FIELD,
        reader::{read_record, read_u32},
    },
    node::NodeTag,
};

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

    /// Reads the conductor DAG spec from this validated blob.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::BadTag`] if the root record is not a
    /// [`NodeTag::ConductorSpec`], and the conductor-body variants if the
    /// body is malformed: a count or offset out of range, an edge table
    /// past the body, or an edge naming a stage ordinal at or beyond the
    /// stage count.
    pub fn conductor(&self) -> Result<ConductorView<'a>, IrError> {
        let root_offset = read_u32(self.bytes, ROOT_OFFSET_FIELD)? as usize;
        let root = read_record(self.bytes, root_offset)?;
        if root.tag != NodeTag::ConductorSpec {
            return Err(IrError::BadTag {
                tag: root.tag as u16,
            });
        }
        ConductorView::parse(root.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::{MAGIC, VERSION, validate};

    #[test]
    fn from_trusted_round_trips() {
        let bytes = [0x4b, 0x57, 0x49, 0x52];
        let ir = KwokkaIr::from_trusted(&bytes);
        assert_eq!(ir.as_bytes(), &bytes);
    }

    fn conductor_blob() -> [u8; 88] {
        let mut blob = [0u8; 88];
        blob[..MAGIC.len()].copy_from_slice(&MAGIC);
        blob[4..6].copy_from_slice(&VERSION.to_le_bytes());
        blob[8..12].copy_from_slice(&88u32.to_le_bytes());
        blob[12..16].copy_from_slice(&16u32.to_le_bytes());
        blob[16..18].copy_from_slice(&(NodeTag::ConductorSpec as u16).to_le_bytes());
        blob[20..24].copy_from_slice(&72u32.to_le_bytes());
        blob[24..28].copy_from_slice(&2u32.to_le_bytes());
        blob[28..32].copy_from_slice(&1u32.to_le_bytes());
        blob[32..36].copy_from_slice(&24u32.to_le_bytes());
        blob[36..40].copy_from_slice(&56u32.to_le_bytes());
        blob[80..82].copy_from_slice(&0u16.to_le_bytes());
        blob[82..84].copy_from_slice(&1u16.to_le_bytes());
        blob
    }

    #[test]
    fn conductor_reads_a_validated_dag() {
        let blob = conductor_blob();
        let view = validate(&blob).and_then(|ir| ir.conductor());
        assert!(matches!(
            view,
            Ok(spec) if spec.stage_count() == 2 && spec.edge_count() == 1
        ));
    }

    #[test]
    fn conductor_rejects_bad_root_tag() {
        let mut blob = conductor_blob();
        blob[16..18].copy_from_slice(&(NodeTag::StageNode as u16).to_le_bytes());
        let ir = KwokkaIr::from_trusted(&blob);
        assert_eq!(
            ir.conductor(),
            Err(IrError::BadTag {
                tag: NodeTag::StageNode as u16,
            })
        );
    }
}
