//! A single directed edge in the conductor DAG.

/// A directed edge between two stage ordinals.
///
/// Decoded from an 8-byte edge entry: `from_ordinal`, `to_ordinal`,
/// `input_index`, then 2 bytes of padding. The ordinals address stages by
/// position; `input_index` is the parameter position on a multi-input
/// target stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeView {
    from_ordinal: u16,
    to_ordinal: u16,
    input_index: u16,
}

impl EdgeView {
    /// Byte length of one edge entry on the wire.
    pub(crate) const LEN: usize = 8;

    /// Decodes one edge entry, or `None` if `bytes` is not [`EdgeView::LEN`]
    /// bytes long.
    pub(crate) fn parse(bytes: &[u8]) -> Option<Self> {
        let &[f0, f1, t0, t1, i0, i1, _, _] = bytes else {
            return None;
        };
        Some(Self {
            from_ordinal: u16::from_le_bytes([f0, f1]),
            to_ordinal: u16::from_le_bytes([t0, t1]),
            input_index: u16::from_le_bytes([i0, i1]),
        })
    }

    /// The source stage ordinal.
    #[must_use]
    pub const fn from_ordinal(&self) -> u16 {
        self.from_ordinal
    }

    /// The target stage ordinal.
    #[must_use]
    pub const fn to_ordinal(&self) -> u16 {
        self.to_ordinal
    }

    /// The parameter position on the target stage.
    #[must_use]
    pub const fn input_index(&self) -> u16 {
        self.input_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_little_endian_fields() {
        let entry = [1, 0, 2, 0, 3, 0, 0, 0];
        let edge = EdgeView::parse(&entry);
        assert_eq!(
            edge.map(|e| (e.from_ordinal(), e.to_ordinal(), e.input_index())),
            Some((1, 2, 3))
        );
    }

    #[test]
    fn rejects_a_short_entry() {
        assert!(EdgeView::parse(&[0u8; 4]).is_none());
    }
}
