//! Node tag discriminants for the IR tagged union.

/// Discriminant for each record kind in the IR wire format.
///
/// Stored as a little-endian `u16` at the start of every record. The
/// tag space is open for additive evolution: a reader rejects an
/// unknown required tag and skips an unknown optional one using the
/// record's length prefix.
#[non_exhaustive]
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeTag {
    /// Root DAG spec record.
    ConductorSpec = 1,
    /// A single stage node.
    StageNode = 2,
    /// A directed edge between two stage ordinals.
    Edge = 3,
    /// Retry policy node.
    PolicyRetry = 4,
    /// Circuit-breaker policy node.
    PolicyBreaker = 5,
    /// Rate-limiter policy node.
    PolicyLimiter = 6,
    /// Timeout policy node.
    PolicyTimeout = 7,
    /// Registry name-to-ordinal entry.
    RegistryEntry = 8,
    /// External config binding descriptor.
    ConfigBinding = 9,
}

impl NodeTag {
    /// Decodes a raw discriminant into a known tag, or `None` if the value
    /// is not a recognized record kind.
    pub(crate) const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::ConductorSpec),
            2 => Some(Self::StageNode),
            3 => Some(Self::Edge),
            4 => Some(Self::PolicyRetry),
            5 => Some(Self::PolicyBreaker),
            6 => Some(Self::PolicyLimiter),
            7 => Some(Self::PolicyTimeout),
            8 => Some(Self::RegistryEntry),
            9 => Some(Self::ConfigBinding),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_u16_maps_known_tags() {
        let known: [(u16, NodeTag); 9] = [
            (1, NodeTag::ConductorSpec),
            (2, NodeTag::StageNode),
            (3, NodeTag::Edge),
            (4, NodeTag::PolicyRetry),
            (5, NodeTag::PolicyBreaker),
            (6, NodeTag::PolicyLimiter),
            (7, NodeTag::PolicyTimeout),
            (8, NodeTag::RegistryEntry),
            (9, NodeTag::ConfigBinding),
        ];
        for (raw, tag) in known {
            assert_eq!(NodeTag::from_u16(raw), Some(tag));
        }
    }

    #[test]
    fn from_u16_rejects_unknown_discriminants() {
        assert_eq!(NodeTag::from_u16(0), None);
        assert_eq!(NodeTag::from_u16(10), None);
        assert_eq!(NodeTag::from_u16(u16::MAX), None);
    }
}
