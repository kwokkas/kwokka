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
