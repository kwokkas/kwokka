//! The conductor DAG spec view: the root of a validated IR blob.

use crate::{
    conductor::{EdgeView, StageView},
    error::IrError,
    flat::reader::{read_record, read_u32},
    policy::PolicyKind,
};

/// Byte offset of the stage-count field within a `ConductorSpec` body.
const STAGE_COUNT_FIELD: usize = 0;

/// Byte offset of the edge-count field within a `ConductorSpec` body.
const EDGE_COUNT_FIELD: usize = 4;

/// Byte offset of the stage-table-offset field within a `ConductorSpec` body.
const STAGE_TABLE_OFFSET_FIELD: usize = 8;

/// Byte offset of the edge-table-offset field within a `ConductorSpec` body.
const EDGE_TABLE_OFFSET_FIELD: usize = 12;

/// Byte length of the fixed `ConductorSpec` body header (four u32 fields).
const CONDUCTOR_HEADER_LEN: usize = 16;

/// Byte length of one stage-table entry: one u32 policy-record offset per
/// [`PolicyKind`] slot.
const STAGE_ENTRY_LEN: usize = PolicyKind::COUNT * 4;

/// The conductor DAG spec: the stage table and the edge table.
///
/// Obtained from [`KwokkaIr::conductor`]. Each stage is addressed by
/// ordinal via [`ConductorView::stage`]; edges connect stage ordinals.
/// Graph well-formedness beyond ordinal bounds and policy framing
/// (cycles, topological order, edge arity) is the consumer's
/// responsibility.
///
/// [`KwokkaIr::conductor`]: crate::KwokkaIr::conductor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConductorView<'a> {
    body: &'a [u8],
    stage_count: u32,
    edge_count: u32,
    stage_table: &'a [u8],
    edge_table: &'a [u8],
}

impl<'a> ConductorView<'a> {
    /// Parses and bounds-checks a `ConductorSpec` record body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::OutOfBounds`] if a table offset aliases the
    /// header or extends past the body, [`IrError::Truncated`] if a count
    /// or offset field is out of range, [`IrError::OrdinalOutOfRange`] if
    /// an edge names a stage ordinal at or beyond `stage_count`, and
    /// [`IrError::BadTag`] if a policy slot points at a record whose tag
    /// does not match its slot kind.
    pub(crate) fn parse(body: &'a [u8]) -> Result<Self, IrError> {
        let stage_count = read_u32(body, STAGE_COUNT_FIELD)?;
        let edge_count = read_u32(body, EDGE_COUNT_FIELD)?;
        let stage_table =
            slice_table(body, STAGE_TABLE_OFFSET_FIELD, stage_count, STAGE_ENTRY_LEN)?;
        let edge_table = slice_table(body, EDGE_TABLE_OFFSET_FIELD, edge_count, EdgeView::LEN)?;
        check_edge_ordinals(edge_table, stage_count)?;
        check_stage_policies(body, stage_table)?;
        Ok(Self {
            body,
            stage_count,
            edge_count,
            stage_table,
            edge_table,
        })
    }

    /// The number of stages; valid ordinals are `0..stage_count`.
    #[must_use]
    pub const fn stage_count(&self) -> u32 {
        self.stage_count
    }

    /// The number of edges in the DAG.
    #[must_use]
    pub const fn edge_count(&self) -> u32 {
        self.edge_count
    }

    /// Returns the stage at `ordinal`, or `None` if `ordinal` is at or
    /// beyond [`ConductorView::stage_count`].
    #[must_use]
    pub fn stage(&self, ordinal: u32) -> Option<StageView<'a>> {
        if ordinal >= self.stage_count {
            return None;
        }
        let offset = (ordinal as usize).checked_mul(STAGE_ENTRY_LEN)?;
        let end = offset.checked_add(STAGE_ENTRY_LEN)?;
        let entry = self.stage_table.get(offset..end)?;
        Some(StageView::new(self.body, entry))
    }

    /// Returns the edge at `index`, or `None` if `index` is past the last
    /// edge.
    #[must_use]
    pub fn edge(&self, index: u32) -> Option<EdgeView> {
        let offset = (index as usize).checked_mul(EdgeView::LEN)?;
        let entry = self
            .edge_table
            .get(offset..offset.checked_add(EdgeView::LEN)?)?;
        EdgeView::parse(entry)
    }

    /// Iterates the DAG edges in wire order.
    pub fn edges(&self) -> impl Iterator<Item = EdgeView> + '_ {
        // edge(index) returns None only for index >= edge_count; the range
        // is bounded to 0..edge_count, so filter_map never drops an edge.
        (0..self.edge_count).filter_map(|index| self.edge(index))
    }
}

/// Slices a fixed-stride table from a `ConductorSpec` body.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if the offset aliases the header or
/// the table extends past the body, and [`IrError::Truncated`] if the
/// offset field is out of range.
fn slice_table(
    body: &[u8],
    offset_field: usize,
    count: u32,
    entry_len: usize,
) -> Result<&[u8], IrError> {
    let table_offset = read_u32(body, offset_field)? as usize;
    if table_offset < CONDUCTOR_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    let span = (count as usize)
        .checked_mul(entry_len)
        .ok_or(IrError::OutOfBounds)?;
    let end = table_offset.checked_add(span).ok_or(IrError::OutOfBounds)?;
    body.get(table_offset..end).ok_or(IrError::OutOfBounds)
}

/// Checks that every edge names stage ordinals within `stage_count`.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry and
/// [`IrError::OrdinalOutOfRange`] if an ordinal is at or beyond
/// `stage_count`.
fn check_edge_ordinals(edge_table: &[u8], stage_count: u32) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < edge_table.len() {
        let entry_end = offset
            .checked_add(EdgeView::LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = edge_table
            .get(offset..entry_end)
            .ok_or(IrError::OutOfBounds)?;
        let edge = EdgeView::parse(entry).ok_or(IrError::OutOfBounds)?;
        if u32::from(edge.from_ordinal()) >= stage_count
            || u32::from(edge.to_ordinal()) >= stage_count
        {
            return Err(IrError::OrdinalOutOfRange);
        }
        offset += EdgeView::LEN;
    }
    Ok(())
}

/// Checks that every non-empty policy slot points at a well-framed record
/// whose tag and body match the slot kind.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry, plus the
/// record-framing or slot-kind [`IrError::BadTag`] variants.
fn check_stage_policies(body: &[u8], stage_table: &[u8]) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < stage_table.len() {
        let entry_end = offset
            .checked_add(STAGE_ENTRY_LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = stage_table
            .get(offset..entry_end)
            .ok_or(IrError::OutOfBounds)?;
        for kind in PolicyKind::ALL {
            check_policy_slot(body, entry, kind)?;
        }
        offset += STAGE_ENTRY_LEN;
    }
    Ok(())
}

/// Checks a single policy slot within a stage-table entry.
///
/// # Errors
///
/// Returns [`IrError::BadTag`] if the slot record's tag does not match
/// `kind`, plus the record-framing and body-length variants.
fn check_policy_slot(body: &[u8], entry: &[u8], kind: PolicyKind) -> Result<(), IrError> {
    let slot_offset = read_u32(entry, kind.slot_field())? as usize;
    if slot_offset == 0 {
        return Ok(());
    }
    if slot_offset < CONDUCTOR_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    let record = read_record(body, slot_offset)?;
    if record.tag != kind.node_tag() {
        return Err(IrError::BadTag {
            tag: record.tag as u16,
        });
    }
    kind.validate_body(record.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeTag;

    fn no_policy_body() -> [u8; 56] {
        let mut body = [0u8; 56];
        body[0..4].copy_from_slice(&2u32.to_le_bytes());
        body[4..8].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&16u32.to_le_bytes());
        body[12..16].copy_from_slice(&48u32.to_le_bytes());
        body[48..50].copy_from_slice(&0u16.to_le_bytes());
        body[50..52].copy_from_slice(&1u16.to_le_bytes());
        body
    }

    fn timeout_body() -> [u8; 48] {
        let mut body = [0u8; 48];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&16u32.to_le_bytes());
        body[12..16].copy_from_slice(&32u32.to_le_bytes());
        body[20..24].copy_from_slice(&32u32.to_le_bytes());
        body[32..34].copy_from_slice(&(NodeTag::PolicyTimeout as u16).to_le_bytes());
        body[36..40].copy_from_slice(&16u32.to_le_bytes());
        body[40..48].copy_from_slice(&5_000u64.to_le_bytes());
        body
    }

    #[test]
    fn parses_counts_and_edges() {
        let body = no_policy_body();
        assert!(matches!(
            ConductorView::parse(&body),
            Ok(view) if view.stage_count() == 2 && view.edge_count() == 1
        ));
    }

    #[test]
    fn stage_past_count_is_none() {
        let body = no_policy_body();
        let stage = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.stage(2));
        assert!(stage.is_none());
    }

    #[test]
    fn empty_slots_yield_no_policy() {
        let body = no_policy_body();
        let stage = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.stage(0));
        let has_any = stage.map(|s| {
            s.limiter().is_some()
                || s.timeout().is_some()
                || s.retry().is_some()
                || s.breaker().is_some()
        });
        assert_eq!(has_any, Some(false));
    }

    #[test]
    fn reads_a_stage_timeout_policy() {
        let body = timeout_body();
        let timeout = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.stage(0))
            .and_then(|stage| stage.timeout());
        assert_eq!(timeout.map(|policy| policy.duration_ns()), Some(5_000));
    }

    #[test]
    fn rejects_a_slot_tag_mismatch() {
        let mut body = timeout_body();
        body[32..34].copy_from_slice(&(NodeTag::PolicyRetry as u16).to_le_bytes());
        assert_eq!(
            ConductorView::parse(&body),
            Err(IrError::BadTag {
                tag: NodeTag::PolicyRetry as u16,
            })
        );
    }

    #[test]
    fn rejects_an_ordinal_overrun() {
        let mut body = no_policy_body();
        body[50..52].copy_from_slice(&5u16.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OrdinalOutOfRange));
    }

    #[test]
    fn rejects_a_truncated_body() {
        assert_eq!(ConductorView::parse(&[0u8; 8]), Err(IrError::Truncated));
    }

    #[test]
    fn reads_edge_fields() {
        let body = no_policy_body();
        let edge = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.edge(0));
        assert_eq!(
            edge.map(|e| (e.from_ordinal(), e.to_ordinal())),
            Some((0, 1))
        );
    }

    #[test]
    fn edges_iterates_every_edge() {
        let body = no_policy_body();
        assert_eq!(
            ConductorView::parse(&body).map(|view| view.edges().count()),
            Ok(1)
        );
    }

    #[test]
    fn edge_past_count_is_none() {
        let body = no_policy_body();
        let edge = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.edge(9));
        assert!(edge.is_none());
    }

    #[test]
    fn rejects_an_aliased_stage_table() {
        let mut body = no_policy_body();
        body[8..12].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    fn all_policy_body() -> [u8; 144] {
        let mut body = [0u8; 144];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&16u32.to_le_bytes());
        body[12..16].copy_from_slice(&144u32.to_le_bytes());
        body[16..20].copy_from_slice(&32u32.to_le_bytes());
        body[20..24].copy_from_slice(&56u32.to_le_bytes());
        body[24..28].copy_from_slice(&72u32.to_le_bytes());
        body[28..32].copy_from_slice(&104u32.to_le_bytes());
        body[32..34].copy_from_slice(&(NodeTag::PolicyLimiter as u16).to_le_bytes());
        body[36..40].copy_from_slice(&24u32.to_le_bytes());
        body[40..44].copy_from_slice(&100u32.to_le_bytes());
        body[44..48].copy_from_slice(&10u32.to_le_bytes());
        body[48..56].copy_from_slice(&1_000u64.to_le_bytes());
        body[56..58].copy_from_slice(&(NodeTag::PolicyTimeout as u16).to_le_bytes());
        body[60..64].copy_from_slice(&16u32.to_le_bytes());
        body[64..72].copy_from_slice(&5_000u64.to_le_bytes());
        body[72..74].copy_from_slice(&(NodeTag::PolicyRetry as u16).to_le_bytes());
        body[76..80].copy_from_slice(&32u32.to_le_bytes());
        body[80..84].copy_from_slice(&5u32.to_le_bytes());
        body[84] = 2;
        body[85] = 1;
        body[88..96].copy_from_slice(&1_000u64.to_le_bytes());
        body[96..104].copy_from_slice(&60_000u64.to_le_bytes());
        body[104..106].copy_from_slice(&(NodeTag::PolicyBreaker as u16).to_le_bytes());
        body[108..112].copy_from_slice(&40u32.to_le_bytes());
        body[112] = 1;
        body[113] = 50;
        body[116..120].copy_from_slice(&20u32.to_le_bytes());
        body[120..128].copy_from_slice(&10_000u64.to_le_bytes());
        body[128..132].copy_from_slice(&3u32.to_le_bytes());
        body[132..136].copy_from_slice(&2u32.to_le_bytes());
        body[136..144].copy_from_slice(&30_000u64.to_le_bytes());
        body
    }

    #[test]
    fn reads_every_stage_policy() {
        let body = all_policy_body();
        let stage = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.stage(0));
        let got = stage.map(|s| {
            (
                s.limiter().map(|policy| policy.capacity()),
                s.timeout().map(|policy| policy.duration_ns()),
                s.retry().map(|policy| policy.max_attempts()),
                s.breaker().map(|policy| policy.failure_rate_percent()),
            )
        });
        assert_eq!(got, Some((Some(100), Some(5_000), Some(5), Some(50))));
    }

    #[test]
    fn rejects_a_header_slot() {
        let mut body = timeout_body();
        body[20..24].copy_from_slice(&8u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }
}
