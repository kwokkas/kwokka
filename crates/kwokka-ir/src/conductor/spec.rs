//! The conductor DAG spec view: the root of a validated IR blob.

use crate::{
    conductor::{EdgeView, StageView, check},
    config::ConfigBindingView,
    error::IrError,
    policy::PolicyKind,
    registry::RegistryView,
};

/// Byte offset of the stage-count field within a `ConductorSpec` body.
pub(super) const STAGE_COUNT_FIELD: usize = 0;

/// Byte offset of the edge-count field within a `ConductorSpec` body.
pub(super) const EDGE_COUNT_FIELD: usize = 4;

/// Byte offset of the stage-table-offset field within a `ConductorSpec` body.
pub(super) const STAGE_TABLE_OFFSET_FIELD: usize = 8;

/// Byte offset of the edge-table-offset field within a `ConductorSpec` body.
pub(super) const EDGE_TABLE_OFFSET_FIELD: usize = 12;

/// Byte offset of the registry-table-offset field within a `ConductorSpec`
/// body (0 when the conductor carries no registry).
pub(super) const REGISTRY_TABLE_OFFSET_FIELD: usize = 16;

/// Byte offset of the config-table-offset field within a `ConductorSpec`
/// body (0 when the conductor carries no config bindings).
pub(super) const CONFIG_TABLE_OFFSET_FIELD: usize = 20;

/// Byte length of the fixed `ConductorSpec` body header (six u32 fields).
pub(super) const CONDUCTOR_HEADER_LEN: usize = 24;

/// Byte length of one stage-table entry: one u32 policy-record offset per
/// [`PolicyKind`] slot.
pub(super) const STAGE_ENTRY_LEN: usize = PolicyKind::COUNT * 4;

/// Byte length of the fixed config-section header: a `u32` binding count
/// and `u32` padding before the entry array.
pub(super) const CONFIG_HEADER_LEN: usize = 8;

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
    registry: Option<RegistryView<'a>>,
    config_count: u32,
    config_entries: &'a [u8],
}

impl<'a> ConductorView<'a> {
    /// Parses and bounds-checks a `ConductorSpec` record body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::OutOfBounds`] if a table offset aliases the
    /// header, a section overlaps another, or a structure extends past the
    /// body; [`IrError::Truncated`] if a count or offset field is out of
    /// range; [`IrError::OrdinalOutOfRange`] if an edge names a stage
    /// ordinal at or beyond `stage_count`; and [`IrError::BadTag`] if a
    /// policy slot points at a record whose tag does not match its kind.
    pub(crate) fn parse(body: &'a [u8]) -> Result<Self, IrError> {
        let checked = check::parse(body)?;
        Ok(Self {
            body: checked.body,
            stage_count: checked.stage_count,
            edge_count: checked.edge_count,
            stage_table: checked.stage_table,
            edge_table: checked.edge_table,
            registry: checked.registry,
            config_count: checked.config_count,
            config_entries: checked.config_entries,
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

    /// Returns the stage-name registry, or `None` if the conductor carries
    /// no registry.
    #[must_use]
    pub const fn registry(&self) -> Option<RegistryView<'a>> {
        self.registry
    }

    /// The number of config bindings in the DAG.
    #[must_use]
    pub const fn config_count(&self) -> u32 {
        self.config_count
    }

    /// Returns the config binding at `index`, or `None` if `index` is past
    /// the last binding.
    #[must_use]
    pub fn config_binding(&self, index: u32) -> Option<ConfigBindingView<'a>> {
        let offset = (index as usize).checked_mul(ConfigBindingView::LEN)?;
        let end = offset.checked_add(ConfigBindingView::LEN)?;
        let entry = self.config_entries.get(offset..end)?;
        ConfigBindingView::parse(self.body, entry).ok()
    }

    /// Iterates the config bindings in wire order.
    pub fn config_bindings(&self) -> impl Iterator<Item = ConfigBindingView<'a>> + '_ {
        // config_binding(index) returns None only past config_count or for a
        // malformed entry; parse already validated every entry, so the bounded
        // range never drops a binding.
        (0..self.config_count).filter_map(|index| self.config_binding(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeTag;

    fn no_policy_body() -> [u8; 64] {
        let mut body = [0u8; 64];
        body[0..4].copy_from_slice(&2u32.to_le_bytes());
        body[4..8].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&56u32.to_le_bytes());
        body[56..58].copy_from_slice(&0u16.to_le_bytes());
        body[58..60].copy_from_slice(&1u16.to_le_bytes());
        body
    }

    fn timeout_body() -> [u8; 56] {
        let mut body = [0u8; 56];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&40u32.to_le_bytes());
        body[28..32].copy_from_slice(&40u32.to_le_bytes());
        body[40..42].copy_from_slice(&(NodeTag::PolicyTimeout as u16).to_le_bytes());
        body[44..48].copy_from_slice(&16u32.to_le_bytes());
        body[48..56].copy_from_slice(&5_000u64.to_le_bytes());
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
        body[40..42].copy_from_slice(&(NodeTag::PolicyRetry as u16).to_le_bytes());
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
        body[58..60].copy_from_slice(&5u16.to_le_bytes());
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

    #[test]
    fn registry_and_config_absent() {
        let body = no_policy_body();
        let view = ConductorView::parse(&body);
        assert!(matches!(
            view,
            Ok(spec) if spec.registry().is_none() && spec.config_count() == 0
        ));
    }

    #[test]
    fn rejects_a_header_slot() {
        let mut body = timeout_body();
        body[28..32].copy_from_slice(&8u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_overlapping_tables() {
        let mut body = no_policy_body();
        body[12..16].copy_from_slice(&24u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    fn record_in_table_body() -> [u8; 56] {
        let mut body = [0u8; 56];
        body[0..4].copy_from_slice(&2u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&56u32.to_le_bytes());
        body[28..32].copy_from_slice(&40u32.to_le_bytes());
        body[40..42].copy_from_slice(&(NodeTag::PolicyTimeout as u16).to_le_bytes());
        body[44..48].copy_from_slice(&16u32.to_le_bytes());
        body[48..56].copy_from_slice(&5_000u64.to_le_bytes());
        body
    }

    #[test]
    fn rejects_a_record_table_overlap() {
        let body = record_in_table_body();
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    fn shared_record_body() -> [u8; 72] {
        let mut body = [0u8; 72];
        body[0..4].copy_from_slice(&2u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&56u32.to_le_bytes());
        body[28..32].copy_from_slice(&56u32.to_le_bytes());
        body[44..48].copy_from_slice(&56u32.to_le_bytes());
        body[56..58].copy_from_slice(&(NodeTag::PolicyTimeout as u16).to_le_bytes());
        body[60..64].copy_from_slice(&16u32.to_le_bytes());
        body[64..72].copy_from_slice(&5_000u64.to_le_bytes());
        body
    }

    #[test]
    fn rejects_a_shared_record() {
        let body = shared_record_body();
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_a_config_header_alias() {
        let mut body = [0u8; 40];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&40u32.to_le_bytes());
        body[20..24].copy_from_slice(&8u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_a_name_ref_overrun() {
        let mut body = [0u8; 50];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&40u32.to_le_bytes());
        body[16..20].copy_from_slice(&40u32.to_le_bytes());
        body[44..48].copy_from_slice(&999u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    /// A valid 2-stage registry: ord 0 = "a", ord 1 = "b", names in the heap
    /// at offsets 76/77, sorted index `[0, 1]`.
    fn registry_body() -> [u8; 80] {
        let mut body = [0u8; 80];
        body[0..4].copy_from_slice(&2u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&56u32.to_le_bytes());
        body[16..20].copy_from_slice(&56u32.to_le_bytes());
        body[56..60].copy_from_slice(&76u32.to_le_bytes());
        body[60..64].copy_from_slice(&1u32.to_le_bytes());
        body[64..68].copy_from_slice(&77u32.to_le_bytes());
        body[68..72].copy_from_slice(&1u32.to_le_bytes());
        body[72..74].copy_from_slice(&0u16.to_le_bytes());
        body[74..76].copy_from_slice(&1u16.to_le_bytes());
        body[76] = b'a';
        body[77] = b'b';
        body
    }

    #[test]
    fn reads_a_registry() {
        let body = registry_body();
        let found = ConductorView::parse(&body).ok().and_then(|view| {
            let registry = view.registry()?;
            Some((registry.name(0)?, registry.lookup(b"b")?))
        });
        assert_eq!(found, Some((&b"a"[..], 1)));
    }

    #[test]
    fn rejects_a_name_below_heap() {
        let mut body = registry_body();
        body[56..60].copy_from_slice(&24u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_an_unsorted_registry() {
        let mut body = registry_body();
        body[72..74].copy_from_slice(&1u16.to_le_bytes());
        body[74..76].copy_from_slice(&0u16.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::RegistryUnsorted));
    }

    /// A valid 1-stage config: one binding with key "k" in the heap at
    /// offset 80, default `ScalarValue` zero.
    fn config_body() -> [u8; 88] {
        let mut body = [0u8; 88];
        body[0..4].copy_from_slice(&1u32.to_le_bytes());
        body[8..12].copy_from_slice(&24u32.to_le_bytes());
        body[12..16].copy_from_slice(&40u32.to_le_bytes());
        body[20..24].copy_from_slice(&40u32.to_le_bytes());
        body[40..44].copy_from_slice(&1u32.to_le_bytes());
        body[50..52].copy_from_slice(&ConfigBindingView::FIELD_TIMEOUT_DURATION_NS.to_le_bytes());
        body[52..56].copy_from_slice(&1u32.to_le_bytes());
        body[56..60].copy_from_slice(&80u32.to_le_bytes());
        body[80] = b'k';
        body
    }

    #[test]
    fn reads_a_config() {
        let body = config_body();
        let key = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.config_binding(0))
            .map(|binding| binding.key());
        assert_eq!(key, Some(&b"k"[..]));
    }

    #[test]
    fn rejects_a_key_below_heap() {
        let mut body = config_body();
        body[56..60].copy_from_slice(&24u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }
}
