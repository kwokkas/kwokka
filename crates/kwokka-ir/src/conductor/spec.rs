//! The conductor DAG spec view: the root of a validated IR blob.

use crate::{
    conductor::{EdgeView, StageView},
    config::ConfigBindingView,
    error::IrError,
    flat::reader::{RECORD_HEADER_LEN, read_record, read_u32},
    policy::PolicyKind,
    registry::RegistryView,
};

/// Byte offset of the stage-count field within a `ConductorSpec` body.
const STAGE_COUNT_FIELD: usize = 0;

/// Byte offset of the edge-count field within a `ConductorSpec` body.
const EDGE_COUNT_FIELD: usize = 4;

/// Byte offset of the stage-table-offset field within a `ConductorSpec` body.
const STAGE_TABLE_OFFSET_FIELD: usize = 8;

/// Byte offset of the edge-table-offset field within a `ConductorSpec` body.
const EDGE_TABLE_OFFSET_FIELD: usize = 12;

/// Byte offset of the registry-table-offset field within a `ConductorSpec`
/// body (0 when the conductor carries no registry).
const REGISTRY_TABLE_OFFSET_FIELD: usize = 16;

/// Byte offset of the config-table-offset field within a `ConductorSpec`
/// body (0 when the conductor carries no config bindings).
const CONFIG_TABLE_OFFSET_FIELD: usize = 20;

/// Byte length of the fixed `ConductorSpec` body header (six u32 fields).
const CONDUCTOR_HEADER_LEN: usize = 24;

/// Byte length of one stage-table entry: one u32 policy-record offset per
/// [`PolicyKind`] slot.
const STAGE_ENTRY_LEN: usize = PolicyKind::COUNT * 4;

/// Byte length of the fixed config-section header: a `u32` binding count
/// and `u32` padding before the entry array.
const CONFIG_HEADER_LEN: usize = 8;

/// A byte range within the conductor body, checked for cross-section overlap.
#[derive(Clone, Copy)]
struct Span {
    offset: usize,
    len: usize,
}

impl Span {
    /// An empty span; pads the fixed section array for the unused slots.
    const ZERO: Self = Self { offset: 0, len: 0 };

    /// A span starting at `offset` covering `len` bytes.
    const fn new(offset: usize, len: usize) -> Self {
        Self { offset, len }
    }
}

/// Reports whether two byte ranges overlap.
const fn ranges_overlap(left: Span, right: Span) -> bool {
    left.offset < right.offset + right.len && right.offset < left.offset + left.len
}

/// Reports whether `span` overlaps any of `sections`.
fn overlaps_any(span: Span, sections: &[Span]) -> bool {
    sections
        .iter()
        .any(|section| ranges_overlap(span, *section))
}

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
        let stage_count = read_u32(body, STAGE_COUNT_FIELD)?;
        let edge_count = read_u32(body, EDGE_COUNT_FIELD)?;
        let (stage_off, stage_table) =
            slice_table(body, STAGE_TABLE_OFFSET_FIELD, stage_count, STAGE_ENTRY_LEN)?;
        let (edge_off, edge_table) =
            slice_table(body, EDGE_TABLE_OFFSET_FIELD, edge_count, EdgeView::LEN)?;
        let registry = parse_registry(body, stage_count)?;
        let config = parse_config(body, stage_count)?;

        let mut spans = [Span::ZERO; 5];
        spans[0] = Span::new(stage_off, stage_table.len());
        spans[1] = Span::new(edge_off, edge_table.len());
        let mut count = 2;
        if let Some(reg) = &registry {
            spans[count] = reg.name_span;
            spans[count + 1] = reg.sorted_span;
            count += 2;
        }
        if let Some(cfg) = &config {
            spans[count] = cfg.section_span;
            count += 1;
        }
        let sections = &spans[..count];

        check_sections_disjoint(sections)?;
        check_edge_ordinals(edge_table, stage_count)?;
        check_stage_policies(body, stage_table, sections)?;
        check_record_overlaps(body, stage_table)?;

        Ok(Self {
            body,
            stage_count,
            edge_count,
            stage_table,
            edge_table,
            registry: registry.map(|reg| reg.view),
            config_count: config.as_ref().map_or(0, |cfg| cfg.count),
            config_entries: config.map_or(&[][..], |cfg| cfg.entries),
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
) -> Result<(usize, &[u8]), IrError> {
    let table_offset = read_u32(body, offset_field)? as usize;
    if table_offset < CONDUCTOR_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    let span = (count as usize)
        .checked_mul(entry_len)
        .ok_or(IrError::OutOfBounds)?;
    let end = table_offset.checked_add(span).ok_or(IrError::OutOfBounds)?;
    let slice = body.get(table_offset..end).ok_or(IrError::OutOfBounds)?;
    Ok((table_offset, slice))
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
fn check_stage_policies(body: &[u8], stage_table: &[u8], sections: &[Span]) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < stage_table.len() {
        let entry_end = offset
            .checked_add(STAGE_ENTRY_LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = stage_table
            .get(offset..entry_end)
            .ok_or(IrError::OutOfBounds)?;
        for kind in PolicyKind::ALL {
            check_policy_slot(body, entry, kind, sections)?;
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
fn check_policy_slot(
    body: &[u8],
    entry: &[u8],
    kind: PolicyKind,
    sections: &[Span],
) -> Result<(), IrError> {
    let Some(record_span) = record_span_at(body, entry, kind)? else {
        return Ok(());
    };
    let record = read_record(body, record_span.offset)?;
    if record.tag != kind.node_tag() {
        return Err(IrError::BadTag {
            tag: record.tag as u16,
        });
    }
    kind.validate_body(record.body)?;
    if overlaps_any(record_span, sections) {
        return Err(IrError::OutOfBounds);
    }
    Ok(())
}

/// The byte span of the policy record a slot points at, or `None` when the
/// slot is empty.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if the slot offset aliases the header,
/// plus the record-framing variants.
fn record_span_at(body: &[u8], entry: &[u8], kind: PolicyKind) -> Result<Option<Span>, IrError> {
    let slot_offset = read_u32(entry, kind.slot_field())? as usize;
    if slot_offset == 0 {
        return Ok(None);
    }
    if slot_offset < CONDUCTOR_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    let record = read_record(body, slot_offset)?;
    let len = RECORD_HEADER_LEN
        .checked_add(record.body.len())
        .ok_or(IrError::OutOfBounds)?;
    Ok(Some(Span {
        offset: slot_offset,
        len,
    }))
}

/// The span of the `n`-th policy record across all stage slots in
/// `[stage, kind]` order, or `None` when that slot is empty.
fn nth_record_span(body: &[u8], stage_table: &[u8], n: usize) -> Result<Option<Span>, IrError> {
    let offset = (n / PolicyKind::COUNT)
        .checked_mul(STAGE_ENTRY_LEN)
        .ok_or(IrError::OutOfBounds)?;
    let end = offset
        .checked_add(STAGE_ENTRY_LEN)
        .ok_or(IrError::OutOfBounds)?;
    let entry = stage_table.get(offset..end).ok_or(IrError::OutOfBounds)?;
    let kind = PolicyKind::ALL[n % PolicyKind::COUNT];
    record_span_at(body, entry, kind)
}

/// Rejects two policy records whose byte spans overlap.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if any two records intersect, plus the
/// record-framing variants.
fn check_record_overlaps(body: &[u8], stage_table: &[u8]) -> Result<(), IrError> {
    let total = (stage_table.len() / STAGE_ENTRY_LEN)
        .checked_mul(PolicyKind::COUNT)
        .ok_or(IrError::OutOfBounds)?;
    for a in 0..total {
        let Some(span_a) = nth_record_span(body, stage_table, a)? else {
            continue;
        };
        for b in (a + 1)..total {
            let Some(span_b) = nth_record_span(body, stage_table, b)? else {
                continue;
            };
            if ranges_overlap(span_a, span_b) {
                return Err(IrError::OutOfBounds);
            }
        }
    }
    Ok(())
}

/// A validated registry section: the view plus its two table spans.
struct ParsedRegistry<'a> {
    view: RegistryView<'a>,
    name_span: Span,
    sorted_span: Span,
}

/// Parses the optional registry section, or `None` when the conductor
/// carries no registry.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if a table offset aliases the header or
/// runs past the body, [`IrError::Truncated`] if the offset field is out of
/// range, and [`IrError::OrdinalOutOfRange`] if a sorted entry names a stage
/// at or beyond `stage_count`.
fn parse_registry(body: &[u8], stage_count: u32) -> Result<Option<ParsedRegistry<'_>>, IrError> {
    let registry_offset = read_u32(body, REGISTRY_TABLE_OFFSET_FIELD)? as usize;
    if registry_offset == 0 {
        return Ok(None);
    }
    let (name_offset, name_table) = slice_table(
        body,
        REGISTRY_TABLE_OFFSET_FIELD,
        stage_count,
        RegistryView::NAME_ENTRY_LEN,
    )?;
    let sorted_offset = name_offset
        .checked_add(name_table.len())
        .ok_or(IrError::OutOfBounds)?;
    let sorted_len = (stage_count as usize)
        .checked_mul(RegistryView::SORTED_ENTRY_LEN)
        .ok_or(IrError::OutOfBounds)?;
    let sorted_end = sorted_offset
        .checked_add(sorted_len)
        .ok_or(IrError::OutOfBounds)?;
    let sorted_index = body
        .get(sorted_offset..sorted_end)
        .ok_or(IrError::OutOfBounds)?;
    let view = RegistryView::new(body, name_table, sorted_index, stage_count);
    check_registry_names(&view, stage_count)?;
    check_sorted_ordinals(sorted_index, stage_count)?;
    Ok(Some(ParsedRegistry {
        view,
        name_span: Span::new(name_offset, name_table.len()),
        sorted_span: Span::new(sorted_offset, sorted_index.len()),
    }))
}

/// Checks that every ordinal-to-name entry resolves within the blob.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if a name string-ref runs past the body.
fn check_registry_names(view: &RegistryView, stage_count: u32) -> Result<(), IrError> {
    for ordinal in 0..stage_count {
        if view.name(ordinal).is_none() {
            return Err(IrError::OutOfBounds);
        }
    }
    Ok(())
}

/// Checks that every sorted-index entry names a stage within `stage_count`.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry and
/// [`IrError::OrdinalOutOfRange`] if an ordinal is at or beyond
/// `stage_count`.
fn check_sorted_ordinals(sorted_index: &[u8], stage_count: u32) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < sorted_index.len() {
        let end = offset
            .checked_add(RegistryView::SORTED_ENTRY_LEN)
            .ok_or(IrError::OutOfBounds)?;
        let &[a, b] = sorted_index.get(offset..end).ok_or(IrError::OutOfBounds)? else {
            return Err(IrError::OutOfBounds);
        };
        if u32::from(u16::from_le_bytes([a, b])) >= stage_count {
            return Err(IrError::OrdinalOutOfRange);
        }
        offset += RegistryView::SORTED_ENTRY_LEN;
    }
    Ok(())
}

/// A validated config section: the binding count, the entry slice, and the
/// section span.
struct ParsedConfig<'a> {
    count: u32,
    entries: &'a [u8],
    section_span: Span,
}

/// Parses the optional config section, or `None` when the conductor carries
/// no config bindings.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if the section offset aliases the header
/// or the entry table runs past the body, [`IrError::Truncated`] if a field
/// is out of range, and [`IrError::OrdinalOutOfRange`] if a binding names a
/// stage at or beyond `stage_count`.
fn parse_config(body: &[u8], stage_count: u32) -> Result<Option<ParsedConfig<'_>>, IrError> {
    let config_offset = read_u32(body, CONFIG_TABLE_OFFSET_FIELD)? as usize;
    if config_offset == 0 {
        return Ok(None);
    }
    if config_offset < CONDUCTOR_HEADER_LEN {
        return Err(IrError::OutOfBounds);
    }
    let count = read_u32(body, config_offset)?;
    let entries_offset = config_offset
        .checked_add(CONFIG_HEADER_LEN)
        .ok_or(IrError::OutOfBounds)?;
    let entries_len = (count as usize)
        .checked_mul(ConfigBindingView::LEN)
        .ok_or(IrError::OutOfBounds)?;
    let entries_end = entries_offset
        .checked_add(entries_len)
        .ok_or(IrError::OutOfBounds)?;
    let entries = body
        .get(entries_offset..entries_end)
        .ok_or(IrError::OutOfBounds)?;
    check_config_entries(body, entries, stage_count)?;
    let section_len = CONFIG_HEADER_LEN
        .checked_add(entries_len)
        .ok_or(IrError::OutOfBounds)?;
    Ok(Some(ParsedConfig {
        count,
        entries,
        section_span: Span::new(config_offset, section_len),
    }))
}

/// Checks that every config entry decodes and names a stage within
/// `stage_count`.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry or key string-ref,
/// [`IrError::Truncated`] on a short entry, and [`IrError::OrdinalOutOfRange`]
/// if a binding names a stage at or beyond `stage_count`.
fn check_config_entries(body: &[u8], entries: &[u8], stage_count: u32) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < entries.len() {
        let end = offset
            .checked_add(ConfigBindingView::LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = entries.get(offset..end).ok_or(IrError::OutOfBounds)?;
        let binding = ConfigBindingView::parse(body, entry)?;
        if u32::from(binding.node_ordinal()) >= stage_count {
            return Err(IrError::OrdinalOutOfRange);
        }
        offset += ConfigBindingView::LEN;
    }
    Ok(())
}

/// Rejects any two body sections whose byte spans overlap.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if any pair of sections intersects.
fn check_sections_disjoint(sections: &[Span]) -> Result<(), IrError> {
    for (index, &section) in sections.iter().enumerate() {
        for &other in &sections[index + 1..] {
            if ranges_overlap(section, other) {
                return Err(IrError::OutOfBounds);
            }
        }
    }
    Ok(())
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
}
