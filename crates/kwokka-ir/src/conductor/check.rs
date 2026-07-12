//! Bounds-checking the body of an untrusted `ConductorSpec` record.
//!
//! Wire bytes arrive untrusted, so every offset, table, ordinal and record
//! frame is checked before [`ConductorView`](super::ConductorView) reads a
//! single field. What survives is a [`Parsed`], and the view is a thin
//! projection of it -- so an accessor never has to ask whether its bytes are
//! in range.
//!
//! DAG well-formedness (cycles, topological order, edge arity) is NOT checked
//! here; that is the consumer's job. This module guarantees framing only.

use crate::{
    conductor::{
        EdgeView,
        spec::{
            CONDUCTOR_HEADER_LEN, CONFIG_HEADER_LEN, CONFIG_TABLE_OFFSET_FIELD, EDGE_COUNT_FIELD,
            EDGE_TABLE_OFFSET_FIELD, REGISTRY_TABLE_OFFSET_FIELD, STAGE_COUNT_FIELD,
            STAGE_ENTRY_LEN, STAGE_TABLE_OFFSET_FIELD,
        },
    },
    config::ConfigBindingView,
    error::IrError,
    flat::reader::{RECORD_HEADER_LEN, read_record, read_u32},
    policy::PolicyKind,
    registry::RegistryView,
};

/// A `ConductorSpec` body that has passed every framing check.
pub(super) struct Parsed<'a> {
    /// The record body the checked spans point into.
    pub(super) body: &'a [u8],
    /// Number of stages; the upper bound on every ordinal.
    pub(super) stage_count: u32,
    /// Number of edges in the edge table.
    pub(super) edge_count: u32,
    /// Fixed-stride stage table, one entry per stage.
    pub(super) stage_table: &'a [u8],
    /// Fixed-stride edge table, one entry per edge.
    pub(super) edge_table: &'a [u8],
    /// The stage-name registry, when the blob carries one.
    pub(super) registry: Option<RegistryView<'a>>,
    /// Number of config bindings.
    pub(super) config_count: u32,
    /// Fixed-stride config-binding entries.
    pub(super) config_entries: &'a [u8],
}

/// Bounds-checks a `ConductorSpec` record body end to end.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if a table offset aliases the header, a
/// section overlaps another, or a structure extends past the body;
/// [`IrError::Truncated`] if a count or offset field is out of range;
/// [`IrError::OrdinalOutOfRange`] if an edge names a stage ordinal at or
/// beyond `stage_count`; and [`IrError::BadTag`] if a policy slot points at a
/// record whose tag does not match its kind.
pub(super) fn parse(body: &[u8]) -> Result<Parsed<'_>, IrError> {
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

    let floor = heap_floor(sections, body, stage_table)?;
    if let Some(reg) = &registry {
        check_names_in_heap(reg.name_table, floor)?;
        check_sorted_order(&reg.view, stage_count)?;
    }
    if let Some(cfg) = &config {
        check_keys_in_heap(cfg.entries, floor)?;
    }

    Ok(Parsed {
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

/// A validated registry section: the view, its two table spans, and the
/// name table for the string-heap containment check.
struct ParsedRegistry<'a> {
    view: RegistryView<'a>,
    name_table: &'a [u8],
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
        name_table,
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

/// The body offset where the trailing string heap begins: past every
/// structural section and policy record. Registry names and config keys
/// must start at or beyond this floor to stay inside the heap.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] if a policy-record span is malformed.
fn heap_floor(sections: &[Span], body: &[u8], stage_table: &[u8]) -> Result<usize, IrError> {
    let mut floor = 0;
    for section in sections {
        floor = floor.max(section.offset + section.len);
    }
    let total = (stage_table.len() / STAGE_ENTRY_LEN)
        .checked_mul(PolicyKind::COUNT)
        .ok_or(IrError::OutOfBounds)?;
    for n in 0..total {
        if let Some(span) = nth_record_span(body, stage_table, n)? {
            floor = floor.max(span.offset + span.len);
        }
    }
    Ok(floor)
}

/// Checks that every registry name string-ref starts at or beyond `floor`,
/// keeping names inside the trailing heap rather than over a section.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry or a name that
/// starts before the heap floor.
fn check_names_in_heap(name_table: &[u8], floor: usize) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < name_table.len() {
        let end = offset
            .checked_add(RegistryView::NAME_ENTRY_LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = name_table.get(offset..end).ok_or(IrError::OutOfBounds)?;
        if (read_u32(entry, 0)? as usize) < floor {
            return Err(IrError::OutOfBounds);
        }
        offset += RegistryView::NAME_ENTRY_LEN;
    }
    Ok(())
}

/// Checks that every config key string-ref starts at or beyond `floor`.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry or a key that
/// starts before the heap floor.
fn check_keys_in_heap(entries: &[u8], floor: usize) -> Result<(), IrError> {
    let mut offset = 0;
    while offset < entries.len() {
        let end = offset
            .checked_add(ConfigBindingView::LEN)
            .ok_or(IrError::OutOfBounds)?;
        let entry = entries.get(offset..end).ok_or(IrError::OutOfBounds)?;
        if (read_u32(entry, 8)? as usize) < floor {
            return Err(IrError::OutOfBounds);
        }
        offset += ConfigBindingView::LEN;
    }
    Ok(())
}

/// Checks that the sorted index lists stage names in non-decreasing order,
/// the invariant the name lookup's binary search relies on.
///
/// # Errors
///
/// Returns [`IrError::OutOfBounds`] on a malformed entry and
/// [`IrError::RegistryUnsorted`] if two adjacent names are out of order.
fn check_sorted_order(view: &RegistryView<'_>, stage_count: u32) -> Result<(), IrError> {
    let mut previous: Option<&[u8]> = None;
    for rank in 0..stage_count {
        let ordinal = view.sorted_ordinal(rank).ok_or(IrError::OutOfBounds)?;
        let name = view.name(u32::from(ordinal)).ok_or(IrError::OutOfBounds)?;
        if let Some(prev) = previous {
            if name < prev {
                return Err(IrError::RegistryUnsorted);
            }
        }
        previous = Some(name);
    }
    Ok(())
}
