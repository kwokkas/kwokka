//! The conductor DAG spec view: the root of a validated IR blob.

use crate::{conductor::EdgeView, error::IrError, flat::reader::read_u32};

/// Byte offset of the stage-count field within a `ConductorSpec` body.
const STAGE_COUNT_FIELD: usize = 0;

/// Byte offset of the edge-count field within a `ConductorSpec` body.
const EDGE_COUNT_FIELD: usize = 4;

/// Byte offset of the edge-table-offset field within a `ConductorSpec` body.
const EDGE_TABLE_OFFSET_FIELD: usize = 12;

/// The conductor DAG spec: the stage count and the edge table.
///
/// Obtained from [`KwokkaIr::conductor`]. Stage bodies (policies, names) are
/// not part of this view; later layers add the stage table at the reserved
/// offset. Graph well-formedness beyond ordinal bounds (cycles, topological
/// order, edge arity) is the consumer's responsibility.
///
/// [`KwokkaIr::conductor`]: crate::KwokkaIr::conductor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConductorView<'a> {
    stage_count: u32,
    edge_count: u32,
    edge_table: &'a [u8],
}

impl<'a> ConductorView<'a> {
    /// Parses and bounds-checks a `ConductorSpec` record body.
    ///
    /// # Errors
    ///
    /// Returns [`IrError::Truncated`] if a count or offset field is out of
    /// range, [`IrError::OutOfBounds`] if the edge table extends past the
    /// body, and [`IrError::OrdinalOutOfRange`] if an edge names a stage
    /// ordinal at or beyond `stage_count`.
    pub(crate) fn parse(body: &'a [u8]) -> Result<Self, IrError> {
        let stage_count = read_u32(body, STAGE_COUNT_FIELD)?;
        let edge_count = read_u32(body, EDGE_COUNT_FIELD)?;
        let table_offset = read_u32(body, EDGE_TABLE_OFFSET_FIELD)? as usize;
        let span = (edge_count as usize)
            .checked_mul(EdgeView::LEN)
            .ok_or(IrError::OutOfBounds)?;
        let end = table_offset.checked_add(span).ok_or(IrError::OutOfBounds)?;
        let edge_table = body.get(table_offset..end).ok_or(IrError::OutOfBounds)?;
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
        Ok(Self {
            stage_count,
            edge_count,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn one_edge_body(stage_count: u32, from: u16, to: u16) -> [u8; 24] {
        let mut body = [0u8; 24];
        body[0..4].copy_from_slice(&stage_count.to_le_bytes());
        body[4..8].copy_from_slice(&1u32.to_le_bytes());
        body[12..16].copy_from_slice(&16u32.to_le_bytes());
        body[16..18].copy_from_slice(&from.to_le_bytes());
        body[18..20].copy_from_slice(&to.to_le_bytes());
        body
    }

    #[test]
    fn parses_a_single_edge_dag() {
        let body = one_edge_body(2, 0, 1);
        assert!(matches!(
            ConductorView::parse(&body),
            Ok(view) if view.stage_count() == 2 && view.edge_count() == 1
        ));
    }

    #[test]
    fn reads_edge_fields() {
        let body = one_edge_body(3, 1, 2);
        let edge = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.edge(0));
        assert_eq!(
            edge.map(|e| (e.from_ordinal(), e.to_ordinal(), e.input_index())),
            Some((1, 2, 0))
        );
    }

    #[test]
    fn edge_past_count_is_none() {
        let body = one_edge_body(2, 0, 1);
        let edge = ConductorView::parse(&body)
            .ok()
            .and_then(|view| view.edge(1));
        assert_eq!(edge.map(|e| e.from_ordinal()), None);
    }

    #[test]
    fn edges_iterates_every_edge() {
        let body = one_edge_body(2, 0, 1);
        assert_eq!(
            ConductorView::parse(&body).map(|view| view.edges().count()),
            Ok(1)
        );
    }

    #[test]
    fn rejects_an_ordinal_overrun() {
        let body = one_edge_body(2, 0, 5);
        assert_eq!(ConductorView::parse(&body), Err(IrError::OrdinalOutOfRange));
    }

    #[test]
    fn rejects_an_edge_table_overrun() {
        let mut body = one_edge_body(2, 0, 1);
        body[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(ConductorView::parse(&body), Err(IrError::OutOfBounds));
    }

    #[test]
    fn rejects_a_truncated_body() {
        let body = [0u8; 8];
        assert_eq!(ConductorView::parse(&body), Err(IrError::Truncated));
    }
}
