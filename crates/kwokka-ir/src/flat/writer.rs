//! Single-pass flat writer that encodes a conductor DAG into an IR blob.
//!
//! [`write_conductor`] computes every section offset up front from the
//! stage and edge counts, then writes the blob front-to-back with no
//! backfill: header, root `ConductorSpec` frame, body header, stage
//! table, edge table, and the policy-record payload heap. The bytes it
//! produces are exactly what [`crate::validate`] accepts.

use crate::{
    conductor::EdgeView,
    flat::{HEADER_LEN, MAGIC, VERSION, reader::RECORD_HEADER_LEN},
    node::NodeTag,
    policy::{BreakerView, LimiterView, PolicyKind, RetryView, TimeoutView},
};

/// Byte length of the fixed conductor body header; mirrors the reader.
const CONDUCTOR_HEADER_LEN: usize = 16;

/// Byte length of one stage-table entry: one u32 policy slot per kind.
const STAGE_ENTRY_LEN: usize = PolicyKind::COUNT * 4;

/// An error from encoding an IR blob.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// The destination buffer is smaller than the encoded blob.
    BufferTooSmall {
        /// Bytes the blob requires.
        needed: usize,
        /// Bytes the destination buffer provides.
        available: usize,
    },
    /// The spec exceeds the wire offset range (64-bit unreachable).
    SpecTooLarge,
}

/// The four policy slots of a stage in `guard()` order.
///
/// A `None` slot means the stage carries no policy of that kind. This is
/// the per-stage writer input, paired with the edge list passed to
/// [`write_conductor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StageSpec {
    /// Rate limiter slot; `None` if this stage carries no rate limit.
    pub limiter: Option<LimiterView>,
    /// Overall timeout slot; `None` if this stage has no wall-clock cap.
    pub timeout: Option<TimeoutView>,
    /// Retry slot; `None` if this stage does not retry on failure.
    pub retry: Option<RetryView>,
    /// Circuit breaker slot; `None` if this stage has no breaker.
    pub breaker: Option<BreakerView>,
}

/// Pre-computed section offsets for one conductor blob.
struct Layout {
    total: usize,
    spec_record_len: usize,
    stage_count: u32,
    edge_count: u32,
    edge_table_off: usize,
    payload_off: usize,
}

impl Layout {
    /// Computes the section offsets, or an error if the spec overflows.
    fn compute(stages: &[StageSpec], edges: &[EdgeView]) -> Result<Self, WriteError> {
        let stage_count = u32::try_from(stages.len())
            .ok()
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_count = u32::try_from(edges.len())
            .ok()
            .ok_or(WriteError::SpecTooLarge)?;
        let stage_table_len = stages
            .len()
            .checked_mul(STAGE_ENTRY_LEN)
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_table_off = CONDUCTOR_HEADER_LEN
            .checked_add(stage_table_len)
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_table_len = edges
            .len()
            .checked_mul(EdgeView::LEN)
            .ok_or(WriteError::SpecTooLarge)?;
        let payload_off = edge_table_off
            .checked_add(edge_table_len)
            .ok_or(WriteError::SpecTooLarge)?;
        let mut body_len = payload_off;
        for stage in stages {
            body_len = body_len
                .checked_add(stage.payload_len()?)
                .ok_or(WriteError::SpecTooLarge)?;
        }
        let spec_record_len = RECORD_HEADER_LEN
            .checked_add(body_len)
            .ok_or(WriteError::SpecTooLarge)?;
        let total = HEADER_LEN
            .checked_add(spec_record_len)
            .ok_or(WriteError::SpecTooLarge)?;
        u32::try_from(total).ok().ok_or(WriteError::SpecTooLarge)?;
        Ok(Self {
            total,
            spec_record_len,
            stage_count,
            edge_count,
            edge_table_off,
            payload_off,
        })
    }
}

impl StageSpec {
    /// Total payload bytes this stage's policy records occupy.
    fn payload_len(&self) -> Result<usize, WriteError> {
        let mut total = 0usize;
        for len in self.record_body_lens().into_iter().flatten() {
            total = total
                .checked_add(RECORD_HEADER_LEN + len)
                .ok_or(WriteError::SpecTooLarge)?;
        }
        Ok(total)
    }

    /// The four slots' record body lengths in `guard()` order, `None` when
    /// the slot is empty.
    const fn record_body_lens(&self) -> [Option<usize>; 4] {
        [
            match self.limiter {
                Some(_) => Some(LimiterView::LEN),
                None => None,
            },
            match self.timeout {
                Some(_) => Some(TimeoutView::LEN),
                None => None,
            },
            match self.retry {
                Some(_) => Some(RetryView::LEN),
                None => None,
            },
            match self.breaker {
                Some(_) => Some(BreakerView::LEN),
                None => None,
            },
        ]
    }
}

/// Byte offset of the conductor body within the blob (header + spec frame).
const BODY_OFFSET: usize = HEADER_LEN + RECORD_HEADER_LEN;

/// Encodes a conductor DAG into `buf`, returning the byte length written.
///
/// Stage `i`'s policy slots come from `stages[i]`; edges connect stage
/// ordinals. When every edge ordinal is within `[0, stages.len())`, the
/// output is a validated-shape blob: feeding the written prefix to
/// [`crate::validate`] round-trips back to these inputs. Out-of-range
/// ordinals are encoded verbatim and rejected by the reader, not here.
///
/// # Errors
///
/// Returns [`WriteError::BufferTooSmall`] if `buf` cannot hold the blob,
/// and [`WriteError::SpecTooLarge`] if the offset arithmetic overflows the
/// wire range.
pub fn write_conductor(
    buf: &mut [u8],
    stages: &[StageSpec],
    edges: &[EdgeView],
) -> Result<usize, WriteError> {
    let layout = Layout::compute(stages, edges)?;
    if buf.len() < layout.total {
        return Err(WriteError::BufferTooSmall {
            needed: layout.total,
            available: buf.len(),
        });
    }
    write_header(buf, &layout);
    write_stage_table(buf, &layout, stages);
    write_edge_table(buf, &layout, edges);
    write_policy_records(buf, &layout, stages);
    Ok(layout.total)
}

/// Writes the blob header, root frame, and conductor body header.
fn write_header(buf: &mut [u8], layout: &Layout) {
    buf[0..4].copy_from_slice(&MAGIC);
    put_u16(buf, 4, VERSION);
    put_u16(buf, 6, 0);
    put_offset(buf, 8, layout.total);
    put_offset(buf, 12, HEADER_LEN);
    put_u16(buf, HEADER_LEN, NodeTag::ConductorSpec as u16);
    put_u16(buf, HEADER_LEN + 2, 0);
    put_offset(buf, HEADER_LEN + 4, layout.spec_record_len);
    put_u32(buf, BODY_OFFSET, layout.stage_count);
    put_u32(buf, BODY_OFFSET + 4, layout.edge_count);
    put_offset(buf, BODY_OFFSET + 8, CONDUCTOR_HEADER_LEN);
    put_offset(buf, BODY_OFFSET + 12, layout.edge_table_off);
}

/// Writes the fixed-stride stage table, filling each policy slot with the
/// body-relative offset of its record (0 when the slot is empty).
fn write_stage_table(buf: &mut [u8], layout: &Layout, stages: &[StageSpec]) {
    let table = BODY_OFFSET + CONDUCTOR_HEADER_LEN;
    let mut payload = layout.payload_off;
    for (i, stage) in stages.iter().enumerate() {
        let entry = table + i * STAGE_ENTRY_LEN;
        for (slot, len) in stage.record_body_lens().into_iter().enumerate() {
            match len {
                Some(len) => {
                    put_offset(buf, entry + slot * 4, payload);
                    payload += RECORD_HEADER_LEN + len;
                }
                None => put_offset(buf, entry + slot * 4, 0),
            }
        }
    }
}

/// Writes the edge table.
fn write_edge_table(buf: &mut [u8], layout: &Layout, edges: &[EdgeView]) {
    let table = BODY_OFFSET + layout.edge_table_off;
    for (j, edge) in edges.iter().enumerate() {
        let entry = table + j * EdgeView::LEN;
        put_u16(buf, entry, edge.from_ordinal());
        put_u16(buf, entry + 2, edge.to_ordinal());
        put_u16(buf, entry + 4, edge.input_index());
        put_u16(buf, entry + 6, 0);
    }
}

/// Writes the policy records into the payload heap in the same slot order
/// the stage table reserved.
fn write_policy_records(buf: &mut [u8], layout: &Layout, stages: &[StageSpec]) {
    let mut payload = layout.payload_off;
    for stage in stages {
        if let Some(v) = stage.limiter {
            let body = write_frame(buf, payload, NodeTag::PolicyLimiter, LimiterView::LEN);
            put_u32(buf, body, v.capacity());
            put_u32(buf, body + 4, v.refill_tokens());
            put_u64(buf, body + 8, v.refill_period_ns());
            payload += RECORD_HEADER_LEN + LimiterView::LEN;
        }
        if let Some(v) = stage.timeout {
            let body = write_frame(buf, payload, NodeTag::PolicyTimeout, TimeoutView::LEN);
            put_u64(buf, body, v.duration_ns());
            payload += RECORD_HEADER_LEN + TimeoutView::LEN;
        }
        if let Some(v) = stage.retry {
            let body = write_frame(buf, payload, NodeTag::PolicyRetry, RetryView::LEN);
            put_u32(buf, body, v.max_attempts());
            buf[body + 4] = v.backoff_kind();
            buf[body + 5] = v.jitter_kind();
            put_u16(buf, body + 6, 0);
            put_u64(buf, body + 8, v.base_delay_ns());
            put_u64(buf, body + 16, v.max_delay_ns());
            payload += RECORD_HEADER_LEN + RetryView::LEN;
        }
        if let Some(v) = stage.breaker {
            let body = write_frame(buf, payload, NodeTag::PolicyBreaker, BreakerView::LEN);
            buf[body] = v.window_kind();
            buf[body + 1] = v.failure_rate_percent();
            put_u16(buf, body + 2, 0);
            put_u32(buf, body + 4, v.minimum_calls());
            put_u64(buf, body + 8, v.window_span());
            put_u32(buf, body + 16, v.half_open_max_calls());
            put_u32(buf, body + 20, v.half_open_success_threshold());
            put_u64(buf, body + 24, v.open_duration_ns());
            payload += RECORD_HEADER_LEN + BreakerView::LEN;
        }
    }
}

/// Writes a record frame at body-relative `payload`, returning the
/// absolute offset of the record body.
fn write_frame(buf: &mut [u8], payload: usize, tag: NodeTag, body_len: usize) -> usize {
    let record = BODY_OFFSET + payload;
    put_u16(buf, record, tag as u16);
    put_u16(buf, record + 2, 0);
    put_offset(buf, record + 4, RECORD_HEADER_LEN + body_len);
    record + RECORD_HEADER_LEN
}

/// Writes a body offset or length as a little-endian u32 at `offset`. The
/// value is bounded by the blob total (checked in `Layout::compute`), so the
/// conversion cannot truncate.
fn put_offset(buf: &mut [u8], offset: usize, value: usize) {
    let value = u32::try_from(value).unwrap_or(u32::MAX);
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// Writes a little-endian `u16` at `offset` (caller guarantees capacity).
fn put_u16(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

/// Writes a little-endian `u32` at `offset` (caller guarantees capacity).
fn put_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// Writes a little-endian `u64` at `offset` (caller guarantees capacity).
fn put_u64(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate;

    #[test]
    fn round_trips_a_full_stage() {
        let stages = [StageSpec {
            limiter: Some(LimiterView::new(100, 10, 1_000)),
            timeout: Some(TimeoutView::new(5_000)),
            retry: Some(RetryView::new(5, 2, 1, 1_000, 60_000)),
            breaker: Some(BreakerView::new(1, 50, 20, 10_000, 3, 2, 30_000)),
        }];
        let edges: [EdgeView; 0] = [];
        let mut buf = [0u8; 256];
        let written = write_conductor(&mut buf, &stages, &edges).unwrap_or(0);
        let got = validate(&buf[..written])
            .and_then(|ir| ir.conductor())
            .ok()
            .and_then(|spec| spec.stage(0));
        let policies = got.map(|s| {
            (
                s.limiter().map(|v| v.capacity()),
                s.timeout().map(|v| v.duration_ns()),
                s.retry().map(|v| v.max_attempts()),
                s.breaker().map(|v| v.failure_rate_percent()),
            )
        });
        assert_eq!(policies, Some((Some(100), Some(5_000), Some(5), Some(50))));
    }

    #[test]
    fn round_trips_edges_and_slots() {
        let stages = [StageSpec::default(), StageSpec::default()];
        let edges = [EdgeView::new(0, 1, 0)];
        let mut buf = [0u8; 128];
        let written = write_conductor(&mut buf, &stages, &edges).unwrap_or(0);
        let spec = validate(&buf[..written]).and_then(|ir| ir.conductor());
        assert!(matches!(
            spec,
            Ok(view) if view.stage_count() == 2 && view.edge_count() == 1
        ));
        let edge = spec.ok().and_then(|view| view.edge(0));
        assert_eq!(
            edge.map(|e| (e.from_ordinal(), e.to_ordinal())),
            Some((0, 1))
        );
    }

    #[test]
    fn rejects_a_small_buffer() {
        let stages = [StageSpec::default()];
        let edges: [EdgeView; 0] = [];
        let mut buf = [0u8; 8];
        assert!(matches!(
            write_conductor(&mut buf, &stages, &edges),
            Err(WriteError::BufferTooSmall { .. })
        ));
    }
}
