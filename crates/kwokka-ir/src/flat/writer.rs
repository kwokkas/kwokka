//! Single-pass flat writer that encodes a conductor DAG into an IR blob.
//!
//! [`write_conductor`] computes every section offset up front from the
//! conductor inputs, then writes the blob front-to-back with no backfill:
//! header, root `ConductorSpec` frame, body header, stage table, edge
//! table, the policy-record payload heap, the optional registry and config
//! sections, and the trailing string heap. The bytes it produces are
//! exactly what [`crate::validate`] accepts.

use crate::{
    conductor::{ConductorBlob, ConfigBindingSpec, EdgeView, RegistrySpec, StageSpec},
    config::ConfigBindingView,
    flat::{HEADER_LEN, MAGIC, VERSION, reader::RECORD_HEADER_LEN},
    node::NodeTag,
    policy::{BreakerView, LimiterView, PolicyKind, RetryView, TimeoutView},
    registry::RegistryView,
};

/// Byte length of the fixed conductor body header; mirrors the reader.
const CONDUCTOR_HEADER_LEN: usize = 24;

/// Byte length of one stage-table entry: one u32 policy slot per kind.
const STAGE_ENTRY_LEN: usize = PolicyKind::COUNT * 4;

/// Byte length of the fixed config-section header: a `u32` binding count
/// and `u32` padding before the entry array.
const CONFIG_HEADER_LEN: usize = 8;

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
    /// A registry's name or sorted-ordinal count disagrees with the stage
    /// count; the writer requires one of each per stage.
    RegistryArity {
        /// The number of names supplied.
        names: usize,
        /// The number of sorted ordinals supplied.
        sorted: usize,
        /// The number of stages.
        stages: usize,
    },
}

/// Pre-computed section offsets for one conductor blob. Offsets are
/// body-relative; an absent registry or config section stores offset 0.
struct Layout {
    total: usize,
    spec_record_len: usize,
    stage_count: u32,
    edge_count: u32,
    edge_table_off: usize,
    policy_heap_off: usize,
    registry_off: usize,
    config_off: usize,
    heap_off: usize,
}

/// Rounds `value` up to the next multiple of 8.
fn align8(value: usize) -> Result<usize, WriteError> {
    let bumped = value.checked_add(7).ok_or(WriteError::SpecTooLarge)?;
    Ok(bumped & !7)
}

/// Verifies a registry supplies exactly one name and one sorted ordinal per
/// stage.
///
/// # Errors
///
/// Returns [`WriteError::RegistryArity`] if the name or sorted-ordinal count
/// disagrees with the stage count.
const fn validate_registry_arity(blob: &ConductorBlob) -> Result<(), WriteError> {
    let Some(registry) = &blob.registry else {
        return Ok(());
    };
    if registry.names.len() != blob.stages.len() || registry.sorted.len() != blob.stages.len() {
        return Err(WriteError::RegistryArity {
            names: registry.names.len(),
            sorted: registry.sorted.len(),
            stages: blob.stages.len(),
        });
    }
    Ok(())
}

impl Layout {
    /// Computes the section offsets, or an error if the spec overflows or a
    /// registry's arity is wrong.
    fn compute(blob: &ConductorBlob) -> Result<Self, WriteError> {
        let stage_count = u32::try_from(blob.stages.len())
            .ok()
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_count = u32::try_from(blob.edges.len())
            .ok()
            .ok_or(WriteError::SpecTooLarge)?;
        validate_registry_arity(blob)?;

        let stage_table_len = blob
            .stages
            .len()
            .checked_mul(STAGE_ENTRY_LEN)
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_table_off = CONDUCTOR_HEADER_LEN
            .checked_add(stage_table_len)
            .ok_or(WriteError::SpecTooLarge)?;
        let edge_table_len = blob
            .edges
            .len()
            .checked_mul(EdgeView::LEN)
            .ok_or(WriteError::SpecTooLarge)?;
        let policy_heap_off = edge_table_off
            .checked_add(edge_table_len)
            .ok_or(WriteError::SpecTooLarge)?;
        let mut after_policies = policy_heap_off;
        for stage in blob.stages {
            after_policies = after_policies
                .checked_add(stage.payload_len()?)
                .ok_or(WriteError::SpecTooLarge)?;
        }

        let (registry_off, after_registry) = if blob.registry.is_some() {
            let len = registry_len(blob.stages.len())?;
            let end = after_policies
                .checked_add(len)
                .ok_or(WriteError::SpecTooLarge)?;
            (after_policies, end)
        } else {
            (0, after_policies)
        };

        let (config_off, after_config) = if blob.config.is_empty() {
            (0, after_registry)
        } else {
            let entries_len = blob
                .config
                .len()
                .checked_mul(ConfigBindingView::LEN)
                .ok_or(WriteError::SpecTooLarge)?;
            let len = CONFIG_HEADER_LEN
                .checked_add(entries_len)
                .ok_or(WriteError::SpecTooLarge)?;
            let end = after_registry
                .checked_add(len)
                .ok_or(WriteError::SpecTooLarge)?;
            (after_registry, end)
        };

        let heap_off = after_config;
        let body_len = align8(
            heap_off
                .checked_add(string_heap_len(blob)?)
                .ok_or(WriteError::SpecTooLarge)?,
        )?;
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
            policy_heap_off,
            registry_off,
            config_off,
            heap_off,
        })
    }
}

/// Total 8-aligned byte length of a registry section for `stage_count` stages.
fn registry_len(stage_count: usize) -> Result<usize, WriteError> {
    let names = stage_count
        .checked_mul(RegistryView::NAME_ENTRY_LEN)
        .ok_or(WriteError::SpecTooLarge)?;
    let sorted = stage_count
        .checked_mul(RegistryView::SORTED_ENTRY_LEN)
        .ok_or(WriteError::SpecTooLarge)?;
    align8(names.checked_add(sorted).ok_or(WriteError::SpecTooLarge)?)
}

/// Total byte length of the string heap: every registry name and config key.
fn string_heap_len(blob: &ConductorBlob) -> Result<usize, WriteError> {
    let mut total = 0usize;
    if let Some(registry) = &blob.registry {
        for name in registry.names {
            total = total
                .checked_add(name.len())
                .ok_or(WriteError::SpecTooLarge)?;
        }
    }
    for binding in blob.config {
        total = total
            .checked_add(binding.key.len())
            .ok_or(WriteError::SpecTooLarge)?;
    }
    Ok(total)
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

    /// The slots' record body lengths in `guard()` order, `None` when the
    /// slot is empty. The array length is pinned to [`PolicyKind::COUNT`],
    /// so adding a policy kind without a slot here fails to compile.
    const fn record_body_lens(&self) -> [Option<usize>; PolicyKind::COUNT] {
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
/// Stage `i`'s policy slots come from `blob.stages[i]`; edges connect stage
/// ordinals; the optional registry and config sections follow. When every
/// edge and binding ordinal is within `[0, stages.len())`, the output is a
/// validated-shape blob: feeding the written prefix to [`crate::validate`]
/// round-trips back to these inputs. Out-of-range ordinals are encoded
/// verbatim and rejected by the reader, not here.
///
/// # Errors
///
/// Returns [`WriteError::BufferTooSmall`] if `buf` cannot hold the blob,
/// [`WriteError::SpecTooLarge`] if the offset arithmetic overflows the wire
/// range, and [`WriteError::RegistryArity`] if a registry's name or sorted
/// count does not match the stage count.
pub fn write_conductor(buf: &mut [u8], blob: &ConductorBlob) -> Result<usize, WriteError> {
    let layout = Layout::compute(blob)?;
    if buf.len() < layout.total {
        return Err(WriteError::BufferTooSmall {
            needed: layout.total,
            available: buf.len(),
        });
    }
    buf[..layout.total].fill(0);
    write_header(buf, &layout);
    write_stage_table(buf, &layout, blob.stages);
    write_edge_table(buf, &layout, blob.edges);
    write_policy_records(buf, &layout, blob.stages);
    let mut heap = layout.heap_off;
    if let Some(registry) = &blob.registry {
        write_registry(buf, &layout, registry, &mut heap);
    }
    if !blob.config.is_empty() {
        write_config(buf, &layout, blob.config, &mut heap);
    }
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
    put_offset(buf, BODY_OFFSET + 16, layout.registry_off);
    put_offset(buf, BODY_OFFSET + 20, layout.config_off);
}

/// Writes the fixed-stride stage table, filling each policy slot with the
/// body-relative offset of its record (0 when the slot is empty).
fn write_stage_table(buf: &mut [u8], layout: &Layout, stages: &[StageSpec]) {
    let table = BODY_OFFSET + CONDUCTOR_HEADER_LEN;
    let mut payload = layout.policy_heap_off;
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
    let mut payload = layout.policy_heap_off;
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

/// Writes the registry's ordinal-to-name table, the sorted-ordinal index,
/// and the name bytes into the string heap, advancing `heap`.
fn write_registry(buf: &mut [u8], layout: &Layout, registry: &RegistrySpec, heap: &mut usize) {
    let name_table = BODY_OFFSET + layout.registry_off;
    let sorted_table = name_table + (layout.stage_count as usize) * RegistryView::NAME_ENTRY_LEN;
    for (i, name) in registry.names.iter().enumerate() {
        let entry = name_table + i * RegistryView::NAME_ENTRY_LEN;
        put_offset(buf, entry, *heap);
        put_offset(buf, entry + 4, name.len());
        write_bytes(buf, heap, name);
    }
    for (k, ordinal) in registry.sorted.iter().enumerate() {
        put_u16(
            buf,
            sorted_table + k * RegistryView::SORTED_ENTRY_LEN,
            *ordinal,
        );
    }
}

/// Writes the config-section header, the binding entries, and the key bytes
/// into the string heap, advancing `heap`.
fn write_config(buf: &mut [u8], layout: &Layout, config: &[ConfigBindingSpec], heap: &mut usize) {
    let section = BODY_OFFSET + layout.config_off;
    put_offset(buf, section, config.len());
    put_u32(buf, section + 4, 0);
    let entries = section + CONFIG_HEADER_LEN;
    for (i, binding) in config.iter().enumerate() {
        let entry = entries + i * ConfigBindingView::LEN;
        put_u16(buf, entry, binding.node_ordinal);
        put_u16(buf, entry + 2, binding.field_tag);
        put_offset(buf, entry + 4, binding.key.len());
        put_offset(buf, entry + 8, *heap);
        put_u32(buf, entry + 12, 0);
        put_u64(buf, entry + 16, u64::from(binding.default_value.kind()));
        put_u64(buf, entry + 24, binding.default_value.raw_value());
        write_bytes(buf, heap, binding.key);
    }
}

/// Copies `bytes` into the string heap at the current `heap` cursor and
/// advances the cursor.
fn write_bytes(buf: &mut [u8], heap: &mut usize, bytes: &[u8]) {
    let start = BODY_OFFSET + *heap;
    buf[start..start + bytes.len()].copy_from_slice(bytes);
    *heap += bytes.len();
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
    use crate::{IrError, ScalarValue, validate};

    #[test]
    fn round_trips_a_full_stage() {
        let stages = [StageSpec {
            limiter: Some(LimiterView::new(100, 10, 1_000)),
            timeout: Some(TimeoutView::new(5_000)),
            retry: Some(RetryView::new(5, 2, 1, 1_000, 60_000)),
            breaker: Some(BreakerView::new(1, 50, 20, 10_000, 3, 2, 30_000)),
        }];
        let blob = ConductorBlob {
            stages: &stages,
            ..Default::default()
        };
        let mut buf = [0u8; 256];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
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
        let blob = ConductorBlob {
            stages: &stages,
            edges: &edges,
            ..Default::default()
        };
        let mut buf = [0u8; 128];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
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
    fn round_trips_a_registry() {
        let stages = [StageSpec::default(), StageSpec::default()];
        let names: [&[u8]; 2] = [b"beta", b"alpha"];
        let sorted = [1u16, 0];
        let blob = ConductorBlob {
            stages: &stages,
            registry: Some(RegistrySpec {
                names: &names,
                sorted: &sorted,
            }),
            ..Default::default()
        };
        let mut buf = [0u8; 256];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
        let registry = validate(&buf[..written])
            .and_then(|ir| ir.conductor())
            .ok()
            .and_then(|spec| spec.registry());
        let lookups = registry.map(|r| (r.name(0), r.lookup(b"alpha"), r.lookup(b"beta")));
        assert_eq!(lookups, Some((Some(&b"beta"[..]), Some(1), Some(0))));
    }

    #[test]
    fn round_trips_config_bindings() {
        let stages = [StageSpec::default(), StageSpec::default()];
        let config = [ConfigBindingSpec {
            node_ordinal: 1,
            field_tag: ConfigBindingView::FIELD_TIMEOUT_DURATION_NS,
            key: b"ttl",
            default_value: ScalarValue::new(ScalarValue::KIND_DURATION_NS, 5_000),
        }];
        let blob = ConductorBlob {
            stages: &stages,
            config: &config,
            ..Default::default()
        };
        let mut buf = [0u8; 256];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
        let spec = validate(&buf[..written]).and_then(|ir| ir.conductor());
        let binding = spec.ok().and_then(|s| s.config_binding(0));
        let fields = binding.map(|b| (b.node_ordinal(), b.key(), b.default_value().raw_value()));
        assert_eq!(fields, Some((1, &b"ttl"[..], 5_000)));
        let count = validate(&buf[..written])
            .and_then(|ir| ir.conductor())
            .map(|s| s.config_bindings().count());
        assert_eq!(count, Ok(1));
    }

    #[test]
    fn rejects_bad_config_ordinal() {
        let stages = [StageSpec::default()];
        let config = [ConfigBindingSpec {
            node_ordinal: 5,
            field_tag: ConfigBindingView::FIELD_TIMEOUT_DURATION_NS,
            key: b"k",
            default_value: ScalarValue::new(ScalarValue::KIND_U64, 0),
        }];
        let blob = ConductorBlob {
            stages: &stages,
            config: &config,
            ..Default::default()
        };
        let mut buf = [0u8; 128];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
        assert!(matches!(
            validate(&buf[..written]),
            Err(IrError::OrdinalOutOfRange)
        ));
    }

    #[test]
    fn rejects_bad_sorted_ordinal() {
        let stages = [StageSpec::default(), StageSpec::default()];
        let names: [&[u8]; 2] = [b"a", b"b"];
        let sorted = [5u16, 0];
        let blob = ConductorBlob {
            stages: &stages,
            registry: Some(RegistrySpec {
                names: &names,
                sorted: &sorted,
            }),
            ..Default::default()
        };
        let mut buf = [0u8; 128];
        let written = write_conductor(&mut buf, &blob).unwrap_or(0);
        assert!(matches!(
            validate(&buf[..written]),
            Err(IrError::OrdinalOutOfRange)
        ));
    }

    #[test]
    fn rejects_a_small_buffer() {
        let stages = [StageSpec::default()];
        let blob = ConductorBlob {
            stages: &stages,
            ..Default::default()
        };
        let mut buf = [0u8; 8];
        assert!(matches!(
            write_conductor(&mut buf, &blob),
            Err(WriteError::BufferTooSmall { .. })
        ));
    }

    #[test]
    fn rejects_a_registry_arity_mismatch() {
        let stages = [StageSpec::default(), StageSpec::default()];
        let names: [&[u8]; 1] = [b"only"];
        let sorted = [0u16];
        let blob = ConductorBlob {
            stages: &stages,
            registry: Some(RegistrySpec {
                names: &names,
                sorted: &sorted,
            }),
            ..Default::default()
        };
        let mut buf = [0u8; 128];
        assert!(matches!(
            write_conductor(&mut buf, &blob),
            Err(WriteError::RegistryArity { .. })
        ));
    }
}
