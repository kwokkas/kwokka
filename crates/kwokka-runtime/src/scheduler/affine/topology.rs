//! CPU topology probe for affine placement.

use crate::worker::WorkerId;

/// Maps a worker to its CPU index under the 1:1 placement policy.
///
/// The lead takes CPU zero and each sibling takes the CPU matching its offset
/// within the crew's contiguous worker-id block.
pub(crate) fn cpu_index_for(worker: WorkerId, lead: WorkerId) -> usize {
    usize::from(worker.raw().saturating_sub(lead.raw()))
}
