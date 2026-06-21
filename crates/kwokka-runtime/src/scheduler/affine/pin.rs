//! Worker-to-core pinning for the affine scheduler.

/// Attempts to pin the calling thread to `cpu_index`.
///
/// Non-fatal by contract. The runtime does not depend on the observability
/// crate at this layer, so a failure is returned for the caller to absorb.
/// Affinity is a performance hint, not a correctness requirement -- an unpinned
/// worker still runs correctly.
///
/// # Errors
///
/// Propagates the `errno` from `sched_setaffinity(2)`: `EPERM` when the process
/// lacks privilege, `EINVAL` when `cpu_index` is out of range.
pub(crate) fn try_pin_current_thread(cpu_index: usize) -> Result<(), i32> {
    crate::scheduler::affine::affinity::set_affinity(cpu_index)
}
