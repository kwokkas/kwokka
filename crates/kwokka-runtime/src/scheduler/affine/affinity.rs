//! CPU affinity for the affine scheduler -- the `sched_setaffinity` wrapper.

/// Pins the calling thread to a single CPU.
///
/// # Errors
///
/// Returns `Err(errno)` carrying the raw `errno` set by `sched_setaffinity(2)`:
/// `EINVAL` when `cpu_index` names no valid CPU, `EPERM` when the caller lacks
/// privilege.
pub(crate) fn set_affinity(cpu_index: usize) -> Result<(), i32> {
    // SAFETY:
    // Invariant: cpu_set_t is a fixed-size kernel CPU bitmask. Zeroing it and
    //   setting one bit through CPU_SET leaves it fully initialized over a
    //   valid stack allocation, with no aliasing and no uninitialized read.
    // Precondition: cpu_index is the worker's zero-based offset from the lead
    //   (topology::cpu_index_for); the crew size is capped by MAX_WORKERS and
    //   available_parallelism. No range pre-check happens here -- where the
    //   crew exceeds the online CPU count the kernel returns EINVAL, which the
    //   caller absorbs non-fatally (pin.rs).
    // Failure mode: an out-of-range cpu_index still writes inside the fixed
    //   128-byte cpu_set_t (sched_setaffinity(2), linux-man-pages mirror
    //   sched_setaffinity.2: the 1024-CPU / 128-byte mask ceiling) and the
    //   kernel rejects the mask with EINVAL -- no out-of-bounds write, no UB.
    unsafe {
        let mut set: libc::cpu_set_t = core::mem::zeroed();
        libc::CPU_SET(cpu_index, &mut set);
        let result = libc::sched_setaffinity(
            0,
            core::mem::size_of::<libc::cpu_set_t>(),
            core::ptr::from_ref(&set),
        );
        if result == 0 {
            Ok(())
        } else {
            Err(*libc::__errno_location())
        }
    }
}
