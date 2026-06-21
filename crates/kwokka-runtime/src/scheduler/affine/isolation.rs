//! CPU isolation query for the affine scheduler -- a later release.
//!
//! Reads the kernel's isolated-CPU set so placement can avoid cores the
//! scheduler is told to leave alone. The placement layer consumes this in a
//! later release; the type is a typed placeholder until then.

#![expect(dead_code, reason = "isolation placement wires up in a later release")]

/// The set of CPUs the kernel has isolated from the general scheduler.
pub(crate) struct IsolatedSet {
    /// One bit per CPU; a set bit marks an isolated core.
    bits: u64,
}

impl IsolatedSet {
    /// Returns an empty isolated set.
    pub(crate) const fn query() -> Self {
        Self { bits: 0 }
    }
}
