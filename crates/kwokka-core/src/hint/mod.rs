//! Task placement and scheduler-selection hints.

mod affinity;
mod scheduling;

pub use affinity::AffinityHint;
pub use scheduling::SchedulingHint;
