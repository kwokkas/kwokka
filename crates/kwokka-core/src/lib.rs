//! Foundational types shared across the kwokka workspace.

pub mod cancellation;
pub mod hint;
pub mod id;

pub use cancellation::{
    AlreadyCancelledBehavior, CancellationContext, CancellationKind, CancellationPolicy,
};
pub use hint::{AffinityHint, SchedulingHint};
pub use id::{Pip, PipError};
