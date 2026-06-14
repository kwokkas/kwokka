//! Cancellation - Kind (semantics), Policy (behavior), Context (metadata).
//!
//! Three-axis separation: cancellation is not a single enum but three
//! orthogonal concerns. `Kind` says why, `Policy` says how to handle,
//! `Context` records who and when.

mod context;
mod kind;
mod policy;

pub use context::CancellationContext;
pub use kind::CancellationKind;
pub use policy::{AlreadyCancelledBehavior, CancellationPolicy};
