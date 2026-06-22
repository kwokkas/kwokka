//! Cancellation policy governing already-cancelled task behavior.

use core::time::Duration;

/// Behavior when `cancel` is invoked on an already-cancelled task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AlreadyCancelledBehavior {
    /// Second cancel call is a no-op. Idempotent cancel is the conventional
    /// default for task cancellation.
    #[default]
    Idempotent,
    /// Second cancel call panics - strict mode for debugging cancel
    /// lifecycle bugs.
    Panic,
}

/// Rules governing cancellation propagation and cleanup behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationPolicy {
    /// Whether to auto-propagate cancellation to child tasks.
    pub propagate_to_children: bool,

    /// Optional cleanup deadline. `None` means immediate; `Some(d)` allows
    /// a graceful window before forced abort.
    pub cleanup_deadline: Option<Duration>,

    /// Whether to request cancellation of in-flight I/O operations
    /// (honoured by the I/O driver layer).
    pub cancel_inflight_io: bool,

    /// Behavior when cancel is called on an already-cancelled task.
    pub on_already_cancelled: AlreadyCancelledBehavior,
}

impl Default for CancellationPolicy {
    fn default() -> Self {
        Self {
            propagate_to_children: true,
            cleanup_deadline: None,
            cancel_inflight_io: true,
            on_already_cancelled: AlreadyCancelledBehavior::Idempotent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn already_cancelled_default_is_idempotent() {
        assert_eq!(
            AlreadyCancelledBehavior::default(),
            AlreadyCancelledBehavior::Idempotent,
        );
    }

    #[test]
    fn policy_default_values() {
        let policy = CancellationPolicy::default();
        assert!(policy.propagate_to_children);
        assert_eq!(policy.cleanup_deadline, None);
        assert!(policy.cancel_inflight_io);
        assert_eq!(
            policy.on_already_cancelled,
            AlreadyCancelledBehavior::Idempotent
        );
    }
}
