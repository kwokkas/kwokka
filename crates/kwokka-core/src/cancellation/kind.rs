//! [`CancellationKind`] - reason a cancellation was triggered.

/// Why a cancellation was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CancellationKind {
    /// Direct cancel - explicit user or system request via
    /// `conductor.cancel(pip, ...)`.
    Hard,
    /// Deadline expired - originated from `Advisor::guard().timeout(dur)`
    /// and auto-propagated to the inner task.
    Timeout,
    /// Propagated from a sibling or dependency failure in a Conductor DAG.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_are_distinct() {
        assert_ne!(CancellationKind::Hard, CancellationKind::Timeout);
        assert_ne!(CancellationKind::Timeout, CancellationKind::Failed);
        assert_ne!(CancellationKind::Hard, CancellationKind::Failed);
    }
}
