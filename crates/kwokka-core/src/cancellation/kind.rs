//! [`CancellationKind`] - reason a cancellation was triggered.

/// Why a cancellation was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CancellationKind {
    /// Direct, explicit cancellation requested by the task's owner.
    Hard,
    /// A deadline expired. Mechanism-free: the kind records that a timeout
    /// fired, not which timer or layer fired it.
    Timeout,
    /// An upstream parent or dependency failed and propagated the cancel.
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
