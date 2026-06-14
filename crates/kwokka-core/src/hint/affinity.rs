//! [`AffinityHint`] - CPU affinity hint for task placement.

/// CPU affinity hint for task placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum AffinityHint {
    /// No placement preference - runtime decides.
    #[default]
    Unbound,
    /// Prefer a specific worker thread (best-effort).
    Worker(u32),
    /// Must run on a specific core (hard pin, `ThreadPerCore` only).
    Pinned(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unbound() {
        assert_eq!(AffinityHint::default(), AffinityHint::Unbound);
    }

    #[test]
    fn variants_are_distinct() {
        assert_ne!(AffinityHint::Unbound, AffinityHint::Worker(0));
        assert_ne!(AffinityHint::Worker(0), AffinityHint::Pinned(0));
        assert_ne!(AffinityHint::Worker(7), AffinityHint::Worker(8));
    }
}
