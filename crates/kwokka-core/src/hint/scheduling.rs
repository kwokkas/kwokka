//! [`SchedulingHint`] - scheduler selection hint for task execution.

/// Hint to the runtime about which scheduler should execute a task.
///
/// Deliberately not `Default`: the scheduler is always chosen explicitly
/// at the macro call site, never inferred from a fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchedulingHint {
    /// Work-stealing scheduler - tasks may migrate between workers.
    WorkStealing,
    /// Thread-per-core scheduler - tasks are pinned to their originating core.
    ThreadPerCore,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_are_distinct() {
        assert_ne!(SchedulingHint::WorkStealing, SchedulingHint::ThreadPerCore);
    }
}
