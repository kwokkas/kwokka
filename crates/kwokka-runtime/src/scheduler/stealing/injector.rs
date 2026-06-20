//! Shared overflow queue every worker pushes to and steals from.
//!
//! Holds [`TaskRef`] handles that no single worker owns yet: external
//! submissions and deque overflow. Workers check it before parking so a
//! queued handle always finds an idle worker.

use crossbeam_deque::{Injector, Steal};

use crate::task::TaskRef;

/// The process-wide overflow queue of runnable task handles.
pub(crate) struct SharedInjector {
    inner: Injector<TaskRef>,
}

impl SharedInjector {
    /// Creates an empty injector.
    pub(crate) fn new() -> Self {
        Self {
            inner: Injector::new(),
        }
    }

    /// Queues a handle for whichever worker takes it first.
    pub(crate) fn push(&self, task: TaskRef) {
        self.inner.push(task);
    }

    /// Takes the oldest queued handle, or `None` when empty.
    ///
    /// A contended attempt retries internally, so the caller only observes
    /// success or empty.
    pub(crate) fn steal(&self) -> Option<TaskRef> {
        loop {
            match self.inner.steal() {
                Steal::Success(task) => return Some(task),
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }
}

impl Default for SharedInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use kwokka_core::Generation;

    fn task_for(index: u32) -> TaskRef {
        TaskRef::from_arena(0, index, Generation::ZERO)
    }

    #[test]
    fn injector_hands_out_oldest_first() {
        let injector = SharedInjector::new();
        injector.push(task_for(1));
        injector.push(task_for(2));
        assert_eq!(injector.steal(), Some(task_for(1)));
        assert_eq!(injector.steal(), Some(task_for(2)));
        assert_eq!(injector.steal(), None);
    }
}
