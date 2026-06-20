//! Per-worker work-stealing deque over [`TaskRef`] handles.
//!
//! The owning worker pushes and pops at one end in LIFO order, keeping the
//! cache-warm newest task local; thieves take the oldest task from the
//! opposite end through cloned [`StealHandle`]s.

use crossbeam_deque::{Steal, Stealer, Worker};

use crate::task::TaskRef;

/// The owning worker's end of its deque.
///
/// Single-owner by construction: the worker that created the deque is the
/// only pusher and popper. Thieves reach it through [`LocalDeque::stealer`].
pub(crate) struct LocalDeque {
    worker: Worker<TaskRef>,
}

impl LocalDeque {
    /// Creates an empty deque owned by the calling worker.
    pub(crate) fn new() -> Self {
        Self {
            worker: Worker::new_lifo(),
        }
    }

    /// Pushes a runnable task handle onto the owner's end.
    pub(crate) fn push(&self, task: TaskRef) {
        self.worker.push(task);
    }

    /// Pops the most recently pushed handle, or `None` when empty.
    pub(crate) fn pop(&self) -> Option<TaskRef> {
        self.worker.pop()
    }

    /// Creates a thief-side handle onto this deque.
    pub(crate) fn stealer(&self) -> StealHandle {
        StealHandle {
            inner: self.worker.stealer(),
        }
    }
}

impl Default for LocalDeque {
    fn default() -> Self {
        Self::new()
    }
}

/// A thief's handle onto another worker's deque.
pub(crate) struct StealHandle {
    inner: Stealer<TaskRef>,
}

impl StealHandle {
    /// Steals the oldest queued handle, or `None` when the deque is empty.
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

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use kwokka_core::Generation;

    fn task_for(index: u32) -> TaskRef {
        TaskRef::from_arena(0, index, Generation::ZERO)
    }

    #[test]
    fn owner_pops_newest_first() {
        let deque = LocalDeque::new();
        deque.push(task_for(1));
        deque.push(task_for(2));
        assert_eq!(deque.pop(), Some(task_for(2)));
        assert_eq!(deque.pop(), Some(task_for(1)));
        assert_eq!(deque.pop(), None);
    }

    #[test]
    fn thief_steals_oldest_first() {
        let deque = LocalDeque::new();
        deque.push(task_for(1));
        deque.push(task_for(2));
        let handle = deque.stealer();
        assert_eq!(handle.steal(), Some(task_for(1)));
        assert_eq!(handle.steal(), Some(task_for(2)));
        assert_eq!(handle.steal(), None);
    }
}
