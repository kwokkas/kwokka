//! One steal attempt -- sweep the sibling deques, then the shared injector.
//!
//! An idle worker calls [`steal_one`] with handles onto every sibling deque.
//! The sweep order favors sibling queues over the injector so locally queued
//! work drains before externally submitted work.

use crate::scheduler::stealing::{deque::StealHandle, injector::SharedInjector};
use crate::task::TaskRef;

/// Steals one runnable handle from the first non-empty source.
///
/// Sweeps `victims` in order, then falls back to `injector`. Returns `None`
/// when every source is empty, which is the caller's cue to park.
pub(crate) fn steal_one(victims: &[StealHandle], injector: &SharedInjector) -> Option<TaskRef> {
    for victim in victims {
        if let Some(task) = victim.steal() {
            return Some(task);
        }
    }
    injector.steal()
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::scheduler::stealing::deque::LocalDeque;
    use kwokka_core::Generation;

    fn task_for(index: u32) -> TaskRef {
        TaskRef::from_arena(0, index, Generation::ZERO)
    }

    #[test]
    fn sweeps_victims_before_the_injector() {
        let deque = LocalDeque::new();
        deque.push(task_for(1));
        let injector = SharedInjector::new();
        injector.push(task_for(2));
        let victims = [deque.stealer()];
        assert_eq!(steal_one(&victims, &injector), Some(task_for(1)));
        assert_eq!(steal_one(&victims, &injector), Some(task_for(2)));
        assert_eq!(steal_one(&victims, &injector), None);
    }

    #[test]
    fn concurrent_thieves_lose_no_handles() {
        const TASKS: usize = 64;
        let deque = LocalDeque::new();
        let injector = SharedInjector::new();
        for index in 0..TASKS {
            let Ok(index) = u32::try_from(index) else {
                panic!("the task count fits a u32 index");
            };
            deque.push(task_for(index));
        }
        let seen: [AtomicBool; TASKS] = [const { AtomicBool::new(false) }; TASKS];
        std::thread::scope(|threads| {
            for _ in 0..3 {
                let victims = [deque.stealer()];
                let injector = &injector;
                let seen = &seen;
                threads.spawn(move || {
                    while let Some(task) = steal_one(&victims, injector) {
                        let index = task.index() as usize;
                        let was_seen = seen[index].swap(true, Ordering::Relaxed);
                        assert!(!was_seen, "a handle must be taken exactly once");
                    }
                });
            }
            while let Some(task) = deque.pop() {
                let index = task.index() as usize;
                let was_seen = seen[index].swap(true, Ordering::Relaxed);
                assert!(!was_seen, "a handle must be taken exactly once");
            }
        });
        assert!(
            seen.iter().all(|slot| slot.load(Ordering::Relaxed)),
            "every queued handle must surface on some thread",
        );
    }
}
