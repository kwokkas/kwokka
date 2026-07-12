//! Per-worker local run queue backed by an intrusive linked list.

use kwokka_core::slab::Slab;

use crate::task::{TaskRef, cell::slot::TaskSlot};

/// FIFO run queue using the intrusive `next_runnable` link in [`TaskHeader`].
///
/// Push and pop both require a slab reference so the queue can read/write
/// the `next_runnable` pointer stored inside each task's header. This
/// avoids any separate node allocation.
///
/// [`TaskHeader`]: crate::task::cell::header::TaskHeader
pub(crate) struct LocalRunQueue {
    head: Option<TaskRef>,
    tail: Option<TaskRef>,
    len: usize,
}

impl LocalRunQueue {
    /// Empty queue.
    pub(crate) const fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Number of enqueued tasks.
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// `true` when no tasks are enqueued.
    #[allow(dead_code, reason = "pending worker loop wire-up")]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Appends a task to the back of the queue.
    pub(crate) fn push(&mut self, task_ref: TaskRef, tasks: &mut Slab<TaskSlot>) {
        let key = kwokka_core::slab::SlabKey::new(task_ref.index(), task_ref.generation());
        let Some(slot) = tasks.get_mut(key) else {
            return;
        };
        slot.header_mut().next_runnable = None;

        if let Some(tail_ref) = self.tail {
            let tail_key = kwokka_core::slab::SlabKey::new(tail_ref.index(), tail_ref.generation());
            if let Some(tail_slot) = tasks.get_mut(tail_key) {
                tail_slot.header_mut().next_runnable = Some(task_ref);
            }
        } else {
            self.head = Some(task_ref);
        }
        self.tail = Some(task_ref);
        self.len += 1;
    }

    /// Removes and returns the task at the front of the queue.
    pub(crate) fn pop(&mut self, tasks: &Slab<TaskSlot>) -> Option<TaskRef> {
        let head_ref = self.head?;
        let key = kwokka_core::slab::SlabKey::new(head_ref.index(), head_ref.generation());
        let next = tasks.get(key).and_then(|slot| slot.header().next_runnable);

        self.head = next;
        if next.is_none() {
            self.tail = None;
        }
        self.len -= 1;
        Some(head_ref)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    fn insert_dummy(slab: &mut Slab<TaskSlot>) -> TaskRef {
        use core::{
            future::Future,
            pin::Pin,
            task::{Context, Poll},
        };

        use crate::task::cell::header::Slot;

        struct DummyFut;
        impl Future for DummyFut {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }

        let cell = Slot::new(
            kwokka_core::id::Pip::detached(),
            kwokka_core::id::Namespace::ROOT,
            DummyFut,
        )
        .into_erased();
        let Ok(key) = slab.insert(cell) else {
            panic!("insert must succeed: slab sized for test");
        };
        TaskRef::from_slab(0, key)
    }

    #[test]
    fn empty_queue_pop_returns_none() {
        let slab = Slab::<TaskSlot>::new(0);
        let mut queue = LocalRunQueue::new();
        assert!(queue.pop(&slab).is_none());
        assert!(queue.is_empty());
    }

    #[test]
    fn push_pop_single_entry() {
        let mut slab = Slab::new(1);
        let task = insert_dummy(&mut slab);
        let mut queue = LocalRunQueue::new();

        queue.push(task, &mut slab);
        assert_eq!(queue.len(), 1);

        let popped = queue.pop(&slab);
        assert_eq!(popped, Some(task));
        assert!(queue.is_empty());
    }

    #[test]
    fn fifo_order_preserved() {
        let mut slab = Slab::new(3);
        let t0 = insert_dummy(&mut slab);
        let t1 = insert_dummy(&mut slab);
        let t2 = insert_dummy(&mut slab);
        let mut queue = LocalRunQueue::new();

        queue.push(t0, &mut slab);
        queue.push(t1, &mut slab);
        queue.push(t2, &mut slab);
        assert_eq!(queue.len(), 3);

        assert_eq!(queue.pop(&slab), Some(t0));
        assert_eq!(queue.pop(&slab), Some(t1));
        assert_eq!(queue.pop(&slab), Some(t2));
        assert!(queue.is_empty());
    }

    #[test]
    fn interleaved_push_pop() {
        let mut slab = Slab::new(2);
        let t0 = insert_dummy(&mut slab);
        let t1 = insert_dummy(&mut slab);
        let mut queue = LocalRunQueue::new();

        queue.push(t0, &mut slab);
        assert_eq!(queue.pop(&slab), Some(t0));

        queue.push(t1, &mut slab);
        assert_eq!(queue.pop(&slab), Some(t1));

        assert!(queue.is_empty());
    }
}
