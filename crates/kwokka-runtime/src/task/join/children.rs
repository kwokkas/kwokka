//! Intrusive children list helpers -- operate on `first_child` /
//! `next_sibling` fields embedded in [`TaskHeader`].
//!
//! Same-worker only. The owning worker holds `&mut S` across any
//! mutation, so the list never sees concurrent writers and the helpers
//! take no locks.
//!
//! # Storage abstraction
//!
//! Helpers are generic over a [`TaskStorage`] lookup so the same code
//! drives the per-worker slab in production and a fixture in tests.
#![allow(
    clippy::redundant_pub_crate,
    reason = "satisfies the workspace `unreachable_pub` lint on a private module"
)]
#![expect(
    dead_code,
    reason = "get_slot_ptr is the slot-wide accessor reserved for the immediate-handle join path; 0.1.0 join reads through Slab::slot_ptr"
)]

use core::fmt;

use crate::task::{TaskRef, cell::header::TaskHeader};

/// Lookup interface for resolving a [`TaskRef`] into a [`TaskHeader`].
///
/// Implementors model how the runtime stores task headers. The
/// same-worker invariant means callers always have `&mut self` on the
/// storage when mutating, so the trait does not require interior
/// mutability.
pub(crate) trait TaskStorage {
    /// Returns a shared reference to the header, or `None` if the slot
    /// is empty or the generation packed in `task_ref` no longer
    /// matches.
    fn get(&self, task_ref: TaskRef) -> Option<&TaskHeader>;

    /// Mutable counterpart to [`Self::get`].
    fn get_mut(&mut self, task_ref: TaskRef) -> Option<&mut TaskHeader>;

    /// Returns a raw pointer to the start of the slot backing
    /// `task_ref`, or `None` if the slot is empty or the generation
    /// no longer matches.
    ///
    /// Slot-local readers (notably the join handle, which reads the
    /// cell's `output` at `OUTPUT_OFFSET = size_of::<TaskHeader>()`)
    /// need provenance over the entire `Slot<F>` allocation, not just
    /// the leading `TaskHeader` prefix. `get` / `get_mut` only expose
    /// the header, which would Stacked-Borrows-violate as soon as the
    /// reader stepped past the header into `cell`. Returning a raw
    /// pointer lets the caller cast once and form a fresh, slot-wide
    /// retag at the use site.
    ///
    /// The `*const u8` choice is deliberate: storage is type-erased
    /// over the future type `F`, so the returned pointer must not
    /// commit to any concrete `Slot<F>` shape. The caller knows `F`
    /// (via `TaskHandle<T, M>`'s output type witness) and casts at
    /// the use site.
    ///
    /// # Safety
    ///
    /// * The pointer is valid only while `self` is alive and the slot has not been reclaimed.
    ///   Storage owns the slot.
    /// * Callers must not retain the pointer past the next mutation of `self` that could reclaim or
    ///   recycle the slot.
    /// * Provenance covers the full slot. Use it as `*const Slot<F>` for the `F` that this
    ///   `TaskRef` was created with.
    fn get_slot_ptr(&self, task_ref: TaskRef) -> Option<*const u8>;
}

/// Failure mode for children-list manipulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildrenError {
    /// The parent [`TaskRef`] resolves to no live header -- either the
    /// slot is empty or its generation has rolled past the embedded
    /// one.
    ParentNotFound,
    /// The child [`TaskRef`] resolves to no live header, or
    /// [`remove_child`] could not find the child anywhere in the
    /// parent's list.
    ChildNotFound,
}

impl fmt::Display for ChildrenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParentNotFound => f.write_str("parent task header not found"),
            Self::ChildNotFound => f.write_str("child task header not found"),
        }
    }
}

impl core::error::Error for ChildrenError {}

/// Pushes `child` to the front of `parent`'s children list. O(1).
///
/// The previous head, if any, becomes `child.next_sibling`. The child
/// must already be `first_child = None` and `next_sibling = None`; the
/// helper does not validate that -- callers either pass a
/// freshly-spawned task or a task already detached via
/// [`remove_child`].
///
/// # Errors
///
/// * [`ChildrenError::ParentNotFound`] if `parent` no longer resolves.
/// * [`ChildrenError::ChildNotFound`] if `child` no longer resolves.
pub(crate) fn push_child<S: TaskStorage>(
    storage: &mut S,
    parent: TaskRef,
    child: TaskRef,
) -> Result<(), ChildrenError> {
    let prev_first = storage
        .get(parent)
        .ok_or(ChildrenError::ParentNotFound)?
        .first_child;
    let child_header = storage.get_mut(child).ok_or(ChildrenError::ChildNotFound)?;
    child_header.next_sibling = prev_first;
    let parent_header = storage
        .get_mut(parent)
        .ok_or(ChildrenError::ParentNotFound)?;
    parent_header.first_child = Some(child);
    Ok(())
}

/// Iterates over `parent`'s immediate children. Returns an empty
/// iterator when `parent` does not resolve -- by design, since callers
/// commonly walk children during cleanup paths where a missing parent
/// is benign.
///
/// Depth-first traversal is the caller's responsibility.
pub(crate) fn iter_children<S: TaskStorage>(storage: &S, parent: TaskRef) -> ChildrenIter<'_, S> {
    let next = storage.get(parent).and_then(|h| h.first_child);
    ChildrenIter { storage, next }
}

/// Removes `child` from `parent`'s list. O(N) over the list length.
/// Lists are expected to be short (handful of direct children per
/// task), so the linear walk is acceptable; trading O(1) removal for
/// an extra `prev_sibling` pointer would add 50% to the list-pointer
/// overhead (two fields to three).
///
/// Detaches by setting `child.next_sibling = None` so the removed task
/// is safe to re-attach via [`push_child`].
///
/// # Errors
///
/// * [`ChildrenError::ParentNotFound`] if `parent` no longer resolves.
/// * [`ChildrenError::ChildNotFound`] if `child` is not in the list.
pub(crate) fn remove_child<S: TaskStorage>(
    storage: &mut S,
    parent: TaskRef,
    child: TaskRef,
) -> Result<(), ChildrenError> {
    let position = locate(storage, parent, child)?;
    let child_next = storage
        .get(child)
        .ok_or(ChildrenError::ChildNotFound)?
        .next_sibling;
    match position {
        RemovePosition::Head => {
            let parent_header = storage
                .get_mut(parent)
                .ok_or(ChildrenError::ParentNotFound)?;
            parent_header.first_child = child_next;
        }
        RemovePosition::AfterPrev(prev) => {
            let prev_header = storage.get_mut(prev).ok_or(ChildrenError::ChildNotFound)?;
            prev_header.next_sibling = child_next;
        }
    }
    let child_header = storage.get_mut(child).ok_or(ChildrenError::ChildNotFound)?;
    child_header.next_sibling = None;
    Ok(())
}

/// Concrete iterator returned by [`iter_children`].
pub(crate) struct ChildrenIter<'a, S> {
    storage: &'a S,
    next: Option<TaskRef>,
}

impl<S: TaskStorage> Iterator for ChildrenIter<'_, S> {
    type Item = TaskRef;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        let header = self.storage.get(current)?;
        self.next = header.next_sibling;
        Some(current)
    }
}

/// Where in `parent`'s list `child` lives. Hidden detail of
/// [`remove_child`].
enum RemovePosition {
    Head,
    AfterPrev(TaskRef),
}

fn locate<S: TaskStorage>(
    storage: &S,
    parent: TaskRef,
    child: TaskRef,
) -> Result<RemovePosition, ChildrenError> {
    let parent_header = storage.get(parent).ok_or(ChildrenError::ParentNotFound)?;
    let mut current = parent_header
        .first_child
        .ok_or(ChildrenError::ChildNotFound)?;
    if current == child {
        return Ok(RemovePosition::Head);
    }
    loop {
        let header = storage.get(current).ok_or(ChildrenError::ChildNotFound)?;
        match header.next_sibling {
            Some(next) if next == child => return Ok(RemovePosition::AfterPrev(current)),
            Some(next) => current = next,
            None => return Err(ChildrenError::ChildNotFound),
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        ptr,
        task::{Context, Poll},
    };
    use std::collections::HashMap;

    use kwokka_core::{Generation, id::Pip, namespace::Namespace};

    use super::*;
    use crate::task::cell::header::Slot;

    struct InertFuture;

    impl Future for InertFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    struct MockStorage(HashMap<u64, TaskHeader>);

    impl MockStorage {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn insert_fresh(&mut self, task_ref: TaskRef) {
            self.0.insert(
                task_ref.raw(),
                TaskHeader::new(
                    Pip::detached(),
                    Namespace::ROOT,
                    &Slot::<InertFuture>::VTABLE,
                ),
            );
        }
    }

    impl TaskStorage for MockStorage {
        fn get(&self, task_ref: TaskRef) -> Option<&TaskHeader> {
            self.0.get(&task_ref.raw())
        }

        fn get_mut(&mut self, task_ref: TaskRef) -> Option<&mut TaskHeader> {
            self.0.get_mut(&task_ref.raw())
        }

        fn get_slot_ptr(&self, task_ref: TaskRef) -> Option<*const u8> {
            self.0
                .get(&task_ref.raw())
                .map(|h| ptr::from_ref(h).cast::<u8>())
        }
    }

    const fn fake_ref(worker_id: u8, generation: u32, index: u32) -> TaskRef {
        TaskRef::from_arena(worker_id, index, Generation::from_raw(generation))
    }

    fn collect_children(storage: &MockStorage, parent: TaskRef) -> Vec<TaskRef> {
        iter_children(storage, parent).collect()
    }

    #[test]
    fn push_child_into_empty_parent_sets_first_child() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let child = fake_ref(0, 1, 2);
        storage.insert_fresh(parent);
        storage.insert_fresh(child);
        let Ok(()) = push_child(&mut storage, parent, child) else {
            panic!("push into empty parent must succeed");
        };
        let Some(parent_header) = storage.get(parent) else {
            panic!("parent must still resolve");
        };
        assert_eq!(parent_header.first_child, Some(child));
        let Some(child_header) = storage.get(child) else {
            panic!("child must still resolve");
        };
        assert!(child_header.next_sibling.is_none());
    }

    #[test]
    fn push_child_chains_in_lifo_order() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let first = fake_ref(0, 1, 2);
        let second = fake_ref(0, 1, 3);
        storage.insert_fresh(parent);
        storage.insert_fresh(first);
        storage.insert_fresh(second);
        let Ok(()) = push_child(&mut storage, parent, first) else {
            panic!("first push must succeed");
        };
        let Ok(()) = push_child(&mut storage, parent, second) else {
            panic!("second push must succeed");
        };
        let Some(parent_header) = storage.get(parent) else {
            panic!("parent must still resolve");
        };
        assert_eq!(parent_header.first_child, Some(second));
        let Some(second_header) = storage.get(second) else {
            panic!("second must still resolve");
        };
        assert_eq!(second_header.next_sibling, Some(first));
    }

    #[test]
    fn iter_children_on_empty_parent_yields_nothing() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        storage.insert_fresh(parent);
        assert!(collect_children(&storage, parent).is_empty());
    }

    #[test]
    fn iter_children_on_missing_parent_yields_nothing() {
        let storage = MockStorage::new();
        let parent = fake_ref(0, 1, 99);
        assert!(collect_children(&storage, parent).is_empty());
    }

    #[test]
    fn iter_children_yields_three_children_in_lifo_order() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let first = fake_ref(0, 1, 2);
        let second = fake_ref(0, 1, 3);
        let third = fake_ref(0, 1, 4);
        storage.insert_fresh(parent);
        storage.insert_fresh(first);
        storage.insert_fresh(second);
        storage.insert_fresh(third);
        for child in [first, second, third] {
            let Ok(()) = push_child(&mut storage, parent, child) else {
                panic!("push must succeed for {child:?}");
            };
        }
        assert_eq!(collect_children(&storage, parent), [third, second, first]);
    }

    #[test]
    fn remove_head_unlinks_first_child() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let head = fake_ref(0, 1, 2);
        let tail = fake_ref(0, 1, 3);
        storage.insert_fresh(parent);
        storage.insert_fresh(head);
        storage.insert_fresh(tail);
        let Ok(()) = push_child(&mut storage, parent, tail) else {
            panic!("tail push must succeed");
        };
        let Ok(()) = push_child(&mut storage, parent, head) else {
            panic!("head push must succeed");
        };
        let Ok(()) = remove_child(&mut storage, parent, head) else {
            panic!("remove head must succeed");
        };
        assert_eq!(collect_children(&storage, parent), [tail]);
        let Some(head_header) = storage.get(head) else {
            panic!("head must still resolve");
        };
        assert!(head_header.next_sibling.is_none());
    }

    #[test]
    fn remove_middle_relinks_neighbors() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let first = fake_ref(0, 1, 2);
        let second = fake_ref(0, 1, 3);
        let third = fake_ref(0, 1, 4);
        for task_ref in [parent, first, second, third] {
            storage.insert_fresh(task_ref);
        }
        for child in [first, second, third] {
            let Ok(()) = push_child(&mut storage, parent, child) else {
                panic!("push must succeed");
            };
        }
        let Ok(()) = remove_child(&mut storage, parent, second) else {
            panic!("remove middle must succeed");
        };
        assert_eq!(collect_children(&storage, parent), [third, first]);
    }

    #[test]
    fn remove_tail_terminates_chain() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let head = fake_ref(0, 1, 2);
        let tail = fake_ref(0, 1, 3);
        for r in [parent, head, tail] {
            storage.insert_fresh(r);
        }
        let Ok(()) = push_child(&mut storage, parent, tail) else {
            panic!("tail push must succeed");
        };
        let Ok(()) = push_child(&mut storage, parent, head) else {
            panic!("head push must succeed");
        };
        let Ok(()) = remove_child(&mut storage, parent, tail) else {
            panic!("remove tail must succeed");
        };
        assert_eq!(collect_children(&storage, parent), [head]);
    }

    #[test]
    fn remove_unknown_child_returns_child_not_found() {
        let mut storage = MockStorage::new();
        let parent = fake_ref(0, 1, 1);
        let live = fake_ref(0, 1, 2);
        let alien = fake_ref(0, 1, 3);
        for r in [parent, live, alien] {
            storage.insert_fresh(r);
        }
        let Ok(()) = push_child(&mut storage, parent, live) else {
            panic!("seed push must succeed");
        };
        match remove_child(&mut storage, parent, alien) {
            Err(ChildrenError::ChildNotFound) => {}
            other => panic!("expected ChildNotFound, got {other:?}"),
        }
    }

    #[test]
    fn push_with_missing_parent_returns_parent_not_found() {
        let mut storage = MockStorage::new();
        let child = fake_ref(0, 1, 2);
        storage.insert_fresh(child);
        let stale_parent = fake_ref(0, 1, 99);
        match push_child(&mut storage, stale_parent, child) {
            Err(ChildrenError::ParentNotFound) => {}
            other => panic!("expected ParentNotFound, got {other:?}"),
        }
    }

    #[test]
    fn stale_generation_is_indistinguishable_from_missing() {
        let mut storage = MockStorage::new();
        let live_parent = fake_ref(0, 1, 1);
        storage.insert_fresh(live_parent);
        let stale_parent = fake_ref(0, 2, 1);
        let child = fake_ref(0, 1, 2);
        storage.insert_fresh(child);
        match push_child(&mut storage, stale_parent, child) {
            Err(ChildrenError::ParentNotFound) => {}
            other => panic!("stale generation must surface as ParentNotFound, got {other:?}"),
        }
    }
}
