//! Type-erased operations on the slab-stored [`TaskSlot`] cell.
//!
//! Every pointer reinterpretation of the erased cell lives here, so
//! [`slot`](crate::task::cell::slot) stays free of the `unsafe` keyword even
//! though its `Drop` calls [`TaskSlot::drop_via_vtable`]. The typed half --
//! building a [`Slot<F>`](crate::task::cell::layout::Slot) and the vtable
//! bodies that recover `F` -- stays in
//! [`layout`](crate::task::cell::layout).

#![allow(
    clippy::redundant_pub_crate,
    reason = "satisfies the workspace `unreachable_pub` lint on a private module"
)]

use core::{
    ptr::{self, NonNull},
    task::{Context, Poll},
};

use crate::task::cell::{header::TaskHeader, slot::TaskSlot, state::TaskState};

impl TaskSlot {
    /// Borrows the [`TaskHeader`] at the cell's offset 0.
    pub(crate) const fn header(&self) -> &TaskHeader {
        // SAFETY:
        // Invariant: an occupied cell holds an initialized `Slot<F>` whose
        //   `TaskHeader` is at offset 0; the pointer derives from the full
        //   cell base, so provenance covers the whole `Slot<F>`. `TaskSlot`
        //   wraps its bytes in an `UnsafeCell`, so a shared `&TaskSlot`
        //   carries SharedReadWrite provenance over the cell -- the leading
        //   `AtomicTaskState` can be read through the resulting `&TaskHeader`
        //   without a Stacked/Tree Borrows violation.
        // Precondition: called only on a cell filled by `Slot::into_erased`
        //   (every TaskSlot reachable through the slab was so filled, per the
        //   slab occupancy gate).
        // Failure: reading an unfilled cell would materialize a garbage
        //   header; aliasing is excluded by the shared borrow.
        unsafe { &*ptr::from_ref(self).cast::<TaskHeader>() }
    }

    /// Exclusively borrows the [`TaskHeader`] at the cell's offset 0.
    pub(crate) fn header_mut(&mut self) -> &mut TaskHeader {
        // SAFETY:
        // Invariant and precondition match [`TaskSlot::header`]; the exclusive
        // borrow additionally rules out aliasing. Failure mode is identical:
        // an unfilled cell would expose a garbage header.
        unsafe { &mut *ptr::from_mut(self).cast::<TaskHeader>() }
    }

    /// Polls the erased future through its stamped vtable.
    ///
    /// The caller must have transitioned the header state to
    /// [`TaskState::Running`] before calling.
    pub(crate) fn poll_via_vtable(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY:
        // Invariant: the `NonNull` is built from the full 512-byte cell base,
        //   so it carries the slot-wide provenance `Slot::poll_fn` requires;
        //   the vtable was stamped by `Slot::<F>::VTABLE` for the `F` occupying
        //   the cell, so `poll` matches that `F`.
        // Precondition: the caller transitioned `Woken -> Running` immediately
        //   before this call, so the future half is still initialized; `&mut
        //   self` proves exclusive access (no concurrent poll or reclaim).
        // Failure: a vtable/`F` mismatch or a recycled-generation cell would
        //   call `poll_fn` with a wrong-typed pointer (type-confusion UB); a
        //   non-`Running` entry would let `poll_fn` double-write the output.
        let (header_ptr, poll) = unsafe {
            let header_ptr = NonNull::new_unchecked(ptr::from_mut(self).cast::<TaskHeader>());
            (header_ptr, header_ptr.as_ref().vtable.poll)
        };
        poll(header_ptr, cx)
    }

    /// Drops the still-live half of the erased cell through its vtable.
    ///
    /// Invoked once from [`TaskSlot`]'s `Drop`; the slab fires that for each
    /// occupied slot exactly once.
    pub(crate) fn drop_via_vtable(&mut self) {
        // SAFETY:
        // Invariant: an occupied cell holds an initialized `Slot<F>`, header at
        //   offset 0; `drop_in_place` (stamped by `Slot::<F>::VTABLE`) drops
        //   exactly the state-selected live half.
        // Precondition: runs only from `TaskSlot::drop`, which the slab fires
        //   once per occupied slot (generation gate); `&mut self` is exclusive.
        // Failure: invoking on an unfilled cell would read a garbage vtable; a
        //   second invocation would double-drop -- excluded because the slab's
        //   `drop` and `remove` are mutually exclusive on occupancy parity.
        let (header_ptr, drop_in_place) = unsafe {
            let header_ptr = NonNull::new_unchecked(ptr::from_mut(self).cast::<TaskHeader>());
            (header_ptr, header_ptr.as_ref().vtable.drop_in_place)
        };
        drop_in_place(header_ptr);
    }

    /// Moves the task's output out of the erased cell, consuming it.
    ///
    /// Transitions the header `Done -> Taken` so the output is read at most
    /// once, then moves the `T` value out of the cell at
    /// [`Slot::OUTPUT_OFFSET`](crate::task::cell::layout::Slot::OUTPUT_OFFSET). After
    /// this returns, [`TaskSlot::drop_via_vtable`]
    /// observes `Taken` and drops neither half: `poll_fn` already dropped the
    /// future on `Ready`, and the output has just been moved out here.
    ///
    /// `T` must be the `Output` of the `F` erased into this cell. The caller
    /// (which spawned the task and so knows `F`) supplies it; the read offset
    /// is `F`-independent and is proven correct for every spawned `F` by
    /// [`Slot::OUTPUT_OFFSET_OK`](crate::task::cell::layout::Slot::OUTPUT_OFFSET_OK).
    ///
    /// # Panics
    ///
    /// Panics if the header is not in [`TaskState::Done`]. The caller must
    /// observe `Done` before consuming the output.
    pub(crate) fn take_output<T>(&mut self) -> T {
        let Ok(()) = self
            .header()
            .state
            .transition(TaskState::Done, TaskState::Taken)
        else {
            panic!("take_output requires the task to be in the Done state");
        };
        // SAFETY:
        // Invariant: an occupied cell holds an initialized `Slot<F>` whose
        //   `output` sits at `OUTPUT_OFFSET == size_of::<TaskHeader>()` --
        //   enforced for every spawned `F` by `Slot::OUTPUT_OFFSET_OK` at
        //   `into_erased`. After `Done`, `poll_fn` wrote `output` and dropped
        //   the future, so the output half is initialized and `T == F::Output`.
        // Precondition: the `Done -> Taken` transition above just succeeded, so
        //   the state was `Done` and this is the sole consume; `T == F::Output`
        //   is the caller's contract; `&mut self` proves exclusive access.
        // Failure: a wrong `T` reads a wrong-typed value (type confusion); a
        //   read before `Done` would read uninitialized memory; a second
        //   `take_output` would double-move -- excluded because `Done -> Taken`
        //   fails on the second call.
        unsafe {
            ptr::from_mut(self)
                .cast::<T>()
                .byte_add(size_of::<TaskHeader>())
                .read()
        }
    }

    /// Moves the task's output out of the erased cell through a shared
    /// reference, consuming it.
    ///
    /// The shared sibling of [`take_output`](TaskSlot::take_output): the join
    /// handle reaches a child slot through a shared `slot_ptr` at a Vec index
    /// disjoint from the worker's live parent `&mut`, so it cannot form `&mut
    /// self`. The `Done -> Taken` atomic transition guards the single move, and
    /// `TaskSlot`'s `UnsafeCell` makes the byte read sound from a shared
    /// reference. `T` must be the `Output` of the `F` erased into this cell, as
    /// for [`take_output`](TaskSlot::take_output).
    ///
    /// # Panics
    ///
    /// Panics if the header is not in [`TaskState::Done`]. The caller must
    /// observe `Done` before consuming the output.
    pub(crate) fn take_output_shared<T>(&self) -> T {
        let Ok(()) = self
            .header()
            .state
            .transition(TaskState::Done, TaskState::Taken)
        else {
            panic!("take_output_shared requires the task to be in the Done state");
        };
        // SAFETY:
        // Invariant: an occupied cell holds an initialized `Slot<F>` whose
        //   `output` sits at `OUTPUT_OFFSET == size_of::<TaskHeader>()` --
        //   enforced for every spawned `F` by `Slot::OUTPUT_OFFSET_OK`. After
        //   `Done`, `poll_fn` wrote `output` and dropped the future, so the
        //   output half is initialized and `T == F::Output`. `TaskSlot` wraps
        //   its bytes in an `UnsafeCell`, so the outer shared `&TaskSlot` does
        //   not freeze the interior: the byte move-out (a read, not a write)
        //   through this shared reference is in-bounds and sound.
        // Precondition: the `Done -> Taken` transition above just succeeded, so
        //   the state was `Done` and this is the sole consume; `T == F::Output`
        //   is the caller's contract. The slot is a distinct Vec index from the
        //   worker's live parent `&mut`, so this shared read does not alias it.
        // Failure: a wrong `T` reads a wrong-typed value (type confusion); a
        //   read before `Done` reads uninitialized memory; a second take would
        //   double-move -- excluded because `Done -> Taken` fails on the second.
        unsafe {
            ptr::from_ref(self)
                .cast::<T>()
                .byte_add(size_of::<TaskHeader>())
                .read()
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{RawWaker, RawWakerVTable, Waker},
    };

    use kwokka_core::id::{Namespace, Pip};

    use super::*;
    use crate::task::cell::layout::Slot;

    #[derive(Default)]
    pub(super) struct DropCounts {
        pub(super) future: AtomicUsize,
        pub(super) output: AtomicUsize,
    }

    /// Future that records its drop in `counts.future` and yields a value
    /// whose drop records into `counts.output`.
    pub(super) struct ProbeFuture {
        pub(super) counts: &'static DropCounts,
        pub(super) ready: bool,
    }

    pub(super) struct ProbeOutput {
        counts: &'static DropCounts,
    }

    impl Drop for ProbeOutput {
        fn drop(&mut self) {
            self.counts.output.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Drop for ProbeFuture {
        fn drop(&mut self) {
            self.counts.future.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Future for ProbeFuture {
        type Output = ProbeOutput;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<ProbeOutput> {
            if self.ready {
                Poll::Ready(ProbeOutput {
                    counts: self.counts,
                })
            } else {
                Poll::Pending
            }
        }
    }

    pub(super) const fn dummy_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        // SAFETY: vtable pointers are no-ops that never dereference `data`.
        // Violating this would dereference a null pointer.
        unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) }
    }

    #[test]
    fn task_slot_matches_cell_budget() {
        assert_eq!(size_of::<TaskSlot>(), TaskSlot::CELL_BYTES);
        assert_eq!(align_of::<TaskSlot>(), TaskSlot::CELL_ALIGN);
    }

    #[test]
    fn poll_via_vtable_ready_drops_future_then_done_drops_output() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: true,
            },
        )
        .into_erased();
        let Ok(()) = cell
            .header()
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(cell.poll_via_vtable(&mut cx), Poll::Ready(())));
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
        let Ok(()) = cell.header().state.complete() else {
            panic!("Running -> Done must succeed");
        };
        drop(cell);
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            1,
            "a Done cell drops the written output via the vtable",
        );
    }

    #[test]
    fn poll_via_vtable_pending_keeps_future_live() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        )
        .into_erased();
        let Ok(()) = cell
            .header()
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(cell.poll_via_vtable(&mut cx), Poll::Pending));
        assert_eq!(
            COUNTS.future.load(Ordering::Relaxed),
            0,
            "Pending leaves the future live",
        );
        drop(cell);
        assert_eq!(
            COUNTS.future.load(Ordering::Relaxed),
            1,
            "dropping a Running cell drops the still-live future",
        );
    }

    #[test]
    fn drop_via_vtable_on_sleeping_drops_future() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        )
        .into_erased();
        drop(cell);
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
        assert_eq!(COUNTS.output.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn output_offset_ok_holds_for_probe() {
        let () = Slot::<ProbeFuture>::OUTPUT_OFFSET_OK;
    }

    #[test]
    fn take_output_moves_value_and_marks_taken() {
        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let mut cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: true,
            },
        )
        .into_erased();
        let Ok(()) = cell
            .header()
            .state
            .transition(TaskState::Sleeping, TaskState::Running)
        else {
            panic!("Sleeping -> Running must succeed");
        };
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        // IGNORE: poll result is not under test; Ready is needed to write the output.
        let _ = cell.poll_via_vtable(&mut cx);
        let Ok(()) = cell.header().state.complete() else {
            panic!("Running -> Done must succeed");
        };
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            0,
            "output is written by poll_fn, not yet dropped",
        );

        let output = cell.take_output::<ProbeOutput>();
        assert_eq!(cell.header().state.load(), TaskState::Taken);
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            0,
            "take_output moves the value out without dropping it",
        );

        drop(output);
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            1,
            "dropping the moved-out value runs its drop exactly once",
        );

        drop(cell);
        assert_eq!(
            COUNTS.output.load(Ordering::Relaxed),
            1,
            "a Taken cell drops neither half, so the output is not double-dropped",
        );
        assert_eq!(COUNTS.future.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn slab_drop_releases_occupied_erased_future() {
        use kwokka_core::slab::Slab;

        static COUNTS: DropCounts = DropCounts {
            future: AtomicUsize::new(0),
            output: AtomicUsize::new(0),
        };
        let cell = Slot::new(
            Pip::detached(),
            Namespace::ROOT,
            ProbeFuture {
                counts: &COUNTS,
                ready: false,
            },
        )
        .into_erased();
        {
            let mut slab = Slab::<TaskSlot>::new(1);
            let Ok(_) = slab.insert(cell) else {
                panic!("insert into a fresh slab must succeed");
            };
        }
        assert_eq!(
            COUNTS.future.load(Ordering::Relaxed),
            1,
            "dropping the slab releases the occupied future via the vtable",
        );
    }
}
