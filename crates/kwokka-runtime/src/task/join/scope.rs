//! Structured-concurrency scope -- the entry a running task uses to spawn
//! children under itself.
//!
//! [`scope`] borrows the polling task a [`Scope`] for the duration of the call.
//! The task spawns children through [`Scope::spawn`], which routes each child
//! into the worker's spawn inbox; the worker stamps the child's id and links it
//! under the parent when it drains the inbox after the poll (see
//! [`cycle`](crate::worker::cycle)). The scope reaches its worker by decoding the
//! polling task's [`TaskRef`] from the waker, so it must be awaited directly --
//! not inside a `select!`/`join!` branch that wraps the waker.
//!
//! [`Scope::spawn`] is defined for both [`Affine`] (pinned children) and
//! [`Stealing`] (migrating, `Send`-bounded) modes; [`scope`] opens an affine
//! scope and [`scope_send`] a stealing one. Child futures are `'static` and the
//! builder is a non-async closure.
//! The scope awaits every child before it resolves, and the worker reaps each
//! settled child's slot after the poll.
//!
//! There is no eager cancel-on-exit, and the normal path needs none: [`scope`]
//! resolves only once every child has settled, so a scope that returns leaves
//! no live child. An abnormal exit reclaims the children at slot teardown
//! rather than cancelling them eagerly -- a panic in the scoped task unwinds
//! past the worker (there is no per-poll catch boundary yet), and a task
//! dropped before settlement frees its children's slots when its own slot
//! tears down through the task vtable drop. Eager cancellation of an abandoned
//! scope's live children waits on that catch boundary, in a later release.
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on the module-private ScopeFuture"
)]

use core::{
    error::Error,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    ptr,
    task::{Context, Poll},
};

use kwokka_core::id::{Namespace, Pip};

use crate::{
    task::{
        TaskRef,
        cell::{header::Slot, slot::TaskSlot},
        marker::{Affine, Mode, Stealing},
        waker,
    },
    worker::{WorkerId, poll::polling, queue::inbox::PendingSpawn},
};

/// The structured scope of the running task.
///
/// Borrowed to the closure passed to [`scope`]. Carries the polling task's
/// [`TaskRef`] (the structured parent of every child spawned here) and its
/// [`WorkerId`], both decoded from the waker at scope entry. The `M` mode
/// parameter selects the children's migration class: an [`Affine`] scope
/// spawns pinned children with no `Send` bound, a [`Stealing`] scope spawns
/// migrating children whose bound carries `Send`. Cross-class misuse is a
/// compile error.
pub struct Scope<'scope, M: Mode> {
    parent: TaskRef,
    worker: WorkerId,
    _life: PhantomData<&'scope ()>,
    _mode: PhantomData<M>,
}

impl<M: Mode> Scope<'_, M> {
    /// Routes an erased child cell into the worker's spawn inbox.
    ///
    /// The worker stamps the child's id and links it under this task when
    /// it drains the inbox after the current poll.
    fn enqueue_child(&self, cell: TaskSlot) -> Result<(), SpawnError> {
        let request = PendingSpawn {
            parent: self.parent,
            cell,
        };
        match polling::with_current(self.worker, |frame| frame.push_spawn(request)) {
            None => Err(SpawnError::NoActiveFrame),
            Some(None) => Ok(()),
            Some(Some(_returned)) => Err(SpawnError::InboxFull),
        }
    }
}

impl Scope<'_, Affine> {
    /// Spawns a child future under this scope.
    ///
    /// Moves `future` into a type-erased cell and routes it into the worker's
    /// spawn inbox. The worker stamps the child's id and links it under this
    /// task when it drains the inbox after the current poll. The child id is
    /// issued at drain time, so this returns no handle: children are reaped
    /// through the Pip-tree in a later phase.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError::InboxFull`] when the worker's spawn inbox is at
    /// capacity for this poll -- the child future is dropped, applying
    /// backpressure to the caller. Returns [`SpawnError::NoActiveFrame`] when
    /// called outside a poll on the owning worker, where no frame is installed.
    pub fn spawn<Fut: Future<Output = ()> + 'static>(&self, future: Fut) -> Result<(), SpawnError> {
        let mut slot = Slot::new(Pip::detached(), Namespace::ROOT, future);
        // The missing Send bound is sound because the child is pinned: a
        // pinned task is never offered to the steal path, so it lives and
        // dies on the worker that spawned it.
        slot.header.is_pinned = true;
        self.enqueue_child(slot.into_erased())
    }
}

impl Scope<'_, Stealing> {
    /// Spawns a migrating child future under this scope.
    ///
    /// The sending counterpart of the unbounded spawn: the child carries
    /// the `Send` bound, so the steal path may relocate it to a sibling
    /// worker. Routing is unchanged -- the cell enters the worker's spawn
    /// inbox and the post-poll drain links it under this task. The child id
    /// is issued at drain time, so this returns no handle.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError::InboxFull`] when the worker's spawn inbox is at
    /// capacity for this poll -- the child future is dropped, applying
    /// backpressure to the caller. Returns [`SpawnError::NoActiveFrame`] when
    /// no poll frame is installed on the owning worker.
    pub fn spawn<Fut: Future<Output = ()> + Send + 'static>(
        &self,
        future: Fut,
    ) -> Result<(), SpawnError> {
        let cell = Slot::new(Pip::detached(), Namespace::ROOT, future).into_erased();
        self.enqueue_child(cell)
    }
}

/// Reason a [`Scope::spawn`] did not enqueue a child.
///
/// `#[non_exhaustive]` so a future variant (e.g. a bounded-backpressure signal)
/// can be added without a major version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpawnError {
    /// The worker's spawn inbox was full for this poll; the child was dropped.
    InboxFull,
    /// `spawn` was called without an active poll frame on the owning worker.
    NoActiveFrame,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InboxFull => {
                "worker spawn inbox full; increase via RuntimeBuilder::task_capacity"
            }
            Self::NoActiveFrame => "scope.spawn called outside an active poll frame",
        })
    }
}

impl Error for SpawnError {}

/// Runs `builder` with a [`Scope`] for the running task, then awaits its children.
///
/// The builder runs once on the first poll -- a non-async closure, so awaiting
/// inside it is not available in 0.1.0. The future then stays `Pending` until
/// every successfully spawned child has settled, so the scope never resolves
/// while a child is still live. That is the structural-concurrency guarantee.
/// Children are linked under the parent by the post-poll drain and observed
/// through the Pip-tree. Only children whose `spawn` returned `Ok` participate;
/// a child dropped on [`SpawnError::InboxFull`] is never linked and does not
/// block settlement.
///
/// 0.1.0 reclaims each settled child's slot after the poll that observes
/// settlement: the worker records this scope's parent and the post-poll reap
/// frees the children's slots, so a scope re-run in a loop does not accumulate
/// them. If the per-worker reap queue is full for that tick the record is
/// dropped and those children wait for the slab drop -- a bounded leak under a
/// reap storm, never a fault.
///
/// # Panics
///
/// Panics if the polling waker is not the runtime's task waker: a combinator
/// (`select!`/`join!`) that wraps the waker hides the parent [`TaskRef`], so the
/// scope cannot identify its parent. Await `scope` directly, not inside a
/// combinator branch.
pub fn scope<F, T>(builder: F) -> impl Future<Output = T>
where
    F: FnOnce(&Scope<'_, Affine>) -> T + Unpin,
    T: Unpin,
{
    ScopeFuture {
        builder: Some(builder),
        value: None,
        _mode: PhantomData,
    }
}

/// Runs `builder` with a sending [`Scope`], then awaits its children.
///
/// The migrating counterpart of [`scope`]: children spawned here carry the
/// `Send` bound, so the steal path may relocate them to sibling workers.
/// Everything else matches [`scope`] -- the builder runs once on the first
/// poll, the future settles only after every successfully spawned child,
/// and settled children are reaped after the observing poll.
///
/// # Panics
///
/// Panics if the polling waker is not the runtime's task waker: a combinator
/// (`select!`/`join!`) that wraps the waker hides the parent [`TaskRef`], so
/// the scope cannot identify its parent. Await `scope_send` directly, not
/// inside a combinator branch.
pub fn scope_send<F, T>(builder: F) -> impl Future<Output = T>
where
    F: FnOnce(&Scope<'_, Stealing>) -> T + Unpin,
    T: Unpin,
{
    ScopeFuture {
        builder: Some(builder),
        value: None,
        _mode: PhantomData,
    }
}

/// Aborts a [`scope`] entered under a wrapped waker.
///
/// Cold path: a combinator replaced the task waker, so the parent [`TaskRef`]
/// cannot be decoded. Split out so the poll path holds no panic string and the
/// runtime-waker check stays a plain branch (not a `panic!`-in-`if`).
#[cold]
#[inline(never)]
fn combinator_waker_panic() -> ! {
    panic!(
        "scope() requires the runtime task waker; a combinator wrapped it, so \
         the parent task cannot be identified -- await scope() directly, not \
         inside a select!/join! branch"
    );
}

/// Two-state future backing [`scope`]: runs the builder once, then waits until
/// every spawned child has settled.
///
/// Holds the builder (Building) then its value (Draining), never an inner
/// future, so it is `Unpin` and projects through `Pin` without `unsafe`.
pub(crate) struct ScopeFuture<F, T, M: Mode> {
    builder: Option<F>,
    value: Option<T>,
    /// Mode selector behind a fn-pointer phantom, so the future stays
    /// `Unpin` and `Send` regardless of the marker's own auto traits.
    _mode: PhantomData<fn() -> M>,
}

impl<F, T, M: Mode> Future for ScopeFuture<F, T, M>
where
    F: FnOnce(&Scope<'_, M>) -> T + Unpin,
    T: Unpin,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The parent TaskRef is encoded in the polling task's waker. A combinator
        // (select!/join!) that wraps the waker installs a different vtable, so its
        // data pointer would decode to a garbage TaskRef -- fail loudly rather
        // than corrupt child routing.
        if !ptr::eq(
            ptr::from_ref(cx.waker().vtable()),
            ptr::from_ref(&waker::VTABLE),
        ) {
            combinator_waker_panic();
        }
        let parent = waker::data_to_task_ref(cx.waker().data());
        let Ok(worker) = WorkerId::new(parent.worker_id()) else {
            panic!("scope() decoded a non-routable parent worker id from the waker");
        };
        let this = self.get_mut();
        if let Some(builder) = this.builder.take() {
            // Building: run the builder once, then yield unconditionally. The
            // children it spawned are still in the inbox; the post-poll drain
            // links them under the parent before the next poll, so first_child is
            // empty until then -- yielding once avoids a false "all settled".
            let scope = Scope {
                parent,
                worker,
                _life: PhantomData,
                _mode: PhantomData,
            };
            this.value = Some(builder(&scope));
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        // Draining: complete once every child has settled. The frame is always
        // installed here -- this future is polled only inside the parent's own
        // poll via poll_one. A missing frame would be a nested-poll misuse
        // (0.2.0+), handled conservatively as not-yet-settled. On the poll that
        // observes settlement, the parent is recorded for the post-poll reap so
        // its settled children's slots are freed after this poll returns.
        let settled = polling::with_current(worker, |frame| {
            let settled = frame.are_children_all_settled();
            if settled {
                // A full reap queue drops the record (the children wait for the
                // slab drop -- a bounded leak), so the bool return needs no action.
                frame.push_reap(parent);
            }
            settled
        })
        .unwrap_or(false);
        if !settled {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let Some(value) = this.value.take() else {
            panic!("scope future polled after completion");
        };
        Poll::Ready(value)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{ptr::NonNull, task::Waker};

    use kwokka_core::{
        Generation,
        slab::{Slab, SlabKey},
    };

    use super::*;
    use crate::{
        task::cell::{header::WakeData, slot::TaskSlot},
        worker::{
            poll::{
                frame::PollFrame,
                polling::{clear, install},
            },
            queue::{
                inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
                reap::{REAP_QUEUE_CAPACITY, ReapQueue},
            },
        },
    };

    /// A child future that never completes, so a spawned cell stays in the inbox.
    struct Pending;
    impl Future for Pending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    fn worker_id(id: u8) -> WorkerId {
        let Ok(worker) = WorkerId::new(id) else {
            panic!("worker id within range");
        };
        worker
    }

    fn parent_ref(worker: u8) -> TaskRef {
        TaskRef::from_slab(worker, SlabKey::new(0, Generation::from_raw(0)))
    }

    fn affine_scope(parent: TaskRef, worker: WorkerId) -> Scope<'static, Affine> {
        Scope {
            parent,
            worker,
            _life: PhantomData,
            _mode: PhantomData,
        }
    }

    #[test]
    fn spawn_enqueues_into_the_installed_frame() {
        let worker = worker_id(12);
        let parent = parent_ref(12);
        let mut slab = Slab::<TaskSlot>::new(2);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: core::sync::atomic::AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker, &frame);
        let result = affine_scope(parent, worker).spawn(Pending);
        clear(worker);

        assert_eq!(result, Ok(()));
        assert_eq!(inbox.len(), 1, "the spawn pushed one child into the inbox");
    }

    #[test]
    fn spawn_full_inbox_is_err() {
        let worker = worker_id(13);
        let parent = parent_ref(13);
        let mut slab = Slab::<TaskSlot>::new(2);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: core::sync::atomic::AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker, &frame);
        let scope = affine_scope(parent, worker);
        for _ in 0..SPAWN_INBOX_CAPACITY {
            let Ok(()) = scope.spawn(Pending) else {
                clear(worker);
                panic!("the inbox must accept up to its capacity");
            };
        }
        let overflow = scope.spawn(Pending);
        clear(worker);

        assert_eq!(overflow, Err(SpawnError::InboxFull));
    }

    #[test]
    fn spawn_without_a_frame_is_err() {
        let worker = worker_id(14);
        let parent = parent_ref(14);
        // No frame installed for worker 14.
        assert_eq!(
            affine_scope(parent, worker).spawn(Pending),
            Err(SpawnError::NoActiveFrame),
        );
    }

    #[test]
    fn scope_yields_then_settles_with_no_children() {
        let parent = parent_ref(9);
        let worker = worker_id(9);
        let mut slab = Slab::<TaskSlot>::new(1);
        let mut inbox = SpawnInbox::<SPAWN_INBOX_CAPACITY>::new();
        let mut reap = ReapQueue::<REAP_QUEUE_CAPACITY>::new();
        let frame = PollFrame {
            current: parent,
            inbox: NonNull::from(&mut inbox),
            tasks: NonNull::from(&mut slab),
            driver: None,
            wake_data: WakeData::EMPTY,
            timer_requests: None,
            submitted_ops: core::sync::atomic::AtomicU16::new(0),
            reap: NonNull::from(&mut reap),
            first_child: None,
        };
        install(worker, &frame);
        let waker = waker::waker_from_task_ref(parent);
        let mut cx = Context::from_waker(&waker);
        let mut future = scope(|scope| scope.parent);
        // First poll: the builder runs and decodes the parent, then the scope
        // yields once so the drain can link children before the settle-check.
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Pending);
        // Second poll: no children, so the scope settles and returns the value.
        let settled = Pin::new(&mut future).poll(&mut cx);
        clear(worker);
        assert_eq!(settled, Poll::Ready(parent));
    }

    #[test]
    #[should_panic(expected = "combinator")]
    fn scope_panics_on_a_foreign_waker() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut future = scope(|_scope| ());
        let _ = Pin::new(&mut future).poll(&mut cx);
    }
}
