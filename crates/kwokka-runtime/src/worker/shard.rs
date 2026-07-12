//! [`WorkerShard`] -- per-worker owned state combining I/O, tasks, and timers.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io;

#[cfg(feature = "steal")]
use kwokka_core::slab::SlabKey;
use kwokka_core::{id::Pip, slab::Slab};
use kwokka_io::{
    DriverType,
    boundary::{
        ACCEPT_CANCEL_CAPACITY, AcceptCancelSet, CANCEL_INBOX_CAPACITY, CONNECT_CANCEL_CAPACITY,
        CancelInbox, ConnectCancelSet, PROVIDED_RECV_CANCEL_CAPACITY, ProvidedRecvCancelSet,
        RECV_CANCEL_INBOX_CAPACITY, RecvCancelInbox,
    },
    buffer::{
        multishot::{
            DEFAULT_MULTISHOT_CAP, DEFAULT_RECV_MULTISHOT_CAP, MultishotSlab, RecvMultishotSlab,
        },
        oneshot::inflight::{DEFAULT_INFLIGHT_CAP, InflightBufSlab},
    },
};

#[cfg(feature = "steal")]
use crate::scheduler::stealing::{handoff::ForwardOrigin, relocate::ForwardTable};
use crate::{
    scheduler::queue::LocalRunQueue,
    task::cell::slot::TaskSlot,
    timer::{
        clock::SystemClock,
        request::{TIMER_INBOX_CAPACITY, TimerInbox},
        wheel::TimerWheel,
    },
    worker::{
        WorkerId,
        queue::{
            inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
            reap::{REAP_QUEUE_CAPACITY, ReapQueue},
        },
    },
};

/// Per-worker shard owning I/O backend, task slab, timer wheel, and run queue.
///
/// Fields are `pub(crate)` so the worker loop can destructure the shard
/// into disjoint borrows, avoiding the double-`&mut self` conflict that
/// accessor methods cause.
///
/// Each worker thread holds exactly one shard. The shard is the
/// single-writer owner of its [`DriverType`] ring, task [`Slab`],
/// [`TimerWheel`], and [`LocalRunQueue`]. Work-stealing moves tasks
/// between workers, not shards.
pub(crate) struct WorkerShard {
    /// Worker identity for task routing and diagnostics.
    pub(crate) id: WorkerId,
    /// Next sequence number stamped into an issued `Pip`. Starts at 1 so the
    /// first id is never the reserved `Pip`(0). Single-writer, no atomic.
    /// Advanced by [`Self::issue_pip`] and by the run-loop spawn drain, which
    /// issues child ids from the same per-worker counter.
    pub(crate) pip_seq: u64,
    /// I/O backend for SQE submission and CQE polling.
    pub(crate) driver: DriverType,
    /// Per-worker in-flight buffer registry, owning buffered-op bytes across
    /// the op lifetime. Declared after `driver` so on drop its mmap unmaps only
    /// after the ring fd closes: closing the ring fd cancels in-flight ops and
    /// releases their registered-buffer references (`io_uring_enter.2`,
    /// `io_uring_register.2`), so no op writes into an unmapped page.
    pub(crate) inflight_slab: InflightBufSlab,
    /// Per-worker cancel queue for dropped buffered futures. Drop-order
    /// independent (no heap, no fd); grouped with `inflight_slab` for cohesion.
    pub(crate) cancel_inbox: CancelInbox<CANCEL_INBOX_CAPACITY>,
    /// Tokens of dropped single-shot accepts awaiting their completion, so the
    /// drain closes a raced accept's fd instead of orphaning it. No heap, no fd.
    pub(crate) accept_cancels: AcceptCancelSet<ACCEPT_CANCEL_CAPACITY>,
    /// Tokens of dropped provided-buffer recvs awaiting their completion, so
    /// the drain recycles a raced recv's kernel-selected buffer instead of
    /// leaking it. No heap, no fd.
    pub(crate) provided_recv_cancels: ProvidedRecvCancelSet<PROVIDED_RECV_CANCEL_CAPACITY>,
    /// Tokens of dropped single-shot connects awaiting their completion, so the
    /// drain diverts a raced connect's belated CQE off the task path instead of
    /// letting a stale result overwrite a live task's wake slot. No heap, no fd.
    pub(crate) connect_cancels: ConnectCancelSet<CONNECT_CANCEL_CAPACITY>,
    /// Per-worker multishot registry, holding the FIFO of completions for each
    /// in-flight multishot op. Drop-order independent (no heap, no fd).
    pub(crate) multishot_slab: MultishotSlab,
    /// Per-worker multishot recv registry, holding the FIFO of `(count, buf_id)`
    /// completions for each in-flight recv stream. mmap-backed FIFO payload,
    /// declared after `driver` for the same teardown discipline as the buffered
    /// slab, though the kernel never writes into it directly: the FIFO holds
    /// CQE-derived tuples, and the provided buffers themselves live in the
    /// driver's pool.
    pub(crate) recv_multishot_slab: RecvMultishotSlab,
    /// Per-worker cancel queue for dropped multishot recv streams. Dedicated and
    /// mmap-backed -- kept off the shared `cancel_inbox` ring, which sits at the
    /// shard's stack-frame budget. Drop-order independent (no fd, no kernel
    /// writes).
    pub(crate) recv_cancel_inbox: RecvCancelInbox<RECV_CANCEL_INBOX_CAPACITY>,
    /// Per-worker generational slab holding task headers and futures.
    pub(crate) tasks: Slab<TaskSlot>,
    /// Hierarchical timer wheel for deadline-based wakeups.
    pub(crate) timer: TimerWheel<SystemClock>,
    /// Local FIFO run queue of tasks ready to poll.
    pub(crate) run_queue: LocalRunQueue,
    /// Deferred child spawns requested mid-poll. A field disjoint from
    /// `tasks`, so a polled task reaches it through the poll frame without
    /// re-borrowing the slab the run-loop holds across the poll. Drained
    /// after each tick.
    pub(crate) spawn_inbox: SpawnInbox<SPAWN_INBOX_CAPACITY>,
    /// Parents whose scope settled this tick, recorded through the poll frame
    /// and drained after each tick by the reap path to free their settled
    /// children's slots. A field disjoint from `tasks`, like `spawn_inbox`.
    pub(crate) reap_queue: ReapQueue<REAP_QUEUE_CAPACITY>,
    /// Timer arms requested mid-poll by sleeping futures. A field disjoint from
    /// `tasks`, reached through the poll frame; drained after each tick, where
    /// each relative delay becomes an absolute deadline on the timer wheel.
    pub(crate) timer_requests: TimerInbox<TIMER_INBOX_CAPACITY>,
    /// Routes from this worker's retired husks to their tasks' new homes,
    /// recorded at ship time in the serve step. Sized to the slab, one
    /// entry per slot.
    #[cfg(feature = "steal")]
    pub(crate) forward: ForwardTable,
    /// Origins of residents relocated into this worker's slab, keyed by
    /// slot index; feeds the settled-note report. Sized to the slab,
    /// like [`Self::forward`].
    #[cfg(feature = "steal")]
    pub(crate) origins: ForwardOrigin,
    /// Round-robin cursor over crew victims for the idle steal sweep.
    #[cfg(feature = "steal")]
    pub(crate) steal_cursor: u8,
    /// The in-flight steal's promised destination. One steal in flight
    /// per thief; the shutdown path unreserves a promise whose reply
    /// never resolved.
    #[cfg(feature = "steal")]
    pub(crate) pending_steal: Option<SlabKey>,
}

impl WorkerShard {
    /// Create a shard for the given worker.
    pub(crate) fn new(id: WorkerId, driver: DriverType, task_capacity: usize) -> io::Result<Self> {
        // Every shard construction precedes its worker's first poll, so the
        // seam can translate task wakers from the very first submit.
        crate::task::waker::register_seam_decoder();
        let timer = TimerWheel::new(SystemClock::new(), task_capacity);
        let inflight_slab = InflightBufSlab::new(id.raw(), DEFAULT_INFLIGHT_CAP)?;
        Ok(Self {
            id,
            pip_seq: 1,
            driver,
            inflight_slab,
            cancel_inbox: CancelInbox::new(),
            accept_cancels: AcceptCancelSet::new(),
            provided_recv_cancels: ProvidedRecvCancelSet::new(),
            connect_cancels: ConnectCancelSet::new(),
            multishot_slab: MultishotSlab::new(id.raw(), DEFAULT_MULTISHOT_CAP),
            recv_multishot_slab: RecvMultishotSlab::new(id.raw(), DEFAULT_RECV_MULTISHOT_CAP)?,
            recv_cancel_inbox: RecvCancelInbox::new()?,
            tasks: Slab::new(task_capacity),
            timer,
            run_queue: LocalRunQueue::new(),
            spawn_inbox: SpawnInbox::new(),
            reap_queue: ReapQueue::new(),
            timer_requests: TimerInbox::new(),
            #[cfg(feature = "steal")]
            forward: ForwardTable::new(task_capacity),
            #[cfg(feature = "steal")]
            origins: ForwardOrigin::new(task_capacity),
            #[cfg(feature = "steal")]
            steal_cursor: 0,
            #[cfg(feature = "steal")]
            pending_steal: None,
        })
    }

    /// Mints the next [`Pip`] for a task spawned on this worker.
    ///
    /// Stamps the issuing worker id and a per-worker sequence number, then
    /// advances the counter. Single-writer, so no atomic. The id records the
    /// ISSUING worker; a stolen task keeps it after migrating to another.
    pub(crate) fn issue_pip(&mut self) -> Pip {
        let seq = self.pip_seq;
        self.pip_seq += 1;
        Pip::issue(u64::from(self.id.raw()), seq)
    }
}
