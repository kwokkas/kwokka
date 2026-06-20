//! [`WorkerShard`] -- per-worker owned state combining I/O, tasks, and timers.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use kwokka_core::id::Pip;
use kwokka_io::DriverType;

#[cfg(feature = "steal")]
use crate::scheduler::stealing::{handoff::ForwardOrigin, relocate::ForwardTable};
use crate::worker::{
    WorkerId,
    inbox::{SPAWN_INBOX_CAPACITY, SpawnInbox},
    reap::{REAP_QUEUE_CAPACITY, ReapQueue},
};
use crate::{
    scheduler::queue::LocalRunQueue,
    task::slot::TaskSlot,
    timer::{clock::SystemClock, wheel::TimerWheel},
};
use kwokka_core::slab::Slab;
#[cfg(feature = "steal")]
use kwokka_core::slab::SlabKey;

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
    /// Advanced by [`Self::issue_nid`] and by the run-loop spawn drain, which
    /// issues child ids from the same per-worker counter.
    pub(crate) nid_seq: u64,
    /// I/O backend for SQE submission and CQE polling.
    pub(crate) driver: DriverType,
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
    pub(crate) fn new(id: WorkerId, driver: DriverType, task_capacity: usize) -> Self {
        // Every shard construction precedes its worker's first poll, so the
        // seam can translate task wakers from the very first submit.
        crate::task::waker::register_seam_decoder();
        let timer = TimerWheel::new(SystemClock::new(), task_capacity);
        Self {
            id,
            nid_seq: 1,
            driver,
            tasks: Slab::new(task_capacity),
            timer,
            run_queue: LocalRunQueue::new(),
            spawn_inbox: SpawnInbox::new(),
            reap_queue: ReapQueue::new(),
            #[cfg(feature = "steal")]
            forward: ForwardTable::new(task_capacity),
            #[cfg(feature = "steal")]
            origins: ForwardOrigin::new(task_capacity),
            #[cfg(feature = "steal")]
            steal_cursor: 0,
            #[cfg(feature = "steal")]
            pending_steal: None,
        }
    }

    /// Mints the next [`Pip`] for a task spawned on this worker.
    ///
    /// Stamps the issuing worker id and a per-worker sequence number, then
    /// advances the counter. Single-writer, so no atomic. The id records the
    /// ISSUING worker; a stolen task keeps it after migrating to another.
    pub(crate) fn issue_nid(&mut self) -> Pip {
        let seq = self.nid_seq;
        self.nid_seq += 1;
        Pip::issue(u64::from(self.id.raw()), seq)
    }
}
