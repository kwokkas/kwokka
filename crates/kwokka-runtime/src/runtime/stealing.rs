//! The work-stealing runtime entry -- multi-worker bootstrap and blocking
//! run-loop.
//!
//! A crew of workers each owns its shard, ring, and wake fd, built inside
//! its own thread so nothing crosses a thread boundary. The lead worker
//! lives on the calling thread and drives [`Runtime::block_on`]; siblings
//! loop over the same scheduler passes and park on their drivers, bracketed
//! by the endpoint parked flag so a cross-worker wake or the shutdown
//! broadcast always completes the park. Under the `steal` feature an idle
//! sibling sweeps the crew before parking: it reserves a destination in
//! its own slab, posts a steal request to the next victim in round-robin
//! order, and the victim's serve step ships a sleeping task back through
//! the handoff ring. The lead serves and receives but never steals.
//!
//! Lifecycle: the builder claims a contiguous worker-id block, builds the
//! lead shard, spawns the siblings, and waits until every sibling has
//! published its endpoint before returning -- an endpoint publish must
//! precede the first park or signal. Siblings outlive `block_on` and park
//! idle between calls; dropping the runtime raises the shutdown flag,
//! signals every sibling awake, joins them, and releases the id block.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{io, thread};

#[cfg(feature = "steal")]
use kwokka_core::slab::SlabKey;
use kwokka_io::{DriverType, wake};

#[cfg(not(feature = "steal"))]
use crate::worker::park::wake::wake_local;
use crate::{
    runtime::{
        bootstrap,
        crew::{Crew, CrewKind, MAX_WORKERS, sibling_id},
        handle::Runtime,
    },
    task::Stealing,
    worker::{WorkerId, cycle::Tick, registry, shard::WorkerShard},
};
#[cfg(feature = "steal")]
use crate::{scheduler::stealing::handoff, worker::park::wake::wake_or_forward};

/// One stealing runtime per process: the crew shares the process-global
/// wake tables, the shutdown flag, and a contiguous id block. Claimed at
/// build, released when the runtime drops.
static STEALING_LIVE: AtomicBool = AtomicBool::new(false);

/// Crew-wide shutdown broadcast. Raised once by the lead before joining;
/// every sibling observes it after its next pass and exits its loop.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Count of siblings that finished building and published their endpoint.
/// The build barrier waits for every sibling before the runtime returns.
static READY: AtomicUsize = AtomicUsize::new(0);

/// Raised by a sibling whose shard construction failed; the build barrier
/// turns it into an error.
static BOOT_FAILED: AtomicBool = AtomicBool::new(false);

/// Resets the crew statics for the next stealing runtime in this process.
pub(crate) fn reset_statics() {
    READY.store(0, Ordering::SeqCst);
    BOOT_FAILED.store(false, Ordering::SeqCst);
    SHUTDOWN.store(false, Ordering::SeqCst);
    STEALING_LIVE.store(false, Ordering::Release);
}

/// Raises the stealing crew's shutdown broadcast. Every sibling observes it
/// after its next pass and exits its loop.
pub(crate) fn raise_shutdown() {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Builds the stealing runtime: claims the id block, builds the lead shard
/// on the calling thread, spawns the sibling crew, and waits for every
/// endpoint publication.
pub(crate) fn build(
    ring_entries: u32,
    task_capacity: usize,
    workers: usize,
) -> io::Result<Runtime<Stealing>> {
    if workers == 0 || workers > MAX_WORKERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker count must be between 1 and the crew cap",
        ));
    }
    if STEALING_LIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(io::Error::other(
            "a stealing runtime is already live in this process",
        ));
    }
    let Some(lead) = registry::claim_block(workers) else {
        STEALING_LIVE.store(false, Ordering::Release);
        return Err(io::Error::other("the worker id space is exhausted"));
    };
    match build_crew(lead, ring_entries, task_capacity, workers) {
        Ok(runtime) => Ok(runtime),
        Err(error) => {
            registry::release_block(lead, workers);
            reset_statics();
            Err(error)
        }
    }
}

/// Builds the lead shard and spawns the siblings; on any failure the
/// already-spawned part of the crew is shut down and joined.
fn build_crew(
    lead: WorkerId,
    ring_entries: u32,
    task_capacity: usize,
    workers: usize,
) -> io::Result<Runtime<Stealing>> {
    let driver = DriverType::for_platform(ring_entries)?;
    let wake_fd = wake::create_wake_fd()?;
    let shard = WorkerShard::new(lead, driver, task_capacity);
    registry::publish_endpoint(lead, wake_fd);
    match spawn_crew(lead, ring_entries, task_capacity, workers) {
        Ok(crew) => Ok(Runtime::from_crew(shard, wake_fd, crew)),
        Err(error) => {
            registry::withdraw_endpoint(lead);
            wake::close_wake_fd(wake_fd);
            Err(error)
        }
    }
}

/// Spawns the sibling threads and waits until every one has published its
/// endpoint -- the publish-before-run contract the endpoint cell requires.
fn spawn_crew(
    lead: WorkerId,
    ring_entries: u32,
    task_capacity: usize,
    workers: usize,
) -> io::Result<Crew> {
    let mut crew = Crew {
        handles: [const { None }; MAX_WORKERS],
        count: workers,
        kind: CrewKind::Stealing,
    };
    for offset in 1..workers {
        let sibling = sibling_id(lead, offset);
        let spawned = thread::Builder::new()
            .name(format!("kwokka-worker-{}", sibling.raw()))
            .spawn(move || {
                sibling_main(
                    sibling,
                    ring_entries,
                    task_capacity,
                    #[cfg(feature = "steal")]
                    lead,
                    #[cfg(feature = "steal")]
                    workers,
                );
            });
        match spawned {
            Ok(handle) => crew.handles[offset - 1] = Some(handle),
            Err(error) => {
                crew.count = offset;
                crew.join_siblings(lead);
                return Err(error);
            }
        }
    }
    while READY.load(Ordering::SeqCst) < workers - 1 {
        thread::yield_now();
    }
    if BOOT_FAILED.load(Ordering::SeqCst) {
        crew.join_siblings(lead);
        return Err(io::Error::other("a sibling worker failed to build"));
    }
    Ok(crew)
}

/// A sibling worker thread: builds its own shard, ring, and wake fd,
/// publishes its endpoint, loops until the shutdown broadcast, then cleans
/// its slot.
fn sibling_main(
    id: WorkerId,
    ring_entries: u32,
    task_capacity: usize,
    #[cfg(feature = "steal")] lead: WorkerId,
    #[cfg(feature = "steal")] workers: usize,
) {
    let Ok(driver) = DriverType::for_platform(ring_entries) else {
        BOOT_FAILED.store(true, Ordering::SeqCst);
        READY.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Ok(wake_fd) = wake::create_wake_fd() else {
        BOOT_FAILED.store(true, Ordering::SeqCst);
        READY.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut shard = WorkerShard::new(id, driver, task_capacity);
    registry::publish_endpoint(id, wake_fd);
    bootstrap::arm_wake(&shard, wake_fd);
    READY.fetch_add(1, Ordering::SeqCst);
    sibling_loop(
        &mut shard,
        wake_fd,
        #[cfg(feature = "steal")]
        lead,
        #[cfg(feature = "steal")]
        workers,
    );
    registry::withdraw_endpoint(id);
    #[cfg(feature = "steal")]
    {
        // A promise whose reply never resolved frees here; a body parked
        // in the handoff ring at shutdown drops on the pop, releasing its
        // future, so nothing leaks into the next runtime claiming this id.
        if let Some(promised) = shard.pending_steal.take() {
            shard.tasks.unreserve(promised);
        }
        while registry::pop_handoff(id).is_some() {}
        while registry::pop_steal_request(id).is_some() {}
    }
    while registry::pop(id).is_some() {}
    wake::close_wake_fd(wake_fd);
}

/// The sibling pass loop: scheduler passes until the shutdown broadcast,
/// parking through the endpoint bracket on idle passes -- after one steal
/// sweep, so an idle sibling pulls work toward itself before sleeping.
fn sibling_loop(
    shard: &mut WorkerShard,
    wake_fd: i32,
    #[cfg(feature = "steal")] lead: WorkerId,
    #[cfg(feature = "steal")] workers: usize,
) {
    loop {
        let outcome = bootstrap::run_pass(shard, wake_fd);
        if SHUTDOWN.load(Ordering::SeqCst) {
            return;
        }
        if outcome == Tick::Idle {
            #[cfg(feature = "steal")]
            try_steal(shard, lead, workers);
            park_bracketed(shard);
        }
    }
}

/// Posts one steal request to the next crew victim, reserving the
/// destination first.
///
/// A no-op while a steal is in flight (one per thief), when the crew has
/// no sibling, or when this slab has no free slot to promise. A request
/// bounced by a full victim ring withdraws the reservation; the sweep
/// retries on a later idle pass.
#[cfg(feature = "steal")]
fn try_steal(shard: &mut WorkerShard, lead: WorkerId, workers: usize) {
    if workers <= 1 || shard.pending_steal.is_some() {
        return;
    }
    let Some(victim) = next_victim(shard, lead, workers) else {
        return;
    };
    let Some(request) = handoff::prepare_steal(&mut shard.tasks, shard.id.raw()) else {
        return;
    };
    let promised = SlabKey::new(request.dest.index(), request.dest.generation());
    if registry::push_steal_request(victim.raw(), request).is_err() {
        shard.tasks.unreserve(promised);
        return;
    }
    shard.pending_steal = Some(promised);
    registry::signal(victim.raw());
}

/// The next crew victim in round-robin order, skipping this worker.
#[cfg(feature = "steal")]
fn next_victim(shard: &mut WorkerShard, lead: WorkerId, workers: usize) -> Option<WorkerId> {
    let Ok(count) = u8::try_from(workers) else {
        return None;
    };
    let offset_of_self = shard.id.raw() - lead.raw();
    let mut next = (shard.steal_cursor + 1) % count;
    if next == offset_of_self {
        next = (next + 1) % count;
    }
    shard.steal_cursor = next;
    if next == offset_of_self {
        return None;
    }
    WorkerId::new(lead.raw() + next).ok()
}

/// Parks the worker through the endpoint's parked bracket.
///
/// Raises the parked flag, then re-checks the shutdown broadcast and the
/// wake inbox -- the consumer half of the handshake the endpoint model
/// pins. A wake or shutdown that landed before the flag was visible is
/// caught by the re-checks; one landing after raises the eventfd and
/// completes the park.
fn park_bracketed(shard: &mut WorkerShard) {
    registry::set_parked(shard.id, true);
    if SHUTDOWN.load(Ordering::SeqCst) {
        registry::set_parked(shard.id, false);
        return;
    }
    if let Some(task_ref) = registry::pop(shard.id) {
        registry::set_parked(shard.id, false);
        #[cfg(feature = "steal")]
        wake_or_forward(
            &mut shard.tasks,
            &mut shard.run_queue,
            &shard.forward,
            task_ref,
        );
        #[cfg(not(feature = "steal"))]
        wake_local(&mut shard.tasks, &mut shard.run_queue, task_ref);
        return;
    }
    // A steal request or handoff reply that landed before the parked flag
    // was visible got a swallowed signal; the bracket re-checks the rings
    // themselves, or a thief and its victim could sleep on each other.
    #[cfg(feature = "steal")]
    if registry::has_steal_request(shard.id) || registry::has_handoff(shard.id) {
        registry::set_parked(shard.id, false);
        return;
    }
    bootstrap::park_for_next_event(shard);
    registry::set_parked(shard.id, false);
}

impl Runtime<Stealing> {
    /// Builds a work-stealing runtime with default configuration, sized to
    /// the host's available parallelism.
    ///
    /// For custom configuration, use
    /// [`RuntimeBuilder`](crate::runtime::builder::RuntimeBuilder).
    ///
    /// # Errors
    ///
    /// Returns the backend setup error from any worker's driver factory,
    /// `InvalidInput` for an out-of-range configuration, or an error when
    /// another stealing runtime is already live in this process or the
    /// worker id space is exhausted.
    pub fn stealing() -> io::Result<Self> {
        let workers = thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_WORKERS);
        crate::runtime::builder::RuntimeBuilder::new()
            .workers(workers)
            .stealing()
    }

    /// Runs `future` to completion on the lead worker, blocking the calling
    /// thread, and returns its output.
    ///
    /// The root task is pinned to the lead worker: it is spawned into the
    /// lead shard, driven by the lead's run-loop, and its output is read
    /// back on this thread. Sibling workers keep parking between calls, so
    /// the runtime can run another future after this one returns; the crew
    /// shuts down when the runtime drops.
    ///
    /// The `Send` bound is the work-stealing admission contract. The root
    /// itself never migrates, but every future entering this runtime
    /// satisfies the bound the steal path relies on.
    ///
    /// # Panics
    ///
    /// Panics if the root task cannot be spawned into the lead shard, or if
    /// it terminates abnormally (cancelled or failed). A recoverable error
    /// is the future's own `Output` and does not panic.
    pub fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
    {
        let root_key = bootstrap::spawn_root(&mut self.shard, future);
        bootstrap::arm_wake(&self.shard, self.wake_fd);
        loop {
            let outcome = bootstrap::run_pass(&mut self.shard, self.wake_fd);
            if bootstrap::root_settled(&self.shard, root_key) {
                break;
            }
            if outcome == Tick::Idle {
                park_bracketed(&mut self.shard);
            }
        }
        bootstrap::take_root_output::<F::Output>(&mut self.shard, root_key)
    }
}

#[cfg(test)]
#[cfg(feature = "steal")]
#[cfg(target_os = "linux")]
// The e2e test needs a real io_uring ring: Miri cannot run the syscalls, and
// the loom build drives loom atomics outside a model.
#[cfg(not(any(miri, loom)))]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };

    use kwokka_io::{
        boundary::{self, IoSeam},
        operation::{IoRequest, SubmitResult},
    };

    use crate::{
        runtime::builder::RuntimeBuilder,
        task::{io::TimerFuture, scope_send},
    };

    /// Whether the io child completed on the thread of its first poll.
    static IO_STAYED: AtomicBool = AtomicBool::new(false);
    /// Whether the seam-routed io child completed on its first-poll thread.
    static SEAM_IO_STAYED: AtomicBool = AtomicBool::new(false);
    /// Whether the sleeper child completed on a different thread.
    static SLEEPER_MIGRATED: AtomicBool = AtomicBool::new(false);

    /// Submits one timeout through the cross-crate seam and resolves with the
    /// drained result -- the seam-routed twin of [`TimerFuture`], standing in
    /// for an I/O future hosted outside this crate.
    struct SeamTimer {
        /// Timeout in nanoseconds, submitted on the first poll.
        duration_ns: u64,
        /// Whether the op has been submitted; gates the submit-once transition.
        is_submitted: bool,
    }

    impl Future for SeamTimer {
        type Output = i32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
            let Some(binding) = boundary::decode_waker(cx.waker()) else {
                panic!("SeamTimer requires the runtime task waker; await it directly");
            };
            if self.is_submitted {
                return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                    Some(Some(slot)) => Poll::Ready(slot.result),
                    _ => Poll::Pending,
                };
            }
            let request = IoRequest::<()>::timeout(self.duration_ns).with_user_data(binding.token);
            match IoSeam::with_current(binding.worker_id, |current| {
                current.submit_internal(request)
            }) {
                Some(Some(SubmitResult::Submitted(_))) => {
                    self.is_submitted = true;
                    Poll::Pending
                }
                // No seam, no driver, or a rejected op: resolve with -EINVAL
                // rather than hang, mirroring TimerFuture's fallback.
                _ => Poll::Ready(-22),
            }
        }
    }

    /// Sleeps with no wake registration on its first poll, completes on its
    /// second: the install-wake on a thief is the only path to that second
    /// poll, so completing at all proves a steal ran during the window.
    struct SecondPollElsewhere {
        first_poll: Option<std::thread::ThreadId>,
    }

    impl Future for SecondPollElsewhere {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let current = std::thread::current().id();
            let Some(first) = self.first_poll else {
                self.first_poll = Some(current);
                return Poll::Pending;
            };
            SLEEPER_MIGRATED.store(first != current, Ordering::Relaxed);
            Poll::Ready(())
        }
    }

    #[test]
    fn an_in_flight_op_completes_on_its_issuing_worker() {
        let Ok(mut runtime) = RuntimeBuilder::new().workers(2).stealing() else {
            panic!("a two-worker stealing runtime must build on this host");
        };
        runtime.block_on(scope_send(|sender| {
            // The sleeper is the steal bait: it migrates and completes on
            // the sibling, proving the crew was actively stealing during
            // the io child's in-flight window -- without it the io
            // assertion could pass with no steal attempted at all.
            let Ok(()) = sender.spawn(SecondPollElsewhere { first_poll: None }) else {
                panic!("the sleeper child must spawn");
            };
            let Ok(()) = sender.spawn(async {
                let issuing = std::thread::current().id();
                // The submit raises the in-flight counter before the child
                // suspends, so there is no sleeping-with-zero window; the
                // serve sweep must decline this child until the CQE lands.
                let result = TimerFuture::new(50_000_000).await;
                assert!(
                    result < 0,
                    "a completed timeout returns a negative -errno, got {result}",
                );
                IO_STAYED.store(issuing == std::thread::current().id(), Ordering::Relaxed);
            }) else {
                panic!("the io child must spawn");
            };
            let Ok(()) = sender.spawn(async {
                let issuing = std::thread::current().id();
                // Same in-flight window as the frame-routed child above, but
                // the submit travels the cross-crate seam: the steal predicate
                // must decline this child too until its CQE drains.
                let result = SeamTimer {
                    duration_ns: 50_000_000,
                    is_submitted: false,
                }
                .await;
                assert!(
                    result < 0,
                    "a completed seam timeout returns a negative -errno, got {result}",
                );
                SEAM_IO_STAYED.store(issuing == std::thread::current().id(), Ordering::Relaxed);
            }) else {
                panic!("the seam io child must spawn");
            };
        }));
        assert!(
            SLEEPER_MIGRATED.load(Ordering::Relaxed),
            "the sleeper child must migrate, proving the sibling was stealing",
        );
        assert!(
            IO_STAYED.load(Ordering::Relaxed),
            "the in-flight child must complete on its issuing worker",
        );
        assert!(
            SEAM_IO_STAYED.load(Ordering::Relaxed),
            "the seam-routed in-flight child must complete on its issuing worker",
        );
    }
}
