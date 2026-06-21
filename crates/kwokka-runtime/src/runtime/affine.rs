//! The multi-worker affine (thread-per-core) runtime entry -- crew bootstrap
//! and blocking run-loop.
//!
//! A multi-worker affine crew is one-per-process, enforced by [`AFFINE_LIVE`].
//! A single-worker affine runtime bypasses this module: the builder follows the
//! solo path, claims one id, and builds from a solo crew, so several solo affine
//! runtimes coexist. The crew mirrors the work-stealing bootstrap without the
//! steal sweep -- each sibling builds its own shard, ring, and wake fd,
//! publishes its endpoint, and loops over the shared scheduler passes, parking
//! on its driver between them. The lead drives `block_on` on the calling thread.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{io, thread};

use kwokka_io::{DriverType, wake};

use crate::runtime::stealing::{Crew, CrewKind, MAX_WORKERS, sibling_id};
use crate::runtime::{bootstrap, handle::Runtime};
use crate::worker::wake::wake_local;
use crate::{
    task::Affine,
    worker::{WorkerId, cycle::Tick, registry, shard::WorkerShard},
};

/// One multi-worker affine runtime per process: the crew shares the
/// process-global wake tables, the shutdown flag, and a contiguous id block.
/// Claimed at build, released when the runtime drops. A solo affine runtime
/// (one worker) never claims this and stays multi-instance.
static AFFINE_LIVE: AtomicBool = AtomicBool::new(false);

/// Crew-wide shutdown broadcast. Raised once by the lead before joining; every
/// sibling observes it after its next pass and exits its loop.
static AFFINE_SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Count of siblings that finished building and published their endpoint. The
/// build barrier waits for every sibling before the runtime returns.
static AFFINE_READY: AtomicUsize = AtomicUsize::new(0);

/// Raised by a sibling whose shard construction failed; the build barrier turns
/// it into an error.
static AFFINE_BOOT_FAILED: AtomicBool = AtomicBool::new(false);

/// Raises the affine crew's shutdown broadcast. Every sibling observes it after
/// its next pass and exits its loop.
pub(crate) fn raise_shutdown() {
    AFFINE_SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Resets the crew statics for the next multi-worker affine runtime in this
/// process.
pub(crate) fn reset_statics() {
    AFFINE_READY.store(0, Ordering::SeqCst);
    AFFINE_BOOT_FAILED.store(false, Ordering::SeqCst);
    AFFINE_SHUTDOWN.store(false, Ordering::SeqCst);
    AFFINE_LIVE.store(false, Ordering::Release);
}

/// Builds the multi-worker affine runtime: claims the id block, builds the lead
/// shard on the calling thread, spawns the sibling crew, and waits for every
/// endpoint publication.
pub(crate) fn build(
    ring_entries: u32,
    task_capacity: usize,
    workers: usize,
) -> io::Result<Runtime<Affine>> {
    if workers == 0 || workers > MAX_WORKERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker count must be between 1 and the crew cap",
        ));
    }
    if AFFINE_LIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(io::Error::other(
            "a multi-worker affine runtime is already live in this process",
        ));
    }
    let Some(lead) = registry::claim_block(workers) else {
        AFFINE_LIVE.store(false, Ordering::Release);
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
) -> io::Result<Runtime<Affine>> {
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
        kind: CrewKind::Affine,
    };
    for offset in 1..workers {
        let sibling = sibling_id(lead, offset);
        let spawned = thread::Builder::new()
            .name(format!("kwokka-affine-{}", sibling.raw()))
            .spawn(move || sibling_main(sibling, ring_entries, task_capacity));
        match spawned {
            Ok(handle) => crew.handles[offset - 1] = Some(handle),
            Err(error) => {
                crew.count = offset;
                crew.join_siblings(lead);
                return Err(error);
            }
        }
    }
    while AFFINE_READY.load(Ordering::SeqCst) < workers - 1 {
        thread::yield_now();
    }
    if AFFINE_BOOT_FAILED.load(Ordering::SeqCst) {
        crew.join_siblings(lead);
        return Err(io::Error::other("a sibling worker failed to build"));
    }
    Ok(crew)
}

/// A sibling worker thread: builds its own shard, ring, and wake fd, publishes
/// its endpoint, loops until the shutdown broadcast, then cleans its slot.
fn sibling_main(id: WorkerId, ring_entries: u32, task_capacity: usize) {
    let Ok(driver) = DriverType::for_platform(ring_entries) else {
        AFFINE_BOOT_FAILED.store(true, Ordering::SeqCst);
        AFFINE_READY.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Ok(wake_fd) = wake::create_wake_fd() else {
        AFFINE_BOOT_FAILED.store(true, Ordering::SeqCst);
        AFFINE_READY.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut shard = WorkerShard::new(id, driver, task_capacity);
    registry::publish_endpoint(id, wake_fd);
    bootstrap::arm_wake(&shard, wake_fd);
    AFFINE_READY.fetch_add(1, Ordering::SeqCst);
    sibling_loop(&mut shard, wake_fd);
    registry::withdraw_endpoint(id);
    while registry::pop(id).is_some() {}
    wake::close_wake_fd(wake_fd);
}

/// The sibling pass loop: scheduler passes until the shutdown broadcast,
/// parking through the endpoint bracket on idle passes.
fn sibling_loop(shard: &mut WorkerShard, wake_fd: i32) {
    loop {
        let outcome = bootstrap::run_pass(shard, wake_fd);
        if AFFINE_SHUTDOWN.load(Ordering::SeqCst) {
            return;
        }
        if outcome == Tick::Idle {
            park_bracketed(shard);
        }
    }
}

/// Parks the worker through the endpoint's parked bracket.
///
/// Raises the parked flag, then re-checks the shutdown broadcast and the wake
/// inbox -- a wake or shutdown that landed before the flag was visible is
/// caught by the re-checks; one landing after raises the eventfd and completes
/// the park.
fn park_bracketed(shard: &mut WorkerShard) {
    registry::set_parked(shard.id, true);
    if AFFINE_SHUTDOWN.load(Ordering::SeqCst) {
        registry::set_parked(shard.id, false);
        return;
    }
    if let Some(task_ref) = registry::pop(shard.id) {
        registry::set_parked(shard.id, false);
        wake_local(&mut shard.tasks, &mut shard.run_queue, task_ref);
        return;
    }
    bootstrap::park_for_next_event(shard);
    registry::set_parked(shard.id, false);
}

impl Runtime<Affine> {
    /// Builds a multi-worker affine (thread-per-core) runtime, sized to the
    /// host's available parallelism.
    ///
    /// Unlike [`Runtime::affine`], which always drives one worker on the
    /// calling thread, the crew runtime spawns one sibling worker per available
    /// core, each on its own thread, and is one-per-process. On a single-core
    /// host (where `available_parallelism` reports one) it falls back to a solo
    /// affine runtime with no one-per-process enforcement. For a custom worker
    /// count use [`RuntimeBuilder`](crate::runtime::builder::RuntimeBuilder).
    ///
    /// # Errors
    ///
    /// Returns the backend setup error from any worker's driver factory,
    /// `InvalidInput` for an out-of-range configuration, or an error when
    /// another multi-worker affine runtime is already live in this process or
    /// the worker id space is exhausted.
    pub fn affine_crew() -> io::Result<Self> {
        let workers = thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_WORKERS);
        crate::runtime::builder::RuntimeBuilder::new()
            .workers(workers)
            .affine()
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
// Builds real io_uring rings, which the miri interpreter cannot execute and the
// loom build drives outside a model; the miri job skips this test by name.
#[cfg(not(any(miri, loom)))]
mod tests {
    use crate::runtime::builder::RuntimeBuilder;

    // AFFINE_LIVE is process-global, so one test drives the whole crew lifecycle
    // (build, run, single-instance) to keep the sequence deterministic.
    #[test]
    fn affine_crew_builds_runs_and_is_single_instance() {
        {
            let Ok(mut runtime) = RuntimeBuilder::new().workers(2).affine() else {
                panic!("a two-worker affine crew must build on this host");
            };
            assert_eq!(runtime.block_on(async { 2 + 2 }), 4);
        }
        let Ok(_live) = RuntimeBuilder::new().workers(2).affine() else {
            panic!("a fresh affine crew must build after the prior one dropped");
        };
        let Err(error) = RuntimeBuilder::new().workers(2).affine() else {
            panic!("a second live affine crew must be rejected");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
    }
}
