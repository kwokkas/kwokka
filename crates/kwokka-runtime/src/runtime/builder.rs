//! [`RuntimeBuilder`] -- runtime configuration and construction.

use std::io;

use kwokka_io::{DriverType, wake};

use crate::{
    runtime::handle::Runtime,
    task::{Affine, Stealing},
    worker::{registry, shard::state::WorkerShard},
};

/// Default `io_uring` submission/completion ring depth.
const DEFAULT_RING_ENTRIES: u32 = 256;

/// Default per-worker task capacity, bounded by the wake-inbox capacity so
/// wakes stay loss-free.
const DEFAULT_TASK_CAPACITY: usize = registry::INBOX_CAPACITY;

/// Configures and constructs a [`Runtime`].
///
/// Configuration methods take `self` by value and return it, so calls chain;
/// the terminal [`Self::affine`] consumes the builder and constructs the
/// runtime.
pub struct RuntimeBuilder {
    ring_entries: u32,
    task_capacity: usize,
    workers: usize,
}

impl RuntimeBuilder {
    /// Creates a builder with default configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ring_entries: DEFAULT_RING_ENTRIES,
            task_capacity: DEFAULT_TASK_CAPACITY,
            workers: 1,
        }
    }

    /// Sets the worker count for a multi-worker runtime.
    ///
    /// Both [`Self::stealing`] and [`Self::affine`] read a count above one --
    /// stealing builds a work-stealing crew, affine a thread-per-core crew. A
    /// count of one builds a single-worker runtime. The count is capped by the
    /// crew limit at build.
    #[must_use]
    pub const fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Sets the `io_uring` ring depth.
    #[must_use]
    pub const fn ring_entries(mut self, entries: u32) -> Self {
        self.ring_entries = entries;
        self
    }

    /// Sets the per-worker task capacity. Must not exceed the wake-inbox
    /// capacity; [`Self::affine`] rejects a larger value.
    #[must_use]
    pub const fn task_capacity(mut self, capacity: usize) -> Self {
        self.task_capacity = capacity;
        self
    }

    /// Builds an affine (thread-per-core) runtime.
    ///
    /// With the default single worker the lead runs on the calling thread and
    /// several such runtimes coexist in one process; with `workers > 1` it
    /// builds a multi-worker crew that is one-per-process. See
    /// [`Runtime::affine_crew`] for the parallelism-sized convenience entry.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` if the task capacity exceeds the wake-inbox
    /// capacity or the worker count exceeds the crew cap. Returns `Other` when
    /// the worker id space is exhausted or a multi-worker affine runtime is
    /// already live. Otherwise returns the backend setup error from the
    /// platform driver factory (e.g. an `io_uring` setup failure under seccomp
    /// or an unsupported kernel).
    pub fn affine(self) -> io::Result<Runtime<Affine>> {
        if self.task_capacity > registry::INBOX_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "task_capacity exceeds the wake-inbox capacity",
            ));
        }
        if self.workers > crate::runtime::crew::MAX_WORKERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker count must not exceed the crew cap",
            ));
        }
        if self.workers > 1 {
            return crate::runtime::affine::build(
                self.ring_entries,
                self.task_capacity,
                self.workers,
            );
        }
        let Some(worker_id) = registry::claim_one() else {
            return Err(io::Error::other("the worker id space is exhausted"));
        };
        let driver = match DriverType::for_platform(self.ring_entries) {
            Ok(driver) => driver,
            Err(error) => {
                registry::release(worker_id);
                return Err(error);
            }
        };
        let wake_fd = match wake::create_wake_fd() {
            Ok(wake_fd) => wake_fd,
            Err(error) => {
                registry::release(worker_id);
                return Err(error);
            }
        };
        let shard = match WorkerShard::new(worker_id, driver, self.task_capacity) {
            Ok(shard) => shard,
            Err(error) => {
                registry::release(worker_id);
                wake::close_wake_fd(wake_fd);
                return Err(error);
            }
        };
        Ok(Runtime::from_shard(shard, wake_fd))
    }

    /// Builds a work-stealing runtime: the lead worker on the calling
    /// thread plus a crew of sibling worker threads.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an out-of-range task capacity or worker
    /// count, an error when another stealing runtime is already live in
    /// this process or the worker id space is exhausted, and otherwise the
    /// backend setup error from any worker's driver factory.
    pub fn stealing(self) -> io::Result<Runtime<Stealing>> {
        if self.task_capacity > registry::INBOX_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "task_capacity exceeds the wake-inbox capacity",
            ));
        }
        crate::runtime::stealing::build(self.ring_entries, self.task_capacity, self.workers)
    }
}

/// Equivalent to [`RuntimeBuilder::new`]: every capacity at its default.
impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn affine_rejects_capacity_over_inbox() {
        let Err(error) = RuntimeBuilder::new()
            .task_capacity(registry::INBOX_CAPACITY + 1)
            .affine()
        else {
            panic!("a task capacity over the inbox bound must be rejected");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    // Builds a real I/O driver, which probes the kernel via syscalls miri
    // cannot execute. The miri job skips this test by name; cfg-gating does
    // not take because cargo-miri does not set cfg(miri) on the runner.
    #[test]
    fn affine_builds_on_this_host() {
        let Ok(_runtime) = RuntimeBuilder::new().affine() else {
            panic!("the affine runtime must build on a supported host");
        };
    }

    // Builds two real runtimes, so the miri job skips this test by name like
    // affine_builds_on_this_host. Two live runtimes in one process must hold
    // distinct worker ids, so their per-worker table slots never collide.
    #[test]
    fn affine_runtimes_claim_distinct_worker_ids() {
        let Ok(first) = RuntimeBuilder::new().affine() else {
            panic!("the first affine runtime must build on a supported host");
        };
        let Ok(second) = RuntimeBuilder::new().affine() else {
            panic!("the second affine runtime must build on a supported host");
        };
        assert_ne!(
            first.shard.id, second.shard.id,
            "two live runtimes never share a worker id",
        );
    }
}
