//! [`Runtime`] -- the constructed runtime handle owning its single worker.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use core::marker::PhantomData;
use std::io;

use kwokka_io::wake;

use crate::runtime::{builder::RuntimeBuilder, stealing::Crew};
use crate::{
    task::{Affine, Mode},
    worker::{registry, shard::WorkerShard},
};

/// A constructed runtime owning its lead worker's state.
///
/// The phantom `Mode` selects the scheduler discipline at the type level.
/// `Runtime<Affine>` carries `!Send` through the [`Affine`] marker, pinning
/// the runtime to the thread that built it. A stealing runtime additionally
/// owns its sibling worker threads through the crew.
pub struct Runtime<M: Mode> {
    pub(crate) shard: WorkerShard,
    /// The lead worker's wake eventfd; armed on its ring so a remote signal
    /// can complete a park. Closed on drop.
    pub(crate) wake_fd: i32,
    /// Sibling worker threads; a solo crew for the affine runtime.
    pub(crate) crew: Crew,
    _mode: PhantomData<M>,
}

impl<M: Mode> Runtime<M> {
    /// Wraps a constructed worker shard. Used by [`RuntimeBuilder`].
    pub(crate) const fn from_shard(shard: WorkerShard, wake_fd: i32) -> Self {
        Self {
            shard,
            wake_fd,
            crew: Crew::solo(),
            _mode: PhantomData,
        }
    }

    /// Wraps the lead shard together with its sibling crew. Used by the
    /// work-stealing build.
    pub(crate) const fn from_crew(shard: WorkerShard, wake_fd: i32, crew: Crew) -> Self {
        Self {
            shard,
            wake_fd,
            crew,
            _mode: PhantomData,
        }
    }
}

impl<M: Mode> Drop for Runtime<M> {
    /// Shuts the crew down and returns the worker ids to the allocator.
    ///
    /// Siblings join first -- each cleans its own slot on the way out --
    /// then the lead drains its residual wakes and the ids release last, so
    /// a later runtime claiming the same ids starts from clean slots.
    fn drop(&mut self) {
        let lead = self.shard.id;
        self.crew.join_siblings(lead);
        #[cfg(not(loom))]
        while registry::pop(lead).is_some() {}
        // A request or reply parked in the lead's steal rings would leak
        // into the next runtime claiming this id; popping a stranded
        // delivery also drops its body, releasing the carried future.
        #[cfg(all(feature = "steal", not(loom)))]
        {
            while registry::pop_handoff(lead).is_some() {}
            while registry::pop_steal_request(lead).is_some() {}
        }
        registry::withdraw_endpoint(lead);
        wake::close_wake_fd(self.wake_fd);
        self.crew.release_ids(lead);
    }
}

impl Runtime<Affine> {
    /// Builds a thread-per-core (affine) runtime on the current thread with
    /// default configuration.
    ///
    /// For custom configuration, use [`RuntimeBuilder`].
    ///
    /// # Errors
    ///
    /// Returns the backend setup error (e.g. an `io_uring` setup failure), or
    /// `InvalidInput` if the configured task capacity exceeds the wake-inbox
    /// capacity. See [`RuntimeBuilder::affine`].
    pub fn affine() -> io::Result<Self> {
        RuntimeBuilder::new().affine()
    }
}
