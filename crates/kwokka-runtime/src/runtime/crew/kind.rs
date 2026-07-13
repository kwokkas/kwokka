//! The worker crew -- sibling threads shared by the affine and stealing runtimes.
//!
//! [`Crew`] owns the sibling [`JoinHandle`](std::thread::JoinHandle)s and the
//! [`CrewKind`] that selects which shutdown barrier the join and reset paths
//! raise. The affine and stealing runtimes both build a crew; the kind routes
//! [`join_siblings`](Crew::join_siblings) and [`release_ids`](Crew::release_ids)
//! to the matching discipline's statics.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::thread;

use crate::worker::{WorkerId, registry};

/// Most workers one runtime drives -- the id-block allocator cap.
pub(crate) const MAX_WORKERS: usize = 64;

/// The scheduler discipline a crew runs, selecting which shutdown barrier the
/// join and reset paths raise.
pub(crate) enum CrewKind {
    /// A single-worker crew -- no siblings, no barrier.
    Solo,
    /// A work-stealing crew, joined through the stealing shutdown barrier.
    Stealing,
    /// A multi-worker affine crew, joined through the affine shutdown barrier.
    Affine,
}

/// Sibling worker threads owned by the runtime handle.
///
/// The affine runtime carries a solo crew (no siblings) or a multi-worker
/// affine crew; the stealing runtime carries one handle per spawned sibling.
/// The handle's drop path joins the crew before the worker ids are released.
pub(crate) struct Crew {
    pub(crate) handles: [Option<thread::JoinHandle<()>>; MAX_WORKERS],
    pub(crate) count: usize,
    pub(crate) kind: CrewKind,
}

impl Crew {
    /// A single-worker crew -- no siblings to spawn, signal, or join.
    pub(crate) const fn solo() -> Self {
        Self {
            handles: [const { None }; MAX_WORKERS],
            count: 1,
            kind: CrewKind::Solo,
        }
    }

    /// Raises the shutdown flag, unparks every sibling, and joins them.
    ///
    /// A no-op for a solo crew. Idempotent -- joined handles are taken, so
    /// a second call finds nothing left to join.
    ///
    /// # Panics
    ///
    /// Panics if a sibling worker thread itself panicked.
    pub(crate) fn join_siblings(&mut self, lead: WorkerId) {
        if self.count <= 1 {
            return;
        }
        match self.kind {
            CrewKind::Stealing => crate::runtime::crew::stealing::raise_shutdown(),
            CrewKind::Affine => crate::runtime::crew::affine::raise_shutdown(),
            CrewKind::Solo => return,
        }
        for offset in 1..self.count {
            registry::signal(None, sibling_id(lead, offset).raw());
        }
        for slot in &mut self.handles[..self.count - 1] {
            let Some(handle) = slot.take() else {
                continue;
            };
            let Ok(()) = handle.join() else {
                panic!("a sibling worker thread panicked");
            };
        }
    }

    /// Releases the claimed worker ids and resets the crew statics.
    ///
    /// Runs after the sibling join and the lead's own drain, so a later
    /// runtime claiming the same ids starts from clean slots.
    pub(crate) fn release_ids(&self, lead: WorkerId) {
        if self.count <= 1 {
            registry::release(lead);
        } else {
            registry::release_block(lead, self.count);
        }
        // A single-worker stealing runtime still sets `STEALING_LIVE` at build,
        // so its drop must reset the statics even though it claimed one id. The
        // solo affine path sets no liveness flag, so it has nothing to reset.
        match self.kind {
            CrewKind::Affine => crate::runtime::crew::affine::reset_statics(),
            CrewKind::Stealing => crate::runtime::crew::stealing::reset_statics(),
            CrewKind::Solo => {}
        }
    }
}

/// The sibling id at `offset` within the crew's contiguous block.
///
/// # Panics
///
/// Panics if the offset leaves the claimed block's id range, which the
/// block allocator's contiguity contract rules out.
pub(crate) fn sibling_id(lead: WorkerId, offset: usize) -> WorkerId {
    let Ok(step) = u8::try_from(offset) else {
        panic!("a crew offset fits a u8");
    };
    let Ok(id) = WorkerId::new(lead.raw() + step) else {
        panic!("a claimed block stays inside the worker id space");
    };
    id
}
