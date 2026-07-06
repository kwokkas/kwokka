//! `io_uring` backend -- `UringDriver`.
//!
//! Owns an [`IoUring`] instance configured via graceful-degrade probe,
//! and implements the [`IoDriver`] trait with
//! `UnsafeCell` interior mutability for `&self` access.
//!
//! # Safety model
//!
//! `UringDriver` wraps `IoUring` in `UnsafeCell` because the
//! [`IoDriver`] trait requires `&self` but SQE push and CQE drain need
//! mutable access. This is sound under these invariants:
//!
//! 1. `IORING_SETUP_SINGLE_ISSUER` is set -- kernel enforces single-thread submission.
//! 2. `UringDriver` is owned by exactly one `WorkerShard`.
//! 3. `WorkerShard` is accessed only from its owning worker thread (the poll-frame contract in
//!    `worker/frame.rs` and `worker/polling.rs`).
//! 4. Reentrant access within the same thread is sequential.
//! 5. `Send` is implemented manually; `Sync` is intentionally omitted to prevent cross-thread
//!    references at compile time.
//!
//! [`IoUring`]: io_uring::IoUring
//! [`IoDriver`]: crate::IoDriver

#![allow(dead_code, reason = "pending setup-tier introspection wire-up")]
#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::{cell::UnsafeCell, io, os::fd::AsRawFd, time::Duration};

use io_uring::{
    EnterFlags, IoUring,
    squeue::Flags,
    types::{SubmitArgs, TimeoutFlags, Timespec},
};

use crate::{
    CancelError, IoDriver, RegisterError,
    boundary::LINK_TIMEOUT_DISCARD_USER_DATA,
    buffer::{
        registration::{RegisteredBuffers, RegisteredFds},
        ring::pool::BufRingPool,
        slot::{BufGroupId, FdSlot},
    },
    capability::CapabilityMatrix,
    operation::{
        Completion, InlineBuf, IoBuf, IoBufMut, IoRequest, OpCode, SubmitResult, SubmitToken,
    },
    uring::{
        completion::drain_completions,
        opcode::control::build_link_timeout,
        setup::{
            detect::{ProbeResult, probe_and_create},
            flags::SetupTier,
        },
        submission::{SubmitScratch, build_entry, build_entry_read, build_entry_write},
    },
};

/// Stack chunk size for `register_buffers` updates.
/// 256 x 16B (iovec) = 4 KB per syscall.
const REGISTER_CHUNK: usize = 256;

/// Provided-buffer ring entries per worker. Power of two; aligned with the
/// in-flight slab capacity so the two per-worker buffer regions match.
const PROVIDED_RECV_RING_ENTRIES: u16 = 256;
/// Per-buffer size in the provided-recv ring, matching the in-flight stride.
const PROVIDED_RECV_BUF_SIZE: u32 = 4096;
/// Buffer group id for the per-worker provided-recv ring. One ring per
/// worker makes a fixed group id unambiguous.
const PROVIDED_RECV_GROUP_ID: u16 = 0;

/// `io_uring` I/O backend.
///
/// Created via [`UringDriver::new`] which probes the kernel for the best
/// available setup tier. Implements [`IoDriver`] for integration with the
/// [`DriverType`](crate::DriverType) enum dispatch.
pub struct UringDriver {
    ring: UnsafeCell<IoUring>,
    capabilities: CapabilityMatrix,
    tier: SetupTier,
    scratch: UnsafeCell<SubmitScratch>,
    /// Landing pad for the wake-fd read; the kernel writes the drained
    /// counter value here. Covered by the same single-issuer ownership as
    /// `scratch`: only the owning worker thread arms the read.
    wake_buf: UnsafeCell<u64>,
    buffers: UnsafeCell<RegisteredBuffers>,
    files: UnsafeCell<RegisteredFds>,
    /// Per-worker provided-buffer ring for kernel-selected recv, present
    /// only when the kernel supports `buf_ring`. Declared after `ring` so
    /// on drop its mmap regions unmap only after the ring fd closes:
    /// closing the fd unregisters the buffer ring and aborts in-flight
    /// recvs referencing the pool storage (`io_uring_register.2`), so no
    /// op writes into an unmapped page. `get` / `recycle` take `&self`, so
    /// no `UnsafeCell` is needed.
    buf_ring_pool: Option<BufRingPool>,
}

// SAFETY: Invariant -- single-owner, single-thread.
// UringDriver ownership is transferred once at worker bootstrap.
// After that, only the owning worker thread accesses the ring.
// Cross-thread sharing is prevented at compile time by the absence
// of a Sync impl. SINGLE_ISSUER kernel flag provides defense-in-depth.
// Failure mode: concurrent access from another thread would race on
// the UnsafeCell contents; the kernel rejects with -EEXIST on the
// submit path and the ring fd close on drop remains sound.
unsafe impl Send for UringDriver {}

impl UringDriver {
    /// Bootstrap an `io_uring` ring with the best available setup tier.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if ring creation fails (kernel too old,
    /// `io_uring` disabled via sysctl, `RLIMIT_MEMLOCK` exhausted).
    pub fn new(entries: u32) -> io::Result<Self> {
        let ProbeResult {
            ring,
            capabilities,
            tier,
        } = probe_and_create(entries)?;

        #[allow(
            clippy::cast_possible_truncation,
            reason = "max_register_slots clamped by CapabilityMatrix"
        )]
        let max_buf_slots = capabilities.max_register_slots as u16;

        // A registration failure degrades to the inline-buffer recv fallback
        // (fallback parity), so it never fails ring bootstrap.
        let buf_ring_pool = if capabilities.buf_ring {
            register_provided_recv_pool(&ring).ok()
        } else {
            None
        };

        Ok(Self {
            ring: UnsafeCell::new(ring),
            capabilities,
            tier,
            scratch: UnsafeCell::new(SubmitScratch::new()),
            wake_buf: UnsafeCell::new(0),
            buffers: UnsafeCell::new(RegisteredBuffers::new(max_buf_slots)),
            files: UnsafeCell::new(RegisteredFds::new(capabilities.max_register_slots)),
            buf_ring_pool,
        })
    }

    /// Setup tier achieved during ring creation.
    pub(crate) const fn tier(&self) -> SetupTier {
        self.tier
    }

    /// The provided-buffer recv pool, present only when the kernel supports
    /// `buf_ring`. `None` selects the inline-buffer recv fallback.
    pub(crate) const fn provided_recv_pool(&self) -> Option<&BufRingPool> {
        self.buf_ring_pool.as_ref()
    }

    #[allow(
        clippy::mut_from_ref,
        reason = "UnsafeCell interior mutability is the intended pattern; SAFETY comment justifies single-thread access"
    )]
    fn ring_mut(&self) -> &mut IoUring {
        // SAFETY: Invariant -- single-thread sequential access.
        // SINGLE_ISSUER kernel flag (submit), WorkerShard TLS ownership
        // (no cross-thread reference), sequential reentrant access (task
        // poll -> submit is same thread).
        // Precondition: caller is the owning worker thread.
        // Failure mode: violated invariant races on ring state; kernel
        // rejects with -EEXIST, ring fd close on drop still sound.
        unsafe { &mut *self.ring.get() }
    }

    #[allow(
        clippy::mut_from_ref,
        reason = "UnsafeCell interior mutability; SAFETY comment justifies single-thread access"
    )]
    fn scratch_mut(&self) -> &mut SubmitScratch {
        // SAFETY: Invariant -- single-thread sequential access.
        // SINGLE_ISSUER kernel flag + WorkerShard TLS ownership
        // guarantee no concurrent access to the scratch buffer.
        // Precondition: caller is the owning worker thread.
        // Failure mode: violated invariant corrupts the SQE scratch
        // buffer (address/timespec), producing an invalid SQE.
        unsafe { &mut *self.scratch.get() }
    }

    #[allow(
        clippy::mut_from_ref,
        reason = "UnsafeCell interior mutability; SAFETY comment justifies single-thread access"
    )]
    fn buffers_mut(&self) -> &mut RegisteredBuffers {
        // SAFETY: Invariant -- single-thread sequential access.
        // Same invariants as ring_mut (SINGLE_ISSUER + WorkerShard TLS).
        // Precondition: caller is the owning worker thread.
        // Failure mode: violated invariant corrupts buffer slot tracking.
        unsafe { &mut *self.buffers.get() }
    }

    #[allow(
        clippy::mut_from_ref,
        reason = "UnsafeCell interior mutability; SAFETY comment justifies single-thread access"
    )]
    fn files_mut(&self) -> &mut RegisteredFds {
        // SAFETY: Invariant -- single-thread sequential access.
        // Same invariants as ring_mut (SINGLE_ISSUER + WorkerShard TLS).
        // Precondition: caller is the owning worker thread.
        // Failure mode: violated invariant corrupts fd slot tracking.
        unsafe { &mut *self.files.get() }
    }

    fn push_and_submit(&self, entry: &io_uring::squeue::Entry, user_data: u64) -> SubmitResult {
        let ring = self.ring_mut();

        // SAFETY: Invariant -- SQE entry validity + single-thread access.
        // The SQE entry was built from a valid IoRequest via
        // build_entry/build_entry_write/build_entry_read. All pointer
        // fields (buffer, timespec, sockaddr) are owned by the IoRequest
        // or SubmitScratch and remain valid until the CQE arrives. A
        // sendmsg/recvmsg msghdr pointer instead references the caller's
        // future-pinned in-flight slot; the io_bound worker pin holds
        // that slot on this worker until the op's own CQE, so it stays
        // valid for the kernel's whole read/write of the message.
        // Precondition: submission queue accessed exclusively by this
        // worker thread (SINGLE_ISSUER).
        // Failure mode: invalid pointer in SQE causes kernel to read/write
        // freed memory (undefined behavior); queue contention produces
        // data race on SQ tail.
        let push_result = unsafe { ring.submission().push(entry) };

        if push_result.is_err() {
            return SubmitResult::QueueFull;
        }

        // IGNORE: non-blocking submit; error means ring fd invalid (unrecoverable)
        let _ = ring.submit();

        SubmitResult::Submitted(SubmitToken::new(user_data))
    }

    /// Pushes a two-SQE linked pair atomically and submits, returning a token
    /// for `primary_user_data`.
    ///
    /// The first entry carries `IOSQE_IO_LINK`; the second is its linked
    /// timeout (`io_uring_linked_requests.7`). Both post their own CQE.
    fn push_pair_and_submit(
        &self,
        entries: &[io_uring::squeue::Entry; 2],
        primary_user_data: u64,
    ) -> SubmitResult {
        let ring = self.ring_mut();

        // SAFETY: Invariant -- SQE validity + single-thread access + atomic pair
        // push. Both entries were built from valid requests via build_entry and
        // build_link_timeout; their pointer fields (scratch addr, timespec,
        // link_timeout) are owned by this driver's SubmitScratch and stay valid
        // until the synchronous submit below copies them. push_multiple checks SQ
        // capacity for the whole slice before writing either slot, so a full
        // queue rejects the pair wholesale -- no dangling IOSQE_IO_LINK primary
        // can link onto an unrelated later SQE.
        // Precondition: submission queue accessed exclusively by this worker
        // thread (SINGLE_ISSUER), the same ownership push_and_submit relies on.
        // Failure mode: an invalid pointer makes the kernel read freed memory
        // (undefined behavior); a foreign-thread push races the SQ tail.
        let push_result = unsafe { ring.submission().push_multiple(entries) };

        if push_result.is_err() {
            return SubmitResult::QueueFull;
        }

        // IGNORE: non-blocking submit; error means ring fd invalid (unrecoverable)
        let _ = ring.submit();

        SubmitResult::Submitted(SubmitToken::new(primary_user_data))
    }

    /// Flushes deferred completion task work on a `DEFER_TASKRUN` ring.
    ///
    /// Under `IORING_SETUP_DEFER_TASKRUN` the kernel posts CQEs only when
    /// the owning thread enters with `IORING_ENTER_GETEVENTS`; the park
    /// path supplies that enter, so a worker that never parks would starve
    /// its completions without this flush. Submits nothing and waits for
    /// nothing: a `min_complete` of zero returns as soon as the deferred
    /// work has run. A no-op returning zero on rings without the flag.
    ///
    /// # Errors
    ///
    /// Returns the `io_uring_enter` error; an interrupted flush surfaces
    /// as [`io::ErrorKind::Interrupted`] and the next pass retries.
    pub(crate) fn flush_deferred(&self) -> io::Result<usize> {
        if !self.capabilities.defer_taskrun {
            return Ok(0);
        }
        // SAFETY:
        // Invariant: `to_submit` is zero, so the kernel reads no SQE and
        // no pointer or buffer lifetime is involved; `min_complete` of
        // zero with GETEVENTS runs deferred task work and returns without
        // blocking (io_uring_enter(2)). The ring fd outlives the call --
        // the driver owns it.
        // Precondition: called from the ring-owning worker thread, the
        // same single-issuer ownership every ring access here relies on.
        // Failure mode: an enter from a foreign thread on a single-issuer
        // ring fails with -EEXIST rather than corrupting ring state.
        unsafe {
            self.ring_mut().submitter().enter::<libc::sigset_t>(
                0,
                0,
                EnterFlags::GETEVENTS.bits(),
                None,
            )
        }
    }

    /// Blocks until at least one completion is ready or `deadline` elapses,
    /// returning the SQE count submitted by the `io_uring_enter` call.
    ///
    /// `None` waits indefinitely for a completion. `Some(deadline)` caps the
    /// wait with the `EXT_ARG` timeout (`IORING_FEAT_EXT_ARG`, kernel 5.11+,
    /// unconditional at the 6.0 minimum). No new `unsafe`: the wait runs on
    /// the `&mut IoUring` from [`Self::ring_mut`].
    ///
    /// # Errors
    ///
    /// Returns the `io_uring_enter` error. A `Some` timeout that elapses with
    /// no completion surfaces as the kernel `-ETIME` (not Rust's `TimedOut`
    /// kind), and an interrupted wait as [`io::ErrorKind::Interrupted`]; the
    /// run-loop maps both to a re-tick.
    pub(crate) fn park(&self, deadline: Option<Duration>) -> io::Result<usize> {
        let submitter = self.ring_mut().submitter();
        let Some(duration) = deadline else {
            return submitter.submit_and_wait(1);
        };
        let timespec = Timespec::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos());
        let args = SubmitArgs::new().timespec(&timespec);
        submitter.submit_with_args(1, &args)
    }

    /// Arms a oneshot read on the wake fd so a remote signal completes the
    /// park as a CQE carrying `user_data`.
    ///
    /// Re-armed by the completion drain after every wake CQE. The eventfd
    /// counter accumulates signals between arms, so the re-arm window
    /// cannot lose a wake.
    pub(crate) fn arm_wake_read(&self, fd: i32, user_data: u64) -> SubmitResult {
        // SAFETY: Invariant -- `wake_buf` is a live 8-byte field of this
        // driver, so the pointer is non-null and valid for 8 writes; the
        // driver outlives every CQE its ring delivers. Precondition: only
        // the owning worker thread arms the read (the same single-issuer
        // ownership `scratch` relies on), AND each arm follows a completed
        // CQE for the previous read -- the initial arm at run start, then
        // re-arms only inside the completion drain after the wake CQE is
        // observed -- so at most one kernel read targets the buffer at any
        // time. Failure mode: a second concurrent arm, from another thread
        // or a premature re-arm before the prior CQE, would race the
        // kernel write into the buffer -- undefined behavior.
        let buf = unsafe { InlineBuf::new(self.wake_buf.get().cast(), 8) };
        let request = IoRequest::read(fd, buf, 0).with_user_data(user_data);
        self.submit_read(request)
    }

    /// The raw fd of this driver's own ring -- the target a peer names in an
    /// `IORING_OP_MSG_RING` wake. Stable for the driver's lifetime; the ring is
    /// never recreated or resized after construction.
    ///
    /// No new `unsafe`: reuses [`Self::ring_mut`]'s single-issuer access to read
    /// the stored ring fd. `as_raw_fd` performs no syscall and mutates no ring
    /// state, and `UringDriver` is `!Sync`, so `&self` is never held across
    /// threads -- the cross-worker wake publishes only the plain fd value, never
    /// a driver reference.
    pub(crate) fn ring_fd(&self) -> i32 {
        self.ring_mut().as_raw_fd()
    }

    /// Submits an `IORING_OP_MSG_RING` wake on this ring targeting
    /// `target_ring_fd`.
    ///
    /// Carries [`MSG_RING_WAKE_USER_DATA`](crate::boundary::MSG_RING_WAKE_USER_DATA)
    /// so the target's completion drain recognizes the CQE and unparks. Returns
    /// [`SubmitResult::Unsupported`] when the kernel lacks `msg_ring`, leaving
    /// the caller to fall back to the eventfd wake (fallback parity).
    pub(crate) fn submit_msg_ring_wake(&self, target_ring_fd: i32) -> SubmitResult {
        if !self.capabilities.msg_ring {
            return SubmitResult::Unsupported;
        }
        self.submit_internal(IoRequest::<()>::msg_ring_wake(target_ring_fd))
    }

    /// Submits `request` bounded by a native `IORING_OP_LINK_TIMEOUT` deadline.
    ///
    /// The op carries `IOSQE_IO_LINK`; a linked timeout SQE follows in the same
    /// atomic submission (`io_uring_linked_requests.7`). If `deadline_ns` elapses
    /// first the kernel cancels the op (`-ECANCELED` on its CQE, or `-EINTR` if
    /// it was already mid-flight); if the op completes first the timeout is
    /// cancelled. The timeout's own CQE carries
    /// [`LINK_TIMEOUT_DISCARD_USER_DATA`] so the drain drops it -- the caller
    /// observes only the op's CQE. Returns [`SubmitResult::Unsupported`] when the
    /// kernel lacks `link_timeout`, leaving the caller to fall back to the
    /// timer-wheel deadline (fallback parity).
    pub(crate) fn submit_linked_timeout_internal(
        &self,
        request: &IoRequest<()>,
        deadline_ns: u64,
    ) -> SubmitResult {
        if !self.capabilities.link_timeout {
            return SubmitResult::Unsupported;
        }
        let primary_user_data = request.common.user_data;
        // One &mut over the scratch: build_entry reborrows it for the primary's
        // addr/timespec, then link_timeout reborrows the disjoint field. A second
        // scratch_mut() call would invalidate the primary's stored pointer.
        let scratch = self.scratch_mut();
        let primary = build_entry(request, scratch).flags(Flags::IO_LINK);
        let timeout = build_link_timeout(
            deadline_ns,
            &mut scratch.link_timeout,
            TimeoutFlags::empty(),
        )
        .user_data(LINK_TIMEOUT_DISCARD_USER_DATA);
        self.push_pair_and_submit(&[primary, timeout], primary_user_data)
    }
}

/// Register a per-worker provided-buffer ring for kernel-selected recv.
///
/// Builds a [`BufRingPool`] -- which owns the ring metadata and buffer
/// storage -- and registers it with the kernel. Called once at worker
/// startup when [`CapabilityMatrix::buf_ring`] holds. On failure the pool
/// drops here, unmapping the never-registered region.
///
/// # Errors
///
/// Returns [`RegisterError`] if the mmap allocation or the kernel
/// registration fails.
fn register_provided_recv_pool(ring: &IoUring) -> Result<BufRingPool, RegisterError> {
    let pool = BufRingPool::new(
        PROVIDED_RECV_RING_ENTRIES,
        PROVIDED_RECV_BUF_SIZE,
        BufGroupId(PROVIDED_RECV_GROUP_ID),
    )
    .map_err(|_| RegisterError::SlotExhausted)?;
    crate::uring::fixed::register_buf_ring(
        ring,
        pool.ring_addr(),
        pool.entries(),
        PROVIDED_RECV_GROUP_ID,
    )?;
    Ok(pool)
}

impl IoDriver for UringDriver {
    fn submit<B: IoBuf>(&self, request: IoRequest<B>) -> SubmitResult {
        let entry = match request.opcode {
            OpCode::Read | OpCode::Recv => {
                return SubmitResult::Unsupported;
            }
            OpCode::Write | OpCode::Send => build_entry_write(&request),
            _ => return SubmitResult::Unsupported,
        };

        let ud = request.common.user_data;
        self.push_and_submit(&entry, ud)
    }

    fn submit_read<B: IoBufMut>(&self, request: IoRequest<B>) -> SubmitResult {
        let entry = build_entry_read(&request);
        let ud = request.common.user_data;
        self.push_and_submit(&entry, ud)
    }

    fn submit_internal(&self, request: IoRequest<()>) -> SubmitResult {
        let ud = request.common.user_data;
        let entry = build_entry(&request, self.scratch_mut());
        self.push_and_submit(&entry, ud)
    }

    fn poll_completions(&self, max: usize, out: &mut [Completion]) -> usize {
        let ring = self.ring_mut();
        let mut cq = ring.completion();
        drain_completions(&mut cq, max, out)
    }

    fn capabilities(&self) -> &CapabilityMatrix {
        &self.capabilities
    }

    fn cancel(&self, token: SubmitToken) -> Result<(), CancelError> {
        let request = IoRequest::<()>::cancel(token);
        let ud = request.common.user_data;
        let entry = build_entry(&request, self.scratch_mut());
        match self.push_and_submit(&entry, ud) {
            SubmitResult::Submitted(_) => Ok(()),
            SubmitResult::QueueFull | SubmitResult::Unsupported => {
                Err(CancelError::BestEffortDetach)
            }
        }
    }

    fn register_buffers(&self, bufs: &[&[u8]]) -> Result<BufGroupId, RegisterError> {
        if bufs.is_empty() {
            return Err(RegisterError::InvalidArgument);
        }

        #[allow(
            clippy::cast_possible_truncation,
            reason = "kernel rejects registrations beyond max_register_slots; callers respect CapabilityMatrix"
        )]
        let total = bufs.len() as u32;

        let submitter = self.ring_mut().submitter();

        submitter
            .register_buffers_sparse(total)
            .map_err(|_| RegisterError::InvalidArgument)?;

        let mut offset: u32 = 0;
        for chunk_bufs in bufs.chunks(REGISTER_CHUNK) {
            // SAFETY: Invariant -- libc::iovec is repr(C) and its all-zero bit
            // pattern (null base, zero len) is a valid inert iovec, overwritten
            // below before the kernel reads it.
            // Precondition: none -- POD zero initialization.
            // Failure mode: none; a zeroed iovec is inert until filled.
            let mut chunk: [libc::iovec; REGISTER_CHUNK] = unsafe { core::mem::zeroed() };
            for (idx, buf) in chunk_bufs.iter().enumerate() {
                chunk[idx] = libc::iovec {
                    #[allow(
                        clippy::as_ptr_cast_mut,
                        reason = "iov_base requires *mut; buffer is caller-owned and kernel reads only"
                    )]
                    iov_base: buf.as_ptr().cast_mut().cast(),
                    iov_len: buf.len(),
                };
            }

            // SAFETY: Invariant -- each iovec points into the caller's buffer,
            // which must remain valid until unregister or ring drop; the kernel
            // copies the iovec array during io_uring_register(2)
            // IORING_REGISTER_BUFFERS_UPDATE.
            // Precondition: caller is the owning worker thread (SINGLE_ISSUER);
            // chunk[..len] holds initialized iovecs filled in the loop above.
            // Failure mode: a dangling iovec pointer causes the kernel to
            // read freed memory (UB); a wrong offset/len corrupts the slot table.
            unsafe {
                submitter
                    .register_buffers_update(offset, &chunk[..chunk_bufs.len()], None)
                    .map_err(|_| RegisterError::InvalidArgument)?;
            }

            #[allow(
                clippy::cast_possible_truncation,
                reason = "chunk_bufs.len() <= REGISTER_CHUNK (256)"
            )]
            {
                offset += chunk_bufs.len() as u32;
            }
        }

        self.buffers_mut().allocate()
    }

    fn unregister_buffers(&self, group: BufGroupId) -> Result<(), RegisterError> {
        crate::uring::fixed::unregister_buffers(self.ring_mut())?;
        self.buffers_mut().release(group)?;
        Ok(())
    }

    fn register_files(&self, fds: &[i32]) -> Result<FdSlot, RegisterError> {
        crate::uring::fixed::register_files(self.ring_mut(), fds)?;
        self.files_mut().allocate()
    }

    fn unregister_files(&self, slot: FdSlot) -> Result<(), RegisterError> {
        crate::uring::fixed::unregister_files(self.ring_mut())?;
        self.files_mut().release(slot)?;
        Ok(())
    }

    fn provided_recv_group(&self) -> Option<BufGroupId> {
        self.provided_recv_pool().map(BufRingPool::group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RING_ENTRIES: u32 = 32;

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn new_creates_driver() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        assert!(driver.capabilities().single_issuer);
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn tier_is_optimal_or_baseline() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        assert!(driver.tier() == SetupTier::Optimal || driver.tier() == SetupTier::Baseline,);
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn submit_internal_timeout_succeeds() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        let request = IoRequest::<()>::timeout(1_000_000);
        let result = driver.submit_internal(request);
        assert!(matches!(result, SubmitResult::Submitted(_)));
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn poll_completions_drains_submitted() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };

        let request = IoRequest::<()>::timeout(1_000_000).with_user_data(0xBEEF);
        driver.submit_internal(request);

        let mut buf = [Completion {
            token: SubmitToken::new(0),
            result: 0,
            flags: crate::operation::CqeFlags::EMPTY,
            buf_id: None,
        }; 4];

        let count = driver.poll_completions(4, &mut buf);
        assert!(count <= 4);
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn msg_ring_wake_delivers_the_sentinel_to_the_target() {
        let Ok(target) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        let Ok(source) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        if !source.capabilities().msg_ring {
            // Kernel below 5.18 or the feature is masked out; the eventfd wake
            // is the fallback and there is nothing to prove here.
            return;
        }
        assert!(
            matches!(
                source.submit_msg_ring_wake(target.ring_fd()),
                SubmitResult::Submitted(_)
            ),
            "the msg_ring wake submits on the source ring",
        );
        // The CQE is already posted to the target; a bounded park returns once
        // it lands rather than blocking.
        let _outcome = target.park(Some(Duration::from_secs(1)));
        let mut completions = [Completion {
            token: SubmitToken::new(0),
            result: 0,
            flags: crate::operation::CqeFlags::EMPTY,
            buf_id: None,
        }; 4];
        let count = target.poll_completions(4, &mut completions);
        assert!(
            completions[..count]
                .iter()
                .any(|completion| completion.token.user_data()
                    == crate::boundary::MSG_RING_WAKE_USER_DATA),
            "the target ring receives the msg_ring wake sentinel",
        );
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn linked_timeout_cancels_the_primary_on_deadline() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        if !driver.capabilities().link_timeout {
            // Kernel below 5.5 or the opcode is masked; the timer-wheel deadline
            // is the fallback and there is nothing to prove here.
            return;
        }
        // A long primary linked to a short deadline: the 1ms link timeout fires
        // first and cancels the 1s primary. Exercises both scratch timespecs.
        let primary = IoRequest::<()>::timeout(1_000_000_000).with_user_data(0xF00D);
        assert!(
            matches!(
                driver.submit_linked_timeout_internal(&primary, 1_000_000),
                SubmitResult::Submitted(_)
            ),
            "the linked-timeout pair submits on the ring",
        );
        // The deadline (`-ETIME`) and the cancelled primary (`-ECANCELED`) may
        // post in separate CQE batches, so accumulate across a bounded drain.
        let mut primary_cancelled = false;
        let mut discard_seen = false;
        for _ in 0..20 {
            if primary_cancelled && discard_seen {
                break;
            }
            let _outcome = driver.park(Some(Duration::from_millis(100)));
            let mut completions = [Completion {
                token: SubmitToken::new(0),
                result: 0,
                flags: crate::operation::CqeFlags::EMPTY,
                buf_id: None,
            }; 4];
            let count = driver.poll_completions(4, &mut completions);
            for completion in &completions[..count] {
                if completion.token.user_data() == 0xF00D && completion.result == -libc::ECANCELED {
                    primary_cancelled = true;
                }
                if crate::boundary::is_link_timeout_discard(completion.token.user_data()) {
                    discard_seen = true;
                }
            }
        }
        assert!(
            primary_cancelled,
            "the primary op is cancelled with -ECANCELED when the deadline fires",
        );
        assert!(
            discard_seen,
            "the paired link-timeout CQE carries the discard sentinel",
        );
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn park_with_timeout_returns_rather_than_blocking() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        // Nothing is pending, so a bounded park returns the kernel timeout
        // (-ETIME) instead of blocking forever. ETIME is not Rust's `TimedOut`
        // kind, so assert only that the wait returned an error rather than
        // hanging.
        let outcome = driver.park(Some(Duration::from_millis(1)));
        assert!(
            outcome.is_err(),
            "a bounded park with nothing pending returns the ETIME timeout",
        );
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn pool_tracks_buf_ring_capability() {
        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        // Fallback parity, not an iff: without buf_ring support there is no
        // pool and recv stays on the inline path; with support, registration
        // can still fall back to None on failure, so a present pool must be
        // sized to the configured constants, but presence is not required.
        let pool = driver.provided_recv_pool();
        assert!(driver.capabilities().buf_ring || pool.is_none());
        if let Some(pool) = pool {
            assert_eq!(pool.entries(), PROVIDED_RECV_RING_ENTRIES);
            assert_eq!(pool.buf_size(), PROVIDED_RECV_BUF_SIZE);
        }
    }

    #[test]
    fn provided_cancel_window_covers_the_ring() {
        // The cancel window's doc claims one token per pool entry: every
        // buffer id can be awaiting disposal at once. The two constants live
        // in different modules, so pin the coupling here -- resizing either
        // side without the other fails this test instead of silently
        // invalidating the documented bound.
        assert_eq!(
            crate::boundary::PROVIDED_RECV_CANCEL_CAPACITY,
            usize::from(PROVIDED_RECV_RING_ENTRIES),
        );
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn provided_recv_delivers_a_buffer_id() {
        use std::{
            io::Write,
            os::{fd::AsRawFd, unix::net::UnixStream},
        };

        const TOKEN: u64 = 0xB0F1;

        let Ok(driver) = UringDriver::new(TEST_RING_ENTRIES) else {
            panic!("UringDriver::new failed");
        };
        let Some(group) = driver.provided_recv_group() else {
            // No buf_ring support here; the inline-buffer recv path is the
            // fallback and there is nothing to probe.
            return;
        };

        let Ok((mut writer, reader)) = UnixStream::pair() else {
            panic!("socketpair creation must succeed");
        };
        let payload = b"hello provided buffer";
        let Ok(written) = writer.write(payload) else {
            panic!("write to the pair must succeed");
        };
        assert_eq!(written, payload.len());

        let request =
            IoRequest::<()>::recv_provided(reader.as_raw_fd(), group).with_user_data(TOKEN);
        assert!(matches!(
            driver.submit_internal(request),
            SubmitResult::Submitted(_)
        ));

        // The data is already waiting; a bounded park returns once the recv
        // completes rather than blocking.
        let _outcome = driver.park(Some(Duration::from_secs(1)));

        let mut completions = [Completion {
            token: SubmitToken::new(0),
            result: 0,
            flags: crate::operation::CqeFlags::EMPTY,
            buf_id: None,
        }; 4];
        let count = driver.poll_completions(4, &mut completions);

        let Some(cqe) = completions[..count]
            .iter()
            .find(|completion| completion.token.user_data() == TOKEN)
        else {
            panic!("the provided-recv completion must arrive");
        };
        let Ok(expected) = i32::try_from(payload.len()) else {
            panic!("payload length fits i32");
        };
        // len=0 fills the kernel-selected buffer up to the payload size.
        assert_eq!(cqe.result, expected);
        assert!(
            cqe.buf_id.is_some(),
            "the kernel must report the selected buffer id"
        );
    }
}
