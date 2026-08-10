//! `DriverType` -- enum dispatch over the available platform backends.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) satisfies unreachable_pub on this private module"
)]

use std::{io, os::fd::OwnedFd, time::Duration};

#[cfg(unix)]
use crate::buffer::oneshot::inflight::InflightBufSlab;
#[cfg(target_os = "linux")]
use crate::buffer::ring::pool::BufRingPool;
#[cfg(target_os = "linux")]
use crate::uring::backend::UringDriver;
use crate::{
    CancelError, IoDriver, RegisterError,
    buffer::registration::slot::{BufGroupId, FdSlot},
    capability::CapabilityMatrix,
    operation::{Completion, CqeFlags, IoBuf, IoBufMut, IoRequest, SubmitResult, SubmitToken},
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use crate::{
    boundary::seam::socket::{Retry, retry_recv, retry_send},
    operation::OpCode,
};

/// Enum dispatch over the available platform backends.
///
/// Each variant wraps a concrete backend. The compiler selects which variants
/// exist via `#[cfg]`. Within the crate, cfg-selected variants make the match
/// exhaustive; external code must include a wildcard arm due to
/// `#[non_exhaustive]`.
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "UringDriver is the primary variant; Box indirection banned by allocation policy"
)]
pub enum DriverType {
    /// `io_uring` backend -- Linux 5.11+ production target.
    #[cfg(target_os = "linux")]
    Uring(UringDriver),

    /// epoll fallback -- Linux without `io_uring` (seccomp, legacy kernel).
    #[cfg(target_os = "linux")]
    Epoll(()),

    /// kqueue backend -- macOS / BSD local development.
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    Kqueue(()),

    /// IOCP backend -- Windows general async runtime. Deferred to 0.2.0+.
    #[cfg(target_os = "windows")]
    #[doc(hidden)]
    Iocp(()),

    /// Windows IoRing backend. Deferred to 0.2.0+.
    #[cfg(target_os = "windows")]
    #[doc(hidden)]
    IoRing(()),
}

static STUB_CAPS: CapabilityMatrix = CapabilityMatrix::thin_fallback();

/// A submit result whose deferred arm owns the descriptor for a later retry.
pub(crate) enum SlotSubmit {
    /// The existing uniform submit result.
    Resolved(SubmitResult),
    /// A readiness operation would block and transfers its duplicate.
    #[expect(
        dead_code,
        reason = "the readiness submit path produces deferrals in #330"
    )]
    Deferred(OwnedFd),
}

/// Retries the deferred operation whose retained descriptor is `fd`.
///
/// A short transfer is terminal and reported as its syscall count: the
/// completion model permits one completion per token, so it cannot be re-armed.
/// Readiness backends have no provided-buffer analogue, so synthetic
/// completions carry empty flags and no buffer id.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "called by readiness event sources in #330")
)]
pub(crate) fn synthesize(slab: &mut InflightBufSlab, fd: i32) -> Option<Completion> {
    let key = slab.deferred_key_by_fd(fd)?;
    let retry = {
        let (fd, bytes, opcode) = slab.retry_parts(key)?;
        match opcode {
            OpCode::Recv => retry_recv(fd, bytes),
            OpCode::Send => retry_send(fd, bytes),
            // `absorb_deferred` accepts only these two opcodes. Keeping an
            // unexpected record intact avoids widening that acceptance set.
            _ => return None,
        }
    };
    let result = match retry {
        Retry::Done(bytes) => i32::try_from(bytes).map_or(-libc::EINVAL, |bytes| bytes),
        Retry::Failed(error) => error,
        Retry::WouldBlock => return None,
    };
    if !slab.retire_deferred(key) {
        return None;
    }
    Some(Completion {
        token: SubmitToken::new(key.op_token),
        result,
        flags: CqeFlags::default(),
        buf_id: None,
    })
}

#[allow(
    unused_variables,
    reason = "parameters consumed by cfg-gated Uring arm on Linux; unused on other platforms"
)]
impl IoDriver for DriverType {
    fn submit<B: IoBuf>(&self, request: IoRequest<B>) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.submit(request),
            _ => SubmitResult::Unsupported,
        }
    }

    fn submit_read<B: IoBufMut>(&self, request: IoRequest<B>) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.submit_read(request),
            _ => SubmitResult::Unsupported,
        }
    }

    fn submit_internal(&self, request: IoRequest<()>) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.submit_internal(request),
            _ => SubmitResult::Unsupported,
        }
    }

    fn poll_completions(&self, max: usize, out: &mut [Completion]) -> usize {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.poll_completions(max, out),
            _ => 0,
        }
    }

    fn capabilities(&self) -> &CapabilityMatrix {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.capabilities(),
            _ => &STUB_CAPS,
        }
    }

    fn cancel(&self, token: SubmitToken) -> Result<(), CancelError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.cancel(token),
            _ => Err(CancelError::BestEffortDetach),
        }
    }

    fn register_buffers(&self, bufs: &[&[u8]]) -> Result<BufGroupId, RegisterError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.register_buffers(bufs),
            _ => Err(RegisterError::Unsupported),
        }
    }

    fn unregister_buffers(&self, group: BufGroupId) -> Result<(), RegisterError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.unregister_buffers(group),
            _ => Err(RegisterError::Unsupported),
        }
    }

    fn register_files(&self, fds: &[i32]) -> Result<FdSlot, RegisterError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.register_files(fds),
            _ => Err(RegisterError::Unsupported),
        }
    }

    fn unregister_files(&self, slot: FdSlot) -> Result<(), RegisterError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.unregister_files(slot),
            _ => Err(RegisterError::Unsupported),
        }
    }

    fn provided_recv_group(&self) -> Option<BufGroupId> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.provided_recv_group(),
            _ => None,
        }
    }
}

impl DriverType {
    /// Drains native or readiness-synthesized completions for the run loop.
    ///
    /// The `io_uring` arm forwards to [`IoDriver::poll_completions`]. Readiness
    /// arms return zero until their event sources are installed.
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like [`park`](Self::park):
    /// readiness synthesis is run-loop plumbing, not part of the uniform completion
    /// API.
    #[doc(hidden)]
    #[allow(
        unused_variables,
        reason = "readiness backends gain an event source in #330; until then only the uring arm consumes the batch"
    )]
    pub fn drain_ready(
        &self,
        slab: &mut InflightBufSlab,
        max: usize,
        out: &mut [Completion],
    ) -> usize {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.poll_completions(max, out),
            _ => 0,
        }
    }

    /// Submits a write-class request through the seam-only deferral boundary.
    #[allow(
        unused_variables,
        reason = "only the cfg-gated io_uring arm consumes `request`; placeholder backends refuse it"
    )]
    pub(crate) fn submit_deferrable<B: IoBuf>(&self, request: IoRequest<B>) -> SlotSubmit {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => SlotSubmit::Resolved(driver.submit(request)),
            _ => SlotSubmit::Resolved(SubmitResult::Unsupported),
        }
    }

    /// Submits a read-class request through the seam-only deferral boundary.
    #[allow(
        unused_variables,
        reason = "only the cfg-gated io_uring arm consumes `request`; placeholder backends refuse it"
    )]
    pub(crate) fn submit_deferrable_read<B: IoBufMut>(&self, request: IoRequest<B>) -> SlotSubmit {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => SlotSubmit::Resolved(driver.submit_read(request)),
            _ => SlotSubmit::Resolved(SubmitResult::Unsupported),
        }
    }

    /// The provided-buffer pool the `io_uring` backend registered, if any.
    ///
    /// `None` on every fallback backend, and on a uring driver whose kernel
    /// lacks `buf_ring` or whose registration failed -- the same degradation
    /// [`provided_recv_group`](IoDriver::provided_recv_group) reports, so the
    /// two accessors stay in fallback parity.
    #[cfg(target_os = "linux")]
    pub(crate) const fn provided_recv_pool(&self) -> Option<&BufRingPool> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.provided_recv_pool(),
            _ => None,
        }
    }

    /// Builds the platform's default driver, running the `io_uring`
    /// capability probe once on Linux. Startup backend selection per the
    /// support matrix; Windows backends are deferred to 0.2.0.
    ///
    /// # Errors
    ///
    /// Propagates the backend constructor error (e.g. an `io_uring` setup
    /// failure under seccomp or an unsupported kernel).
    #[doc(hidden)]
    #[allow(
        clippy::missing_const_for_fn,
        clippy::unnecessary_wraps,
        unused_variables,
        reason = "only the cfg-gated io_uring arm uses `entries`, returns `Err`, or is non-const; the thin-fallback arms are trivial const Ok"
    )]
    pub fn for_platform(entries: u32) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self::Uring(UringDriver::new(entries)?))
        }
        #[cfg(any(
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd"
        ))]
        {
            Ok(Self::Kqueue(()))
        }
        #[cfg(target_os = "windows")]
        {
            Ok(Self::Iocp(()))
        }
    }

    /// Blocks the worker until a completion is ready or `deadline` elapses.
    ///
    /// Dispatches to the `io_uring` backend. Thin-fallback backends have no
    /// blocking wait in this build and return `Ok(0)`. This stays an inherent
    /// method (not on `IoDriver`) so the backend surface remains completion
    /// only.
    ///
    /// # Errors
    ///
    /// Propagates the backend wait error. A `Some` timeout that elapses
    /// surfaces as the kernel `-ETIME`, not Rust's `TimedOut` kind.
    #[doc(hidden)]
    #[allow(
        unused_variables,
        clippy::missing_const_for_fn,
        clippy::unnecessary_wraps,
        reason = "the cfg-gated io_uring arm is the only path that uses `deadline`, returns `Err`, or is non-const; on thin-fallback builds park degenerates to a trivial Ok(0)"
    )]
    pub fn park(&self, deadline: Option<Duration>) -> io::Result<usize> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.park(deadline),
            _ => Ok(0),
        }
    }

    /// Flushes deferred completion task work on the backend's ring.
    ///
    /// Only the `io_uring` backend defers task work (`DEFER_TASKRUN`); every
    /// other backend posts completions eagerly and returns zero, as does a
    /// uring ring set up without the flag. The run loop calls this ahead
    /// of every completion drain so a worker that never parks still reaps.
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like
    /// [`park`](Self::park): the flush is run-loop plumbing, not part of
    /// the uniform completion API.
    ///
    /// # Errors
    ///
    /// Returns the backend's `io_uring_enter` error.
    #[doc(hidden)]
    #[allow(
        clippy::missing_const_for_fn,
        clippy::unnecessary_wraps,
        reason = "the cfg-gated io_uring arm is the only path that performs the enter or returns `Err`; on thin-fallback builds the flush degenerates to a trivial Ok(0)"
    )]
    pub fn flush_deferred(&self) -> io::Result<usize> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.flush_deferred(),
            _ => Ok(0),
        }
    }

    /// Arms a oneshot read on the wake fd so a remote signal completes the
    /// park as a CQE carrying `user_data`. Unsupported off the uring
    /// backend.
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like
    /// [`park`](Self::park): the wake fd is run-loop plumbing, not part of
    /// the uniform completion API.
    #[allow(
        unused_variables,
        clippy::missing_const_for_fn,
        reason = "only the cfg-gated io_uring arm uses `fd`/`user_data` or is non-const; on thin-fallback builds the arm degenerates to a trivial const Unsupported"
    )]
    pub fn arm_wake_read(&self, fd: i32, user_data: u64) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.arm_wake_read(fd, user_data),
            _ => SubmitResult::Unsupported,
        }
    }

    /// The raw fd of the backend's own ring -- the target a peer names in an
    /// `IORING_OP_MSG_RING` wake. `None` off the uring backend, which has no
    /// ring to target and falls back to the eventfd wake.
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like [`park`](Self::park):
    /// the ring fd is run-loop plumbing, not part of the uniform completion API.
    #[doc(hidden)]
    #[allow(
        clippy::missing_const_for_fn,
        reason = "only the cfg-gated io_uring arm is non-const; on thin-fallback builds the accessor degenerates to a trivial const None"
    )]
    pub fn ring_fd(&self) -> Option<i32> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => Some(driver.ring_fd()),
            _ => None,
        }
    }

    /// Submits an `IORING_OP_MSG_RING` wake on the backend's ring targeting
    /// `target_ring_fd`.
    ///
    /// [`SubmitResult::Unsupported`] off the uring backend or when the kernel
    /// lacks `msg_ring`; the caller falls back to the eventfd wake (fallback
    /// parity).
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like [`park`](Self::park):
    /// cross-ring wake is run-loop plumbing, not part of the uniform completion
    /// API.
    #[doc(hidden)]
    #[allow(
        unused_variables,
        clippy::missing_const_for_fn,
        reason = "only the cfg-gated io_uring arm uses `target_ring_fd` or is non-const; on thin-fallback builds the submit degenerates to a trivial const Unsupported"
    )]
    pub fn submit_msg_ring_wake(&self, target_ring_fd: i32) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.submit_msg_ring_wake(target_ring_fd),
            _ => SubmitResult::Unsupported,
        }
    }

    /// Submits `request` bounded by a native `IORING_OP_LINK_TIMEOUT` deadline
    /// on the backend's ring.
    ///
    /// [`SubmitResult::Unsupported`] off the uring backend or when the kernel
    /// lacks `link_timeout`; the caller falls back to the timer-wheel deadline
    /// (fallback parity).
    ///
    /// Kept off the [`IoDriver`](crate::IoDriver) trait like [`park`](Self::park):
    /// the native deadline is a submit-path optimization, not part of the uniform
    /// completion API.
    #[doc(hidden)]
    #[allow(
        unused_variables,
        clippy::missing_const_for_fn,
        reason = "only the cfg-gated io_uring arm uses `request`/`deadline_ns` or is non-const; on thin-fallback builds the submit degenerates to a trivial const Unsupported"
    )]
    pub fn submit_linked_timeout_internal(
        &self,
        request: &IoRequest<()>,
        deadline_ns: u64,
    ) -> SubmitResult {
        match self {
            #[cfg(target_os = "linux")]
            Self::Uring(driver) => driver.submit_linked_timeout_internal(request, deadline_ns),
            _ => SubmitResult::Unsupported,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    use std::{
        io::{Read, Write},
        os::{fd::AsRawFd, unix::net::UnixStream},
    };

    use super::*;
    #[cfg(all(target_os = "linux", not(miri)))]
    use crate::boundary::seam::socket::shrink_socket_buffers;
    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    use crate::{
        boundary::seam::socket::{duplicate_fd, restore_sigpipe_default_for_test},
        buffer::oneshot::inflight::{DeferredOp, InflightBufSlab},
    };
    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    fn slab() -> InflightBufSlab {
        let Ok(slab) = InflightBufSlab::new(1, 8) else {
            panic!("mmap must succeed for the retry slab");
        };
        slab
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    fn defer(
        slab: &mut InflightBufSlab,
        fd: i32,
        token: u64,
        len: u32,
        opcode: OpCode,
    ) -> (crate::buffer::oneshot::inflight::InflightSlotKey, i32) {
        let Some(key) = slab.allocate(token) else {
            panic!("the retry slot must allocate");
        };
        let Ok(duplicated) = duplicate_fd(fd) else {
            panic!("the retry descriptor must duplicate");
        };
        let deferred = DeferredOp {
            fd: duplicated.into_owned(),
            len,
            opcode,
        };
        let retry_fd = deferred.fd.as_raw_fd();
        assert!(slab.mark_deferred_by_op_token(token, deferred));
        (key, retry_fd)
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_recv_completes_once_and_preserves_slot_bytes() {
        let Ok((server, mut client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let mut slab = slab();
        let (key, retry_fd) = defer(&mut slab, server.as_raw_fd(), 41, 4, OpCode::Recv);
        let Ok(()) = client.write_all(b"recv") else {
            panic!("the peer must make the receive descriptor ready");
        };
        let Some(completion) = synthesize(&mut slab, retry_fd) else {
            panic!("a ready receive must synthesize a completion");
        };
        assert_eq!(completion.token.user_data(), 41);
        assert_eq!(completion.result, 4);
        assert_eq!(slab.slot_slice(key, 4), Some(&b"recv"[..]));
        assert!(synthesize(&mut slab, retry_fd).is_none());
        assert!(slab.deferred_key_by_fd(retry_fd).is_none());
        assert!(slab.slot_array(key).is_some());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_send_completes_once_and_peer_receives_bytes() {
        let Ok((mut server, client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let mut slab = slab();
        let (key, retry_fd) = defer(&mut slab, client.as_raw_fd(), 42, 4, OpCode::Send);
        let Some(bytes) = slab.slot_array_mut(key) else {
            panic!("the send slot must be writable");
        };
        bytes[..4].copy_from_slice(b"send");
        let Some(completion) = synthesize(&mut slab, retry_fd) else {
            panic!("a ready send must synthesize a completion");
        };
        assert_eq!(completion.token.user_data(), 42);
        assert_eq!(completion.result, 4);
        let mut received = [0u8; 4];
        let Ok(()) = server.read_exact(&mut received) else {
            panic!("the peer must receive the retry bytes");
        };
        assert_eq!(received, *b"send");
        assert!(synthesize(&mut slab, retry_fd).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_unready_recv_keeps_its_record() {
        let Ok((server, _client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let mut slab = slab();
        let (key, retry_fd) = defer(&mut slab, server.as_raw_fd(), 43, 4, OpCode::Recv);
        assert!(synthesize(&mut slab, retry_fd).is_none());
        assert_eq!(slab.deferred_key_by_fd(retry_fd), Some(key));
        assert!(slab.deferred(key).is_some());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_ignores_unowned_and_reclaimed_descriptors() {
        let Ok((server, _client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let mut slab = slab();
        assert!(synthesize(&mut slab, server.as_raw_fd()).is_none());

        let (key, retry_fd) = defer(&mut slab, server.as_raw_fd(), 46, 1, OpCode::Recv);
        slab.free(key);
        assert!(synthesize(&mut slab, retry_fd).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_send_avoids_sigpipe_with_default_disposition() {
        const CHILD: &str = "KWOKKA_IO_SYNTHESIS_SIGPIPE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let Ok(executable) = std::env::current_exe() else {
                panic!("the test executable path must be available");
            };
            let Ok(status) = std::process::Command::new(executable)
                .arg("--exact")
                .arg("driver::dispatch::tests::synthesize_send_avoids_sigpipe_with_default_disposition")
                .env(CHILD, "1")
                .status()
            else {
                panic!("the SIGPIPE fixture must start in its own process");
            };
            assert!(
                status.success(),
                "the isolated retry send must survive SIGPIPE"
            );
            return;
        }
        let Ok((server, mut client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        drop(server);
        let mut eof = [0u8; 1];
        let Ok(0) = client.read(&mut eof) else {
            panic!("the closed peer must be observed before sending");
        };
        let mut slab = slab();
        let (key, retry_fd) = defer(&mut slab, client.as_raw_fd(), 44, 1, OpCode::Send);
        let Some(bytes) = slab.slot_array_mut(key) else {
            panic!("the send slot must be writable");
        };
        bytes[0] = b'x';
        restore_sigpipe_default_for_test();
        let Some(completion) = synthesize(&mut slab, retry_fd) else {
            panic!("the closed peer must produce a completion");
        };
        assert_eq!(completion.result, -libc::EPIPE);
        assert!(synthesize(&mut slab, retry_fd).is_none());
    }

    #[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
    #[test]
    fn synthesize_reports_a_short_receive_count() {
        let Ok((server, mut client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let mut slab = slab();
        let (_key, retry_fd) = defer(&mut slab, server.as_raw_fd(), 45, 2, OpCode::Recv);
        let Ok(()) = client.write_all(b"more") else {
            panic!("the peer must make the receive descriptor ready");
        };
        let Some(completion) = synthesize(&mut slab, retry_fd) else {
            panic!("a ready receive must synthesize a completion");
        };
        assert_eq!(completion.result, 2);
        assert!(synthesize(&mut slab, retry_fd).is_none());
    }

    // The constrained-buffer fixture is only deterministic on Linux.
    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn synthesize_reports_a_short_send_count_and_retires_the_record() {
        let Ok((mut server, client)) = UnixStream::pair() else {
            panic!("a socket pair must be created");
        };
        let Ok(()) = shrink_socket_buffers(&server) else {
            panic!("the peer receive buffer must shrink");
        };
        let Ok(()) = shrink_socket_buffers(&client) else {
            panic!("the send buffer must shrink");
        };
        let Ok(filler) = duplicate_fd(client.as_raw_fd()) else {
            panic!("the queue-filling descriptor must duplicate");
        };
        let filler = filler.into_owned();
        let mut full = false;
        for _ in 0..4096 {
            match retry_send(&filler, b"x") {
                Retry::Done(_) => {}
                Retry::WouldBlock => {
                    full = true;
                    break;
                }
                Retry::Failed(error) => panic!("the queue-filling send failed: {error}"),
            }
        }
        assert!(full, "the constrained peer queue must fill");
        let mut slab = slab();
        let (key, retry_fd) = defer(&mut slab, client.as_raw_fd(), 47, 4096, OpCode::Send);
        let Some(bytes) = slab.slot_array_mut(key) else {
            panic!("the send slot must be writable");
        };
        bytes.fill(b'x');

        let mut released = [0u8; 1];
        let completion = (0..256).find_map(|_| {
            let Ok(()) = server.read_exact(&mut released) else {
                panic!("the peer must release a queued byte");
            };
            synthesize(&mut slab, retry_fd)
        });
        let Some(completion) = completion else {
            panic!("the constrained peer must accept a partial write after 256 reads");
        };
        assert!(completion.result > 0);
        assert!(completion.result < 4096);
        assert!(synthesize(&mut slab, retry_fd).is_none());
        assert!(slab.deferred_key_by_fd(retry_fd).is_none());
    }

    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn drain_ready_returns_zero_for_the_placeholder_epoll_backend() {
        let mut slab = slab();
        let mut completions = [Completion::default(); 1];
        assert_eq!(
            DriverType::Epoll(()).drain_ready(&mut slab, 1, &mut completions),
            0
        );
    }

    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn drain_ready_forwards_the_uring_batch_contract() {
        let Ok(uring_driver) = UringDriver::new(32) else {
            panic!("the uring test ring must be created");
        };
        let driver = DriverType::Uring(uring_driver);
        let mut slab = slab();
        let mut completions = [Completion::default(); 1];

        assert!(matches!(
            driver.submit_internal(IoRequest::<()>::timeout(1_000_000).with_user_data(0xBEEF)),
            SubmitResult::Submitted(_)
        ));
        let Ok(_) = driver.park(None) else {
            panic!("the submitted timeout must complete");
        };

        assert_eq!(driver.drain_ready(&mut slab, 1, &mut completions), 1);
        assert_eq!(completions[0].token.user_data(), 0xBEEF);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_submit_returns_unsupported() {
        let result = DriverType::Epoll(()).submit_internal(IoRequest::<()>::accept(3));
        assert!(matches!(result, SubmitResult::Unsupported));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_ring_fd_is_none() {
        assert_eq!(
            DriverType::Epoll(()).ring_fd(),
            None,
            "a backend with no ring has no msg_ring target",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_msg_ring_wake_returns_unsupported() {
        assert!(
            matches!(
                DriverType::Epoll(()).submit_msg_ring_wake(5),
                SubmitResult::Unsupported
            ),
            "the msg_ring wake falls back to eventfd off the uring backend",
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_linked_timeout_returns_unsupported() {
        assert!(
            matches!(
                DriverType::Epoll(())
                    .submit_linked_timeout_internal(&IoRequest::<()>::accept(3), 1_000_000),
                SubmitResult::Unsupported
            ),
            "the linked-timeout submit falls back to the timer wheel off the uring backend",
        );
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    #[test]
    fn kqueue_capabilities_returns_thin_fallback() {
        assert!(!DriverType::Kqueue(()).capabilities().defer_taskrun);
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    #[test]
    fn kqueue_register_files_returns_unsupported() {
        let Err(error) = DriverType::Kqueue(()).register_files(&[]) else {
            panic!("expected Err");
        };
        assert_eq!(error, RegisterError::Unsupported);
    }
}
