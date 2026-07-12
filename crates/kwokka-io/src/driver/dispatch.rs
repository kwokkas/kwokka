//! `DriverType` -- enum dispatch over the available platform backends.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) satisfies unreachable_pub on this private module"
)]

use std::{io, time::Duration};

#[cfg(target_os = "linux")]
use crate::uring::backend::UringDriver;
use crate::{
    CancelError, IoDriver, RegisterError,
    buffer::{
        registration::slot::{BufGroupId, FdSlot},
        ring::pool::BufRingPool,
    },
    capability::CapabilityMatrix,
    operation::{Completion, IoBuf, IoBufMut, IoRequest, SubmitResult, SubmitToken},
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
    /// The provided-buffer pool the `io_uring` backend registered, if any.
    ///
    /// `None` on every fallback backend, and on a uring driver whose kernel
    /// lacks `buf_ring` or whose registration failed -- the same degradation
    /// [`provided_recv_group`](IoDriver::provided_recv_group) reports, so the
    /// two accessors stay in fallback parity.
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
    use super::*;

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
