//! Backend capability snapshot and kernel version types.
//!
//! # Feature gating and fallback parity
//!
//! Each `io_uring` advanced capability sits behind a Cargo feature
//! (compile-time) AND the runtime probe (kernel support): the flag is
//! `true` only when both hold. Otherwise the caller takes the
//! correctness-equivalent fallback, which compiles regardless of the
//! feature, so turning a feature off never removes a working path.
//!
//! | Capability | Feature | Kernel | epoll / kqueue (completion via internal readiness) |
//! |---|---|---|---|
//! | `buf_ring` | `uring-buf-ring` (default) | 5.19 | userspace buffer pool selects the slot |
//! | `multishot_accept` / `multishot_recv` | `uring-multishot` (default) | 5.19 / 6.0 | readiness loop synthesizes each completion |
//! | `msg_ring` | `uring-msg-ring` (default) | 5.18 | eventfd or kqueue user-event wake completion |
//! | `send_zc` / `sendmsg_zc` | `uring-send-zc` (opt-in) | 6.0 / 6.1 | plain send completion, no zero-copy |
//!
//! `io_uring` and `IOCP` are native completion backends; epoll and kqueue
//! synthesize completions from internal readiness. The rightmost column
//! is the parity contract for the eventual readiness backends.

/// Kernel version triple.
///
/// Used by [`CapabilityMatrix`] to record the kernel version detected at
/// startup. Thin-fallback backends set this to [`KernelVersion::ZERO`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct KernelVersion {
    /// Major version component.
    pub major: u32,
    /// Minor version component.
    pub minor: u32,
    /// Patch version component.
    pub patch: u32,
}

impl KernelVersion {
    /// Version `0.0.0` - returned by thin-fallback backends.
    pub const ZERO: Self = Self {
        major: 0,
        minor: 0,
        patch: 0,
    };

    /// Constructs a kernel version triple.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// `true` if this version is `(major, minor, 0)` or later.
    pub const fn at_least(self, major: u32, minor: u32) -> bool {
        self.major > major || (self.major == major && self.minor >= minor)
    }
}

/// Snapshot of backend capabilities detected at runtime.
///
/// Populated by the backend's startup probe (e.g. `capability/detect.rs`) and
/// intended to be cached for the process lifetime. User code will query it
/// via `IoDriver::capabilities()` to decide which code paths are available.
///
/// Conservative default: [`CapabilityMatrix::thin_fallback`] - all
/// `io_uring`-specific flags false. Use [`CapabilityMatrix::full`] in tests.
#[allow(
    clippy::struct_excessive_bools,
    reason = "each field maps to a distinct kernel feature flag with no natural grouping"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CapabilityMatrix {
    /// Kernel version detected at ring setup. [`KernelVersion::ZERO`] on thin fallbacks.
    pub kernel_version: KernelVersion,

    /// `IORING_SETUP_DEFER_TASKRUN` - executor-controlled completion timing (5.19+).
    pub defer_taskrun: bool,
    /// `IORING_SETUP_COOP_TASKRUN` - `DEFER_TASKRUN` fallback (5.19+).
    pub coop_taskrun: bool,
    /// `IORING_SETUP_SINGLE_ISSUER` - eliminates kernel SQ locking (5.18+).
    pub single_issuer: bool,
    /// `IORING_SETUP_SQPOLL` - eliminates submit syscall. Reserved for 0.2.0+.
    pub sqpoll: bool,

    /// `IORING_REGISTER_BUFFERS` - fixed-buffer registered I/O.
    pub registered_buffers: bool,
    /// `IORING_REGISTER_FILES` - fixed-file table eliminates per-op fd lookup.
    pub registered_files: bool,
    /// Maximum number of registration slots (buffers or files).
    pub max_register_slots: u32,

    /// `IORING_REGISTER_PBUF_RING` - kernel-selected recv buffers (5.19+).
    pub buf_ring: bool,
    /// `IORING_ACCEPT_MULTISHOT` - single SQE drives an accept loop (5.19+).
    pub multishot_accept: bool,
    /// `IORING_RECV_MULTISHOT` - single SQE drives a recv loop (6.0+).
    pub multishot_recv: bool,

    /// `IORING_OP_MSG_RING` - cross-ring worker wake without eventfd (5.18+).
    pub msg_ring: bool,

    /// `IORING_OP_SEND_ZC` - zero-copy send (6.0+).
    pub send_zc: bool,
    /// `IORING_OP_SENDMSG_ZC` - zero-copy sendmsg (6.1+).
    pub sendmsg_zc: bool,
    /// `IORING_OP_RECV_ZC` (`zcrx`) - zero-copy recv. Reserved for 0.2.0+ (6.10+, unstable API).
    pub recv_zc: bool,

    /// `IORING_OP_ASYNC_CANCEL` - in-flight op cancellation (5.5+).
    pub async_cancel: bool,

    /// Minimum buffer alignment for `O_DIRECT`. Default 4096.
    pub direct_io_align: usize,
}

impl CapabilityMatrix {
    /// Conservative baseline - no `io_uring`-specific features, `direct_io_align` = 4096.
    ///
    /// Used by epoll/kqueue thin-fallback backends and as the `Default` impl.
    pub const fn thin_fallback() -> Self {
        Self {
            kernel_version: KernelVersion::ZERO,
            defer_taskrun: false,
            coop_taskrun: false,
            single_issuer: false,
            sqpoll: false,
            registered_buffers: false,
            registered_files: false,
            max_register_slots: 0,
            buf_ring: false,
            multishot_accept: false,
            multishot_recv: false,
            msg_ring: false,
            send_zc: false,
            sendmsg_zc: false,
            recv_zc: false,
            async_cancel: false,
            direct_io_align: 4096,
        }
    }

    /// All capabilities enabled - for use in unit tests only.
    pub const fn full() -> Self {
        Self {
            kernel_version: KernelVersion::new(6, 10, 0),
            defer_taskrun: true,
            coop_taskrun: true,
            single_issuer: true,
            sqpoll: true,
            registered_buffers: true,
            registered_files: true,
            max_register_slots: 1024,
            buf_ring: true,
            multishot_accept: true,
            multishot_recv: true,
            msg_ring: true,
            send_zc: true,
            sendmsg_zc: true,
            recv_zc: true,
            async_cancel: true,
            direct_io_align: 4096,
        }
    }
}

impl Default for CapabilityMatrix {
    fn default() -> Self {
        Self::thin_fallback()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_version_zero_is_minimum() {
        assert!(KernelVersion::ZERO < KernelVersion::new(5, 11, 0));
    }

    #[test]
    fn kernel_version_ord_compares_major_first() {
        assert!(KernelVersion::new(6, 0, 0) > KernelVersion::new(5, 19, 0));
    }

    #[test]
    fn kernel_version_at_least_same_version() {
        let version = KernelVersion::new(5, 19, 0);
        assert!(version.at_least(5, 19));
        assert!(version.at_least(5, 18));
        assert!(!version.at_least(6, 0));
    }

    #[test]
    fn kernel_version_at_least_higher_major() {
        assert!(KernelVersion::new(6, 0, 0).at_least(5, 19));
    }

    #[test]
    fn thin_fallback_all_bools_false() {
        let cap = CapabilityMatrix::thin_fallback();
        assert!(!cap.defer_taskrun);
        assert!(!cap.coop_taskrun);
        assert!(!cap.single_issuer);
        assert!(!cap.sqpoll);
        assert!(!cap.registered_buffers);
        assert!(!cap.registered_files);
        assert!(!cap.buf_ring);
        assert!(!cap.multishot_accept);
        assert!(!cap.multishot_recv);
        assert!(!cap.msg_ring);
        assert!(!cap.send_zc);
        assert!(!cap.sendmsg_zc);
        assert!(!cap.recv_zc);
        assert!(!cap.async_cancel);
    }

    #[test]
    fn thin_fallback_direct_io_align_is_4096() {
        assert_eq!(CapabilityMatrix::thin_fallback().direct_io_align, 4096);
    }

    #[test]
    fn thin_fallback_equals_default() {
        assert_eq!(
            CapabilityMatrix::default(),
            CapabilityMatrix::thin_fallback()
        );
    }

    #[test]
    fn full_all_bools_true() {
        let cap = CapabilityMatrix::full();
        assert!(cap.defer_taskrun);
        assert!(cap.single_issuer);
        assert!(cap.registered_buffers);
        assert!(cap.multishot_accept);
        assert!(cap.send_zc);
        assert!(cap.async_cancel);
    }

    #[test]
    fn full_kernel_version_at_least_6_0() {
        assert!(CapabilityMatrix::full().kernel_version.at_least(6, 0));
    }
}
