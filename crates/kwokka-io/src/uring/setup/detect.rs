//! Kernel `io_uring` feature probe.
//!
//! Bootstraps a ring with graceful-degrade setup flags and probes
//! per-opcode support to populate a [`CapabilityMatrix`]. The probe
//! runs once at startup; results are cached for the process lifetime.

#![allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) on module-private items"
)]

use std::io;

use io_uring::{IoUring, opcode};

use crate::{
    capability::{CapabilityMatrix, KernelVersion},
    uring::setup::flags::SetupTier,
};

/// Result of the `io_uring` capability probe.
pub(crate) struct ProbeResult {
    /// The configured `io_uring` instance.
    pub ring: IoUring,
    /// Detected capabilities.
    pub capabilities: CapabilityMatrix,
    /// Setup tier achieved.
    pub tier: SetupTier,
}

/// Probe the kernel and create a ring with the best available setup flags.
///
/// Attempts tier 1 (optimal) first, falling back to tier 2 (baseline)
/// on `EINVAL`. With the 6.0+ kernel minimum, both tiers set
/// `COOP_TASKRUN` + `SINGLE_ISSUER`; tier 1 adds `DEFER_TASKRUN`.
///
/// # Errors
///
/// Returns an error if no setup tier succeeds (kernel too old or
/// `io_uring` disabled via sysctl).
pub(crate) fn probe_and_create(entries: u32) -> io::Result<ProbeResult> {
    let (ring, tier) = try_tier_optimal(entries).or_else(|_| try_tier_baseline(entries))?;

    let capabilities = detect_capabilities(&ring, tier)?;

    Ok(ProbeResult {
        ring,
        capabilities,
        tier,
    })
}

fn try_tier_optimal(entries: u32) -> io::Result<(IoUring, SetupTier)> {
    let ring = IoUring::builder()
        .setup_defer_taskrun()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .build(entries)?;
    Ok((ring, SetupTier::Optimal))
}

fn try_tier_baseline(entries: u32) -> io::Result<(IoUring, SetupTier)> {
    let ring = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .build(entries)?;
    Ok((ring, SetupTier::Baseline))
}

fn detect_capabilities(ring: &IoUring, tier: SetupTier) -> io::Result<CapabilityMatrix> {
    let params = ring.params();

    let mut probe = io_uring::Probe::new();
    ring.submitter().register_probe(&mut probe)?;

    let kernel_version = read_kernel_version();

    Ok(CapabilityMatrix {
        kernel_version,

        defer_taskrun: tier == SetupTier::Optimal,
        coop_taskrun: true,
        single_issuer: params.is_setup_single_issuer(),
        sqpoll: false,

        registered_buffers: true,
        registered_files: true,
        max_register_slots: 1024,

        buf_ring: cfg!(feature = "uring-buf-ring") && kernel_version.at_least(5, 19),
        multishot_accept: cfg!(feature = "uring-multishot")
            && probe.is_supported(opcode::AcceptMulti::CODE),
        multishot_recv: cfg!(feature = "uring-multishot")
            && probe.is_supported(opcode::RecvMulti::CODE),

        msg_ring: cfg!(feature = "uring-msg-ring") && probe.is_supported(opcode::MsgRingData::CODE),

        send_zc: cfg!(feature = "uring-send-zc") && probe.is_supported(opcode::SendZc::CODE),
        sendmsg_zc: cfg!(feature = "uring-send-zc") && probe.is_supported(opcode::SendMsgZc::CODE),
        recv_zc: false,

        async_cancel: probe.is_supported(opcode::AsyncCancel::CODE),

        direct_io_align: 4096,
    })
}

/// Parse kernel version from `uname -r` output.
fn read_kernel_version() -> KernelVersion {
    parse_utsname_release().unwrap_or(KernelVersion::ZERO)
}

fn parse_utsname_release() -> Option<KernelVersion> {
    // SAFETY: zeroed memory is a valid initial state for libc::utsname
    // (all-zero bytes, no padding invariants). If zeroing somehow failed
    // to produce a valid repr(C) layout, uname(2) would write into
    // misaligned fields -- undefined behavior.
    let mut utsname: libc::utsname = unsafe { std::mem::zeroed() };
    // SAFETY: utsname is stack-local, exclusively borrowed, properly
    // aligned, and large enough for the kernel write. If the pointer
    // were invalid or misaligned, uname(2) would corrupt the stack.
    let result = unsafe { libc::uname(&raw mut utsname) };
    if result != 0 {
        return None;
    }

    let release = utsname.release;
    let cstr = {
        // SAFETY: release is a null-terminated C string written by the
        // kernel into utsname.release. The pointer is valid for the
        // lifetime of utsname (stack-local above). If the kernel wrote
        // a non-null-terminated buffer, CStr::from_ptr would read past
        // the array boundary -- undefined behavior.
        unsafe { std::ffi::CStr::from_ptr(release.as_ptr()) }
    };
    let bytes: &[u8] = cstr.to_bytes();
    let release_str = std::str::from_utf8(bytes).ok()?;

    parse_version_string(release_str)
}

fn parse_version_string(release: &str) -> Option<KernelVersion> {
    let version_part = release.split('-').next()?;
    let mut parts = version_part.split('.');

    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    let patch = parts
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    Some(KernelVersion::new(major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RING_ENTRIES: u32 = 32;

    #[test]
    fn parse_version_string_full() {
        let Some(version) = parse_version_string("6.1.42-generic") else {
            panic!("expected valid version from '6.1.42-generic'");
        };
        assert_eq!(version, KernelVersion::new(6, 1, 42));
    }

    #[test]
    fn parse_version_string_no_patch() {
        let Some(version) = parse_version_string("6.0-rc1") else {
            panic!("expected valid version from '6.0-rc1'");
        };
        assert_eq!(version, KernelVersion::new(6, 0, 0));
    }

    #[test]
    fn parse_version_string_major_minor_only() {
        let Some(version) = parse_version_string("5.19") else {
            panic!("expected valid version from '5.19'");
        };
        assert_eq!(version, KernelVersion::new(5, 19, 0));
    }

    #[test]
    fn parse_version_string_empty_returns_none() {
        assert!(parse_version_string("").is_none());
    }

    #[test]
    fn parse_version_string_garbage_returns_none() {
        assert!(parse_version_string("not-a-version").is_none());
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn probe_and_create_succeeds() {
        let result = match probe_and_create(TEST_RING_ENTRIES) {
            Ok(result) => result,
            Err(error) => panic!("probe_and_create failed: {error}"),
        };
        assert!(result.tier == SetupTier::Optimal || result.tier == SetupTier::Baseline,);
        assert!(result.capabilities.single_issuer);
        assert!(result.capabilities.async_cancel);
    }

    #[cfg(feature = "uring-msg-ring")]
    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn probe_detects_msg_ring() {
        let result = match probe_and_create(TEST_RING_ENTRIES) {
            Ok(result) => result,
            Err(error) => panic!("probe_and_create failed: {error}"),
        };
        assert!(
            result.capabilities.msg_ring,
            "msg_ring required on 6.0+ kernel",
        );
    }

    #[cfg(feature = "uring-multishot")]
    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn probe_detects_multishot() {
        let result = match probe_and_create(TEST_RING_ENTRIES) {
            Ok(result) => result,
            Err(error) => panic!("probe_and_create failed: {error}"),
        };
        assert!(result.capabilities.multishot_accept);
        assert!(result.capabilities.multishot_recv);
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[cfg(not(feature = "uring-send-zc"))]
    #[test]
    fn send_zc_feature_off_keeps_capability_false() {
        let result = match probe_and_create(TEST_RING_ENTRIES) {
            Ok(result) => result,
            Err(error) => panic!("probe_and_create failed: {error}"),
        };
        assert!(!result.capabilities.send_zc);
        assert!(!result.capabilities.sendmsg_zc);
    }

    #[cfg_attr(
        miri,
        ignore = "io_uring_setup(2) is unsupported under miri; real kernel required"
    )]
    #[test]
    fn kernel_version_is_at_least_6_0() {
        let result = match probe_and_create(TEST_RING_ENTRIES) {
            Ok(result) => result,
            Err(error) => panic!("probe_and_create failed: {error}"),
        };
        assert!(
            result.capabilities.kernel_version.at_least(6, 0),
            "kernel must be 6.0+; got {:?}",
            result.capabilities.kernel_version,
        );
    }

    #[cfg_attr(miri, ignore = "uname(2) is unsupported under miri")]
    #[test]
    fn read_kernel_version_succeeds() {
        let version = read_kernel_version();
        assert_ne!(
            version,
            KernelVersion::ZERO,
            "uname(2) should return a valid kernel version",
        );
    }
}
