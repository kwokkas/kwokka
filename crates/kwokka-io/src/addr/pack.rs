//! Raw `sockaddr_storage` packing -- unix-only, used by SQE submission.

#[cfg(unix)]
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

#[cfg(unix)]
use crate::addr::unix::UnixAddr;

#[cfg(unix)]
#[allow(
    clippy::cast_possible_truncation,
    reason = "AF_* constants are small positive integers guaranteed to fit in u16 by POSIX sa_family_t"
)]
mod family {
    pub(super) const AF_INET: u16 = libc::AF_INET as u16;
    pub(super) const AF_INET6: u16 = libc::AF_INET6 as u16;
    pub(super) const AF_UNIX: u16 = libc::AF_UNIX as u16;
}
#[cfg(unix)]
use family::{AF_INET, AF_INET6, AF_UNIX};

/// Packs an IPv4 address into a `sockaddr_storage`-compatible buffer.
///
/// Writes a `sockaddr_in` at `out[0..16]` and returns 16 (the `addr_len` for the SQE).
#[cfg(unix)]
pub(super) fn pack_v4(addr: SocketAddrV4, out: &mut [u8; 128]) -> u32 {
    out.fill(0);
    out[0..2].copy_from_slice(&AF_INET.to_ne_bytes());
    out[2..4].copy_from_slice(&addr.port().to_be_bytes());
    out[4..8].copy_from_slice(&addr.ip().octets());
    16
}

/// Packs an IPv6 address into a `sockaddr_storage`-compatible buffer.
///
/// Writes a `sockaddr_in6` at `out[0..28]` and returns 28.
#[cfg(unix)]
pub(super) fn pack_v6(addr: SocketAddrV6, out: &mut [u8; 128]) -> u32 {
    out.fill(0);
    out[0..2].copy_from_slice(&AF_INET6.to_ne_bytes());
    out[2..4].copy_from_slice(&addr.port().to_be_bytes());
    out[4..8].copy_from_slice(&addr.flowinfo().to_be_bytes());
    out[8..24].copy_from_slice(&addr.ip().octets());
    out[24..28].copy_from_slice(&addr.scope_id().to_ne_bytes());
    28
}

/// Reads the `sa_family` discriminant a `pack_*` call wrote at `buf[0..2]`.
#[cfg(unix)]
pub(super) const fn packed_family(buf: &[u8; 128]) -> u16 {
    u16::from_ne_bytes([buf[0], buf[1]])
}

/// Whether `family` is the `AF_INET` (IPv4) discriminant.
#[cfg(unix)]
pub(super) const fn is_inet(family: u16) -> bool {
    family == AF_INET
}

/// Whether `family` is the `AF_INET6` (IPv6) discriminant.
#[cfg(unix)]
pub(super) const fn is_inet6(family: u16) -> bool {
    family == AF_INET6
}

/// Reconstructs an IPv4 address from a [`pack_v4`]-packed buffer.
#[cfg(unix)]
pub(super) fn unpack_v4(buf: &[u8; 128]) -> SocketAddrV4 {
    let port = u16::from_be_bytes([buf[2], buf[3]]);
    let octets = [buf[4], buf[5], buf[6], buf[7]];
    SocketAddrV4::new(Ipv4Addr::from(octets), port)
}

/// Reconstructs an IPv6 address from a [`pack_v6`]-packed buffer.
#[cfg(unix)]
pub(super) fn unpack_v6(buf: &[u8; 128]) -> SocketAddrV6 {
    let port = u16::from_be_bytes([buf[2], buf[3]]);
    let flowinfo = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let mut octets = [0u8; 16];
    octets.copy_from_slice(&buf[8..24]);
    let scope_id = u32::from_ne_bytes([buf[24], buf[25], buf[26], buf[27]]);
    SocketAddrV6::new(Ipv6Addr::from(octets), port, flowinfo, scope_id)
}

/// Packs a Unix domain socket address into a `sockaddr_storage`-compatible buffer.
///
/// Returns the number of bytes written as `addr_len` for the SQE.
///
/// | Variant  | Layout                               | `addr_len`     |
/// |----------|--------------------------------------|----------------|
/// | Path     | `sun_family` + path bytes + `\0`     | `2 + len + 1`  |
/// | Abstract | `sun_family` + `\0` + name bytes     | `2 + 1 + len`  |
/// | Unnamed  | `sun_family` only                    | `2`            |
#[cfg(unix)]
#[allow(
    clippy::cast_possible_truncation,
    reason = "path/name lengths validated <= 107 in constructor, so addr_len <= 110 fits in u32"
)]
pub(super) fn pack_unix(addr: &UnixAddr, out: &mut [u8; 128]) -> u32 {
    out.fill(0);
    out[0..2].copy_from_slice(&AF_UNIX.to_ne_bytes());
    match addr {
        UnixAddr::Path { buf, len } => {
            let path_len = *len as usize;
            out[2..2 + path_len].copy_from_slice(&buf[..path_len]);
            (2 + path_len + 1) as u32
        }
        #[cfg(target_os = "linux")]
        UnixAddr::Abstract { buf, len } => {
            let name_len = *len as usize;
            out[3..3 + name_len].copy_from_slice(&buf[..name_len]);
            (2 + 1 + name_len) as u32
        }
        UnixAddr::Unnamed => 2,
    }
}
