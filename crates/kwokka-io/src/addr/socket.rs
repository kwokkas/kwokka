//! Socket address type covering IPv4, IPv6, and Unix domain sockets.

use std::net::{SocketAddrV4, SocketAddrV6};

use crate::addr::unix::UnixAddr;

/// Socket address for `connect`, `sendmsg`, and `recvmsg` operations.
///
/// Wraps IPv4, IPv6, and Unix domain addresses in a single type.
/// Use [`pack_into`][SockAddr::pack_into] to obtain the raw `sockaddr_storage`
/// bytes and length required by `io_uring` SQE `addr` / `addr_len` fields.
///
/// # Lifetime model
///
/// The caller holds a `[u8; 128]` buffer on the stack (or in an arena slot) and
/// passes `buf.as_ptr()` to the SQE. The buffer must remain valid until the CQE
/// arrives. `pack_into` writes the packed bytes inline with no heap allocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SockAddr {
    /// IPv4 socket address.
    V4(SocketAddrV4),
    /// IPv6 socket address.
    V6(SocketAddrV6),
    /// Unix domain socket address (filesystem path, abstract, or unnamed).
    Unix(UnixAddr),
}

impl SockAddr {
    /// Packs the address into a POSIX `sockaddr_storage`-compatible buffer.
    ///
    /// Returns the number of valid bytes written, which must be passed as
    /// `addr_len` to the `io_uring` SQE. The buffer is zeroed before writing.
    ///
    /// | Variant | Bytes written | Standard           |
    /// |---------|---------------|--------------------|
    /// | V4      | 16            | `sockaddr_in`      |
    /// | V6      | 28            | `sockaddr_in6`     |
    /// | Unix    | 2 + path + 1  | `sockaddr_un`      |
    #[cfg(unix)]
    pub fn pack_into(&self, buf: &mut [u8; 128]) -> u32 {
        match self {
            Self::V4(addr) => crate::addr::pack::pack_v4(*addr, buf),
            Self::V6(addr) => crate::addr::pack::pack_v6(*addr, buf),
            Self::Unix(addr) => crate::addr::pack::pack_unix(addr, buf),
        }
    }

    /// Address family discriminant for this address.
    pub const fn family(&self) -> AddressFamily {
        match self {
            Self::V4(_) => AddressFamily::Inet,
            Self::V6(_) => AddressFamily::Inet6,
            Self::Unix(_) => AddressFamily::Unix,
        }
    }
}

/// Address family discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AddressFamily {
    /// `AF_INET` - IPv4.
    Inet,
    /// `AF_INET6` - IPv6.
    Inet6,
    /// `AF_UNIX` - Unix domain socket.
    Unix,
}

#[cfg(all(test, unix))]
#[allow(
    clippy::cast_possible_truncation,
    reason = "AF_* constants and test path lengths are small, truncation impossible in tests"
)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use super::*;
    use crate::addr::unix::UnixAddr;

    fn packed(addr: &SockAddr) -> ([u8; 128], u32) {
        let mut buf = [0u8; 128];
        let len = addr.pack_into(&mut buf);
        (buf, len)
    }

    #[test]
    fn v4_pack_family_is_af_inet() {
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let (buf, len) = packed(&addr);
        assert_eq!(len, 16);
        let family = u16::from_ne_bytes([buf[0], buf[1]]);
        assert_eq!(family, libc::AF_INET as u16);
    }

    #[test]
    fn v4_pack_port_is_big_endian() {
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0x1F90));
        let (buf, _) = packed(&addr);
        assert_eq!([buf[2], buf[3]], 0x1F90u16.to_be_bytes());
    }

    #[test]
    fn v4_pack_addr_octets() {
        let addr = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 80));
        let (buf, _) = packed(&addr);
        assert_eq!(&buf[4..8], &[192, 168, 1, 1]);
    }

    #[test]
    fn v6_pack_family_is_af_inet6() {
        let addr = SockAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 0, 0));
        let (buf, len) = packed(&addr);
        assert_eq!(len, 28);
        let family = u16::from_ne_bytes([buf[0], buf[1]]);
        assert_eq!(family, libc::AF_INET6 as u16);
    }

    #[test]
    fn v6_pack_port_is_big_endian() {
        let addr = SockAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0x01BB, 0, 0));
        let (buf, _) = packed(&addr);
        assert_eq!([buf[2], buf[3]], 0x01BBu16.to_be_bytes());
    }

    #[test]
    fn unix_path_pack_family_and_path() {
        let Ok(unix) = UnixAddr::path("/tmp/kwokka.sock") else {
            panic!("expected Ok");
        };
        let addr = SockAddr::Unix(unix);
        let (buf, len) = packed(&addr);
        let path_bytes = b"/tmp/kwokka.sock";
        assert_eq!(len, (2 + path_bytes.len() + 1) as u32);
        assert_eq!(&buf[2..2 + path_bytes.len()], path_bytes);
        assert_eq!(buf[2 + path_bytes.len()], 0);
    }

    #[test]
    fn unix_unnamed_pack_len_is_two() {
        let addr = SockAddr::Unix(UnixAddr::Unnamed);
        let (_, len) = packed(&addr);
        assert_eq!(len, 2);
    }

    #[test]
    fn family_method_returns_correct_variant() {
        let v4 = SockAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80));
        let v6 = SockAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 80, 0, 0));
        assert_eq!(v4.family(), AddressFamily::Inet);
        assert_eq!(v6.family(), AddressFamily::Inet6);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_abstract_pack_first_byte_zero() {
        let Ok(unix) = UnixAddr::abstract_(b"kwokka") else {
            panic!("expected Ok");
        };
        let addr = SockAddr::Unix(unix);
        let (buf, len) = packed(&addr);
        assert_eq!(len, (2 + 1 + 6) as u32);
        assert_eq!(buf[2], 0);
        assert_eq!(&buf[3..9], b"kwokka");
    }
}
