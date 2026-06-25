#![no_main]

use arbitrary::Arbitrary;
use kwokka_io::{SockAddr, UnixAddr};
use libfuzzer_sys::fuzz_target;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

/// Structured input mapping arbitrary bytes onto a `SockAddr` variant.
#[derive(Arbitrary, Debug)]
enum FuzzAddr {
    V4 {
        octets: [u8; 4],
        port: u16,
    },
    V6 {
        octets: [u8; 16],
        port: u16,
        flowinfo: u32,
        scope_id: u32,
    },
    UnixPath(Vec<u8>),
    UnixAbstract(Vec<u8>),
    UnixUnnamed,
}

fuzz_target!(|input: FuzzAddr| {
    let addr = match input {
        FuzzAddr::V4 { octets, port } => {
            SockAddr::V4(SocketAddrV4::new(Ipv4Addr::from(octets), port))
        }
        FuzzAddr::V6 {
            octets,
            port,
            flowinfo,
            scope_id,
        } => SockAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(octets),
            port,
            flowinfo,
            scope_id,
        )),
        FuzzAddr::UnixPath(bytes) => {
            let Ok(text) = std::str::from_utf8(&bytes) else {
                return;
            };
            let Ok(unix) = UnixAddr::path(text) else {
                return;
            };
            SockAddr::Unix(unix)
        }
        FuzzAddr::UnixAbstract(bytes) => {
            let Ok(unix) = UnixAddr::abstract_(&bytes) else {
                return;
            };
            SockAddr::Unix(unix)
        }
        FuzzAddr::UnixUnnamed => SockAddr::Unix(UnixAddr::Unnamed),
    };

    // Oracle: pack_into must never report more bytes than the 128-byte buffer
    // holds. ASAN (enabled by cargo-fuzz) catches any out-of-bounds write in
    // the pack_v4 / pack_v6 / pack_unix unsafe paths; this guards the reported
    // length and that pack_into does not panic on any address shape.
    let mut buf = [0u8; 128];
    let written = addr.pack_into(&mut buf) as usize;
    if written > buf.len() {
        panic!(
            "pack_into reported {written} bytes for a {}-byte buffer",
            buf.len()
        );
    }
});
