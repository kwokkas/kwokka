#![doc(html_logo_url = "https://cdn.kwokka.dev/images/icon-light.png")]
#![doc(html_favicon_url = "https://cdn.kwokka.dev/images/icon-light.png")]
//! TCP and UDP networking for the kwokka runtime.
//!
//! Network endpoints live here; the completion futures that drive them
//! migrate in from the runtime as the crate grows. Construction calls
//! (`bind`, `listen`) are synchronous one-shot syscalls; everything that
//! waits on a peer (`accept`, `connect`, `recv`, `send`) is a future.
//!
//! [`tcp::TcpListener`] binds a stream endpoint whose raw fd feeds the accept
//! op; [`udp::UdpSocket`] binds a datagram endpoint driving `sendmsg` /
//! `recvmsg`.

pub mod tcp;

#[cfg(unix)]
pub mod udp;
