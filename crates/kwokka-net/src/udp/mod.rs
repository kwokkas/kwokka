//! UDP primitives -- the bound datagram socket.
//!
//! The datagram futures stay unnamed: [`send_to_buf`](UdpSocket::send_to_buf),
//! [`recv_from_buf`](UdpSocket::recv_from_buf), and their connected siblings
//! return opaque futures the caller only awaits.

mod socket;

pub use socket::UdpSocket;
