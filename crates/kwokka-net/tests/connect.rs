//! End-to-end connect through the affine run-loop.
//!
//! Binds two std UDP sockets, then drives [`ConnectFuture`] on the client fd
//! through the real `io_uring` ring: submit a connect op toward the peer,
//! park, harvest the CQE, wake, and read the result back. UDP gives std an
//! unconnected socket fd with no extra dependency, and connect on a datagram
//! socket records the default peer, which the follow-up std `send` proves: a
//! send with no destination succeeds only on a connected socket.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    net::{SocketAddr, UdpSocket},
    os::fd::AsRawFd,
};

use kwokka_io::addr::SockAddr;
use kwokka_net::tcp::ConnectFuture;
use kwokka_runtime::Runtime;

#[test]
fn connect_records_the_peer() {
    let Ok(peer) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the peer socket must succeed");
    };
    let Ok(client) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the client socket must succeed");
    };
    let Ok(SocketAddr::V4(peer_v4)) = peer.local_addr() else {
        panic!("a loopback bind must report a V4 local address");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let result = runtime.block_on(ConnectFuture::new(
        client.as_raw_fd(),
        SockAddr::V4(peer_v4),
    ));
    assert_eq!(result, 0, "the connect completed with an error: {result}");

    // A destination-less send succeeds only on a connected socket, proving the
    // kernel recorded the peer the future submitted.
    let payload = b"kwokka connect probe";
    let Ok(sent) = client.send(payload) else {
        panic!("a send on the connected socket must succeed");
    };
    assert_eq!(sent, payload.len(), "the send delivered every byte");

    let mut buf = [0u8; 32];
    let Ok(received) = peer.recv(&mut buf) else {
        panic!("the peer recv must succeed");
    };
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the peer holds the bytes sent over the connected socket",
    );
}
