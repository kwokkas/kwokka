//! End-to-end accept resolving to a connected, conversing stream.
//!
//! Connects a std client to a kwokka listener, then drives
//! `TcpListener::accept` through the real `io_uring` ring: the resolved
//! [`TcpStream`] must report the client as its peer and the listener
//! address as its local end, proving the accepted descriptor was adopted
//! into an owned, inspectable endpoint. The stream then converses both
//! ways through its own recv and send futures.
//!
//! [`TcpStream`]: kwokka_net::tcp::TcpStream

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::{Read, Write},
    net::TcpStream as StdTcpStream,
};

use kwokka_net::tcp::TcpListener;
use kwokka_runtime::Runtime;

#[test]
fn accept_resolves_a_connected_stream() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };

    // Connect completes the handshake via the listen backlog, so the
    // accept resolves sequentially with no concurrent peer.
    let Ok(mut client) = StdTcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok(client_addr) = client.local_addr() else {
        panic!("the client must report its local address");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let Ok(stream) = runtime.block_on(listener.accept()) else {
        panic!("the accept must resolve to a connected stream");
    };

    let Ok(peer) = stream.peer_addr() else {
        panic!("the accepted stream must report its peer address");
    };
    assert_eq!(
        peer, client_addr,
        "the accepted stream's peer is the client"
    );
    let Ok(local) = stream.local_addr() else {
        panic!("the accepted stream must report its local address");
    };
    assert_eq!(
        local, addr,
        "the accepted stream is bound at the listener address",
    );

    // The client speaks first; the stream's recv future drains it.
    let payload = b"kwokka stream converses";
    let Ok(()) = client.write_all(payload) else {
        panic!("the client write must succeed");
    };
    let (received, buf) = runtime.block_on(stream.recv::<64>());
    let Ok(received) = usize::try_from(received) else {
        panic!("a successful recv result fits usize, got {received}");
    };
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the stream holds the bytes the client sent",
    );

    // The stream replies through its send future; the client reads it back.
    let mut data = [0u8; 64];
    data[..payload.len()].copy_from_slice(payload);
    let sent = runtime.block_on(stream.send::<64>(data, payload.len()));
    let Ok(sent) = usize::try_from(sent) else {
        panic!("a successful send result fits usize, got {sent}");
    };
    assert_eq!(sent, payload.len(), "the kernel sent every requested byte");
    let mut echoed = [0u8; 64];
    let Ok(()) = client.read_exact(&mut echoed[..sent]) else {
        panic!("the client read must succeed");
    };
    assert_eq!(
        &echoed[..sent],
        &payload[..],
        "the client holds the bytes the stream sent",
    );
}
