//! Client connect over the public entry, on the real `io_uring` ring.
//!
//! Drives [`TcpStream::connect`] against a loopback [`TcpListener`]: the connect
//! op completes the handshake into the listen backlog, so it resolves in its own
//! `block_on` before the listener accepts the queued connection. Proves the
//! client-side entry the connect future was built for, plus the deadline-armed
//! and refused-peer paths. Each op runs in a separate `block_on` so no single
//! task-slot future holds both the connect and accept state machines at once.
//!
//! [`TcpStream::connect`]: kwokka_net::tcp::TcpStream::connect
//! [`TcpListener`]: kwokka_net::tcp::TcpListener

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use core::time::Duration;

use kwokka_net::tcp::{TcpListener, TcpStream};
use kwokka_runtime::Runtime;

#[test]
fn connect_reaches_a_loopback_listener() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    // The connect completes the handshake into the backlog, so it resolves on
    // its own; the accept then dequeues the established connection.
    let Ok(client) = runtime.block_on(TcpStream::connect(addr)) else {
        panic!("the connect must resolve with a connected stream");
    };
    let Ok(_server) = runtime.block_on(listener.accept()) else {
        panic!("the listener must accept the connected client");
    };
    let Ok(peer) = client.peer_addr() else {
        panic!("the connected client must report its peer address");
    };
    assert_eq!(
        peer, addr,
        "the client is connected to the listener address"
    );
}

#[test]
fn connect_timeout_reaches_listener() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    // Five seconds dwarfs a loopback connect, so the connect wins the link and
    // the deadline never fires.
    let Ok(_client) = runtime.block_on(TcpStream::connect_timeout(&addr, Duration::from_secs(5)))
    else {
        panic!("the deadline-armed connect must resolve with a connected stream");
    };
    let Ok(_server) = runtime.block_on(listener.accept()) else {
        panic!("the listener must accept the deadline-armed client");
    };
}

#[test]
fn connect_to_closed_port_errors() {
    // Bind then drop a listener to free a loopback port that now refuses.
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    drop(listener);
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let result = runtime.block_on(TcpStream::connect(addr));
    assert!(
        result.is_err(),
        "connecting to a closed port must surface an error, not hang",
    );
}
