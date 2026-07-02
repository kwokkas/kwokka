//! End-to-end accept through the affine run-loop.
//!
//! Connects a std client to a loopback listener, then drives the public
//! [`TcpListener::accept`] entry through the real `io_uring` ring: submit an
//! accept op, park, harvest the CQE, wake, and adopt the connection. The
//! first no-buffer op over the production submit path. The connect completes
//! via the listen backlog, so the accept harvests it sequentially with no
//! concurrent peer.
//!
//! [`TcpListener::accept`]: kwokka_net::tcp::TcpListener::accept

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::net::TcpStream;

use kwokka_net::tcp::TcpListener;
use kwokka_runtime::Runtime;

#[test]
fn accept_returns_a_connected_stream() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };

    // Connect completes the handshake via the listen backlog, so the accept op
    // resolves immediately with no concurrent peer.
    let Ok(client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let accepted = runtime.block_on(listener.accept());

    let Ok(conn) = accepted else {
        panic!("the accept must resolve with a connection, not an error");
    };
    let Ok(peer) = conn.peer_addr() else {
        panic!("the accepted stream must report its peer address");
    };
    let Ok(client_local) = client.local_addr() else {
        panic!("the client must report its local address");
    };
    assert_eq!(
        peer, client_local,
        "the accepted stream converses with the connecting client",
    );
}
