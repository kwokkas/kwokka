//! End-to-end buffered recv through the affine run-loop.
//!
//! Connects a loopback socket pair via std, writes bytes on the client end,
//! then drives the public [`TcpStream::recv`] entry on the adopted server end
//! through the real `io_uring` ring: submit a recv, park, harvest the CQE,
//! wake, and read the bytes back. The socket counterpart of the buffered-read
//! e2e test. The write buffers into the server's receive queue, so the recv
//! harvests it sequentially with no concurrent peer.
//!
//! [`TcpStream::recv`]: kwokka_net::tcp::TcpStream::recv

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

#[test]
fn recv_returns_sent_bytes() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let payload = b"kwokka inline socket recv";

    // Connect completes the handshake via the listen backlog without accept, so
    // the client write lands in the server's receive buffer before the recv runs.
    let Ok(mut client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok((server, _peer)) = listener.accept() else {
        panic!("accepting the loopback connection must succeed");
    };
    let Ok(()) = client.write_all(payload) else {
        panic!("the client write must succeed");
    };

    let Ok(mut runtime) = kwokka_runtime::Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let server = kwokka_net::tcp::TcpStream::from(server);
    let (result, buf) = runtime.block_on(server.recv::<64>());

    // Hold the connection open until the recv has drained its bytes.
    drop(client);

    let Ok(received) = result else {
        panic!("the recv must resolve with a byte count, not an error");
    };
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the inline buffer holds the bytes the peer sent",
    );
}
