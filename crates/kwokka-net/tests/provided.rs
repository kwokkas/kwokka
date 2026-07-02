//! End-to-end provided-buffer recv through the public net entry.
//!
//! Connects a loopback socket pair via std, writes bytes on the client end,
//! then drives the public [`TcpStream::recv_provided`] entry on the adopted
//! server end through the real `io_uring` ring: submit with `BUFFER_SELECT`,
//! park, drain the CQE naming the kernel-selected buffer, and read the pool's
//! bytes back through the borrowed zero-copy view -- no userspace copy. Hosts
//! without a registered provided-buffer group resolve `Unsupported`, the
//! caller's fall-back-to-[`recv`] signal, so the test returns early (fallback
//! parity).
//!
//! [`TcpStream::recv_provided`]: kwokka_net::tcp::TcpStream::recv_provided
//! [`recv`]: kwokka_net::tcp::TcpStream::recv

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::{ErrorKind, Write},
    net::{TcpListener, TcpStream},
};

use kwokka_runtime::Runtime;

#[test]
fn recv_provided_returns_sent_bytes() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let payload = b"kwokka provided recv entry";

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

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let server = kwokka_net::tcp::TcpStream::from(server);
    // The zero-copy view borrows the worker pool for the run, so copy its bytes
    // out for the post-run assertion and drop the view inside the run itself.
    let outcome = runtime.block_on(async move {
        server.recv_provided().await.map(|view| {
            let mut copy = [0u8; 64];
            let count = view.len().min(copy.len());
            copy[..count].copy_from_slice(&view[..count]);
            (count, copy)
        })
    });

    // Hold the connection open until the recv has drained its bytes.
    drop(client);

    match outcome {
        Ok((count, copy)) => assert_eq!(
            &copy[..count],
            &payload[..],
            "the borrowed view holds the bytes the peer sent",
        ),
        // No buf_ring on this host or kernel: the fall-back-to-recv signal,
        // not a bug -- the inline recv path stays available (fallback parity).
        Err(error) if error.kind() == ErrorKind::Unsupported => {}
        Err(error) => panic!("the provided recv must resolve: {error}"),
    }
}
