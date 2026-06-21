//! End-to-end recv EOF through the affine run-loop.
//!
//! Drops the peer before the recv so the op completes with result 0, which
//! must read as an arrived completion rather than pending wake data. Lives in
//! its own binary on purpose: every affine runtime claims worker 0, so two
//! runtimes in one process race on the per-worker statics when the test
//! harness runs them on parallel threads. One runtime test per binary keeps
//! each runtime in its own process.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
};

#[test]
fn recv_resolves_zero_when_peer_closes() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let Ok(client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok((server, _peer)) = listener.accept() else {
        panic!("accepting the loopback connection must succeed");
    };

    // A closed peer makes the recv complete with result 0 (EOF). The zero
    // result must read as an arrived completion, not as pending wake data --
    // the regression this test pins down.
    drop(client);

    let Ok(mut runtime) = kwokka_runtime::Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (result, _buf) =
        runtime.block_on(kwokka_net::tcp::RecvFuture::<64>::new(server.as_raw_fd()));

    assert_eq!(
        result, 0,
        "a recv on a closed peer resolves with 0, not {result}"
    );
}
