//! End-to-end accept through the affine run-loop.
//!
//! Connects a std client to a loopback listener, then drives [`AcceptFuture`]
//! on the listener fd through the real `io_uring` ring: submit an accept op,
//! park, harvest the CQE, wake, and read the accepted descriptor back. The
//! first no-buffer op over the production submit path. The connect completes
//! via the listen backlog, so the accept harvests it sequentially with no
//! concurrent peer.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
};

use kwokka_net::tcp::AcceptFuture;
use kwokka_runtime::Runtime;

#[test]
fn accept_returns_connection_fd() {
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
    let result = runtime.block_on(AcceptFuture::new(listener.as_raw_fd()));

    // Hold the connection open until the accept has resolved.
    drop(client);

    // AcceptFuture resolves to the accepted descriptor as a raw i32; a
    // non-negative value is a live fd, a negative one is -errno.
    assert!(result >= 0, "the accept completed with an error: {result}");
    assert_ne!(
        result,
        listener.as_raw_fd(),
        "the accepted descriptor is a new fd, not the listener"
    );
}
