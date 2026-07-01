//! End-to-end multishot accept draining a stream of backlog connections.
//!
//! Connects two std clients to a kwokka listener, then drives
//! [`TcpListener::accept_multi`] through the real `io_uring` ring: one submitted
//! multishot accept SQE posts a completion per backlog entry, so the stream
//! yields both connections from a single submission. The first test of the
//! one-SQE-many-CQE path -- the accept future submits once per connection, the
//! stream submits once for all of them. Each yielded item must be an owned,
//! inspectable [`TcpStream`], the two peer addresses must match the two clients,
//! and the accepted descriptors must be distinct live fds. Dropping the stream
//! at the end of the task cancels the still-armed op.
//!
//! [`TcpListener::accept_multi`]: kwokka_net::tcp::TcpListener::accept_multi
//! [`TcpStream`]: kwokka_net::tcp::TcpStream

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{io::Write, net::TcpStream as StdTcpStream, os::fd::AsRawFd};

use kwokka_net::tcp::TcpListener;
use kwokka_runtime::Runtime;

#[test]
fn accept_multi_streams_backlog_connections() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };

    // Two clients land in the listen backlog before the accept is submitted, so
    // the multishot op posts a completion per backlog entry and the stream
    // yields both sequentially with no concurrent peer.
    let Ok(mut first_client) = StdTcpStream::connect(addr) else {
        panic!("connecting the first client must succeed");
    };
    let Ok(second_client) = StdTcpStream::connect(addr) else {
        panic!("connecting the second client must succeed");
    };
    let Ok(first_client_addr) = first_client.local_addr() else {
        panic!("the first client must report its local address");
    };
    let Ok(second_client_addr) = second_client.local_addr() else {
        panic!("the second client must report its local address");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };

    let (first, second) = runtime.block_on(async {
        let mut stream = listener.accept_multi();
        let Some(first) = stream.next().await else {
            panic!("the stream must yield the first backlog connection");
        };
        let Some(second) = stream.next().await else {
            panic!("the stream must yield the second backlog connection");
        };
        // The stream drops here, cancelling the still-armed multishot op.
        (first, second)
    });

    let Ok(first) = first else {
        panic!("the first accepted item must be a connection");
    };
    let Ok(second) = second else {
        panic!("the second accepted item must be a connection");
    };

    // The two backlog entries are the two clients; their peer addresses cover
    // both client addresses regardless of the order the kernel drained them.
    let Ok(first_peer) = first.peer_addr() else {
        panic!("the first accepted stream must report its peer address");
    };
    let Ok(second_peer) = second.peer_addr() else {
        panic!("the second accepted stream must report its peer address");
    };
    let peers = [first_peer, second_peer];
    assert!(
        peers.contains(&first_client_addr),
        "an accepted connection carries the first client's address",
    );
    assert!(
        peers.contains(&second_client_addr),
        "an accepted connection carries the second client's address",
    );

    let Ok(first_local) = first.local_addr() else {
        panic!("the first accepted stream must report its local address");
    };
    assert_eq!(
        first_local, addr,
        "each accepted connection is bound at the listener address",
    );
    assert_ne!(
        first.as_raw_fd(),
        second.as_raw_fd(),
        "each accepted connection is a distinct descriptor",
    );

    // One accepted fd converses, proving the stream hands out live endpoints and
    // not merely descriptor numbers. Match the client to the accepted stream by
    // peer address so the write reaches the fd the recv drains.
    let payload = b"kwokka multishot converses";
    let Ok(()) = first_client.write_all(payload) else {
        panic!("the client write must succeed");
    };
    let conversing = if first_peer == first_client_addr {
        first
    } else {
        second
    };
    let (received, buf) = runtime.block_on(conversing.recv::<64>());
    let Ok(received) = received else {
        panic!("the stream recv must resolve with a byte count");
    };
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the accepted stream holds the bytes the client sent",
    );

    drop(second_client);
}
