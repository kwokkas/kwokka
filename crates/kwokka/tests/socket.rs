//! Socket round trip through the facade: bind, accept, recv -- all via
//! `kwokka::net` over the real completion backend.
//!
//! A std loopback connect fills the listen backlog and writes the payload, so
//! the single-threaded `block_on` accepts and harvests the bytes sequentially
//! with no peer thread. Complements `facade.rs`, which binds but never accepts
//! a live connection. One affine runtime per binary keeps worker 0 uncontended.

#![cfg(all(target_os = "linux", feature = "net"))]
#![cfg(not(any(miri, loom)))]

use std::{io::Write, net::TcpStream};

use kwokka::{net::TcpListener, runtime::Runtime};

#[test]
fn the_facade_accepts_and_recvs_a_loopback_payload() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };

    let payload = b"kwokka facade socket round trip";

    // Connect completes the handshake via the listen backlog, so the client
    // write lands in the server receive queue before the recv below runs.
    let Ok(mut client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok(()) = client.write_all(payload) else {
        panic!("the client write must succeed");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };

    let outcome = runtime.block_on(async {
        let stream = listener.accept().await?;
        let (result, buf) = stream.recv::<64>().await;
        let received = result?;
        Ok::<_, std::io::Error>((received, buf))
    });

    // Hold the connection open until the recv has drained its bytes.
    drop(client);

    let Ok((received, buf)) = outcome else {
        panic!("the facade accept and recv must resolve without error");
    };
    assert_eq!(
        received,
        payload.len(),
        "the recv drained the full payload the client sent",
    );
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the facade reads back the bytes the peer sent",
    );
}
