//! End-to-end socket roundtrip on the runtime's own futures.
//!
//! Drives [`AcceptFuture`], [`SendFuture`], and [`RecvFuture`] sequentially
//! inside one task on the real `io_uring` ring: accept the backlog connection,
//! send a payload from the client fd, then receive it back on the accepted fd.
//! The first test composing several I/O ops in a single task, so it proves the
//! per-op wake-data handoff and the submit-once gating across consecutive
//! awaits. The connect op stays out: std cannot produce an unconnected TCP fd,
//! so [`ConnectFuture`] is covered by its own datagram-socket test.
//!
//! [`ConnectFuture`]: kwokka_net::tcp::ConnectFuture

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
};

use kwokka_net::tcp::{AcceptFuture, RecvFuture, SendFuture};
use kwokka_runtime::Runtime;

#[test]
fn roundtrip_delivers_payload_over_runtime_futures() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };

    // Connect completes the handshake via the listen backlog, so the accept,
    // send, and recv below all resolve sequentially with no concurrent peer.
    let Ok(client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };

    let payload = b"kwokka runtime roundtrip";
    let mut data = [0u8; 64];
    data[..payload.len()].copy_from_slice(payload);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let listener_fd = listener.as_raw_fd();
    let client_fd = client.as_raw_fd();
    let (accepted, sent, received, buf) = runtime.block_on(async move {
        let accepted = AcceptFuture::new(listener_fd).await;
        let sent = SendFuture::<64>::new(client_fd, data, payload.len()).await;
        let (received, buf) = RecvFuture::<64>::new(accepted).await;
        (accepted, sent, received, buf)
    });

    assert!(
        accepted >= 0,
        "the accept completed with an error: {accepted}"
    );
    let Ok(sent) = sent else {
        panic!("the send must resolve with a byte count");
    };
    assert_eq!(sent, payload.len(), "the kernel sent every requested byte");
    let Ok(received) = received else {
        panic!("the recv must resolve with a byte count");
    };
    assert_eq!(received, payload.len(), "the recv drained the full payload");
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the accepted side holds the bytes the client sent",
    );
}
