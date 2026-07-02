//! End-to-end socket roundtrip over the public entries.
//!
//! Composes accept, send, and recv sequentially inside one task on the real
//! `io_uring` ring: accept the backlog connection through
//! [`TcpListener::accept`], send a payload from the adopted client end, then
//! receive it back on the accepted stream. The first test composing several
//! I/O ops in a single task, so it proves the per-op wake-data handoff and
//! the submit-once gating across consecutive awaits. The connect op stays
//! out: std cannot produce an unconnected TCP fd, so the connect future is
//! covered by its own in-crate datagram-socket test.
//!
//! [`TcpListener::accept`]: kwokka_net::tcp::TcpListener::accept

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::net::TcpStream;

use kwokka_net::tcp::TcpListener;
use kwokka_runtime::Runtime;

#[test]
fn roundtrip_delivers_payload_over_public_entries() {
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
    let client = kwokka_net::tcp::TcpStream::from(client);

    let payload = b"kwokka runtime roundtrip";
    let mut data = [0u8; 64];
    data[..payload.len()].copy_from_slice(payload);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (accepted, sent, received, buf) = runtime.block_on(async move {
        let accepted = listener.accept().await;
        let sent = client.send::<64>(data, payload.len()).await;
        let (received, buf) = match &accepted {
            Ok(conn) => conn.recv::<64>().await,
            Err(_) => (Ok(0), [0u8; 64]),
        };
        (accepted.map(drop), sent, received, buf)
    });

    let Ok(()) = accepted else {
        panic!("the accept must resolve with a connection, not an error");
    };
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
