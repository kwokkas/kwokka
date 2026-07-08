//! End-to-end Unix-domain stream roundtrip over the public entries.
//!
//! Binds a loopback [`UnixListener`] at a temp path on the real `io_uring`
//! ring, connects a client into the backlog synchronously, accepts it into a
//! server stream through [`UnixListener::accept`], then sends a payload from
//! the client and receives it on the accepted stream. Proves the reused accept
//! / send / recv ops drive over the Unix fd. The send precedes the recv in one
//! task: the kernel buffers the loopback bytes, so the recv finds them already
//! delivered.
//!
//! [`UnixListener`]: kwokka_net::unix::UnixListener
//! [`UnixListener::accept`]: kwokka_net::unix::UnixListener::accept

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::path::PathBuf;

use kwokka_net::unix::{UnixListener, UnixStream};
use kwokka_runtime::Runtime;

// Unlinks the socket path on drop so the test leaves no filesystem residue; the
// guard owns the path so the test borrows it without cloning.
struct PathGuard(PathBuf);

impl Drop for PathGuard {
    fn drop(&mut self) {
        // IGNORE: best-effort cleanup; the path may already be gone.
        let _ = std::fs::remove_file(&self.0);
    }
}

// A per-process socket path under the temp dir, cleared of any socket a crashed
// prior run left behind so the bind starts clean.
fn unique_path() -> PathGuard {
    let mut path = std::env::temp_dir();
    path.push(format!("kwokka-unix-roundtrip-{}.sock", std::process::id()));
    // IGNORE: best-effort clear of a stale socket; absence is the goal.
    let _ = std::fs::remove_file(&path);
    PathGuard(path)
}

#[test]
fn accept_send_recv_roundtrips_over_public_entries() {
    let guard = unique_path();
    let path = guard.0.as_path();

    let Ok(listener) = UnixListener::bind(path) else {
        panic!("binding a loopback listener must succeed");
    };
    // Connect completes into the listen backlog, so the accept, send, and recv
    // below all resolve sequentially with no concurrent peer.
    let Ok(client) = UnixStream::connect(path) else {
        panic!("connecting to the loopback listener must succeed");
    };

    let payload = *b"kwokka unix roundtrip";

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (accepted, sent, received, buf) = runtime.block_on(async move {
        let accepted = listener.accept().await;
        let sent = client.send_buf(payload).await;
        let (received, buf) = match &accepted {
            Ok(server) => server.recv_buf([0u8; 64]).await,
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
