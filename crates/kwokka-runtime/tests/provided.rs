//! End-to-end provided-buffer recv through the affine run-loop.
//!
//! Connects a loopback socket pair via std, writes bytes on the client end,
//! then drives `ProvidedRecvFuture` on the server fd through the real
//! `io_uring` ring: submit with `BUFFER_SELECT`, park, drain the CQE carrying
//! the kernel-selected buffer id, and read the pool's bytes back through the
//! borrowed view. Hosts without a registered provided-buffer group resolve
//! `Unsupported` and each test returns early (fallback parity).

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::{ErrorKind, Write},
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::Poll,
};

use kwokka_io::operation::ProvidedRecvFuture;
use kwokka_runtime::Runtime;

/// Connects a loopback pair: the client end and the accepted server end.
fn loopback_pair() -> (TcpStream, TcpStream) {
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
    (client, server)
}

#[test]
fn provided_recv_returns_sent_bytes() {
    let (mut client, server) = loopback_pair();
    let payload = b"kwokka provided recv";
    let Ok(()) = client.write_all(payload) else {
        panic!("the client write must succeed");
    };
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let fd = server.as_raw_fd();
    let outcome = runtime.block_on(async move {
        // The view borrows the worker pool's bytes, so it is consumed --
        // copied out for the assertion -- and dropped inside the run.
        ProvidedRecvFuture::new(fd).await.map(|view| {
            let mut copy = [0u8; 64];
            let count = view.len().min(copy.len());
            copy[..count].copy_from_slice(&view[..count]);
            (count, copy)
        })
    });
    drop(client);
    match outcome {
        Ok((count, copy)) => {
            assert_eq!(
                &copy[..count],
                payload,
                "the borrowed view holds the bytes the peer sent",
            );
        }
        // No buf_ring on this host or kernel: the fallback signal, not a bug.
        Err(error) if error.kind() == ErrorKind::Unsupported => {}
        Err(error) => panic!("the provided recv must resolve: {error}"),
    }
}

/// One recv round: writes `payload`, receives it into a provided buffer on a
/// fresh root task, and returns the pool address the view read from.
fn recv_round(
    client: &mut TcpStream,
    runtime: &mut Runtime<kwokka_runtime::task::Affine>,
    fd: i32,
) -> usize {
    let Ok(()) = client.write_all(b"spin") else {
        panic!("the client write must succeed");
    };
    let outcome = runtime.block_on(async move {
        ProvidedRecvFuture::new(fd)
            .await
            .map(|view| (view.len(), view.as_slice().as_ptr() as usize))
    });
    let Ok((count, address)) = outcome else {
        panic!("a rotation round must receive the bytes it wrote");
    };
    assert!(count > 0, "each round receives at least its own write");
    address
}

#[test]
fn dropped_recv_keeps_the_pool_rotating() {
    let (mut client, server) = loopback_pair();
    let fd = server.as_raw_fd();
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    // Probe support first so an epoll/no-buf_ring host skips cleanly.
    let Ok(()) = client.write_all(b"ping") else {
        panic!("the client write must succeed");
    };
    let probe =
        runtime.block_on(async move { ProvidedRecvFuture::new(fd).await.map(|view| view.len()) });
    match probe {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::Unsupported => return,
        Err(error) => panic!("the probe recv must resolve: {error}"),
    }
    // Drop-race: submit a recv whose data is already waiting, then abandon
    // the future mid-flight. Its completion carries a real buffer id that the
    // drain must recycle against the (now settled) root's token.
    let Ok(()) = client.write_all(b"raced") else {
        panic!("the client write must succeed");
    };
    runtime.block_on(async move {
        let mut raced = ProvidedRecvFuture::new(fd);
        core::future::poll_fn(|cx| {
            // IGNORE: the poll outcome is irrelevant -- the future is
            // abandoned mid-flight on purpose to exercise the cancel path.
            let _ = Pin::new(&mut raced).poll(cx);
            Poll::Ready(())
        })
        .await;
        drop(raced);
    });
    // Rotate well past the pool's 256 entries on fresh roots (fresh tokens,
    // so the raced op's completion cannot alias a live round). Every entry
    // must come back into kernel rotation: a single leaked buffer id -- the
    // raced one included -- caps the distinct storage addresses below 256.
    let mut seen = [0usize; 256];
    let mut distinct = 0usize;
    for _ in 0..300 {
        let address = recv_round(&mut client, &mut runtime, fd);
        if !seen[..distinct].contains(&address) {
            assert!(
                distinct < seen.len(),
                "more distinct buffers than the pool holds",
            );
            seen[distinct] = address;
            distinct += 1;
        }
    }
    assert_eq!(
        distinct, 256,
        "every pool entry rotates back through the kernel, none leaked",
    );
}

#[test]
fn escaped_view_refuses_access_outside_its_run() {
    let (mut client, server) = loopback_pair();
    let Ok(()) = client.write_all(b"escape") else {
        panic!("the client write must succeed");
    };
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let fd = server.as_raw_fd();
    let view = match runtime.block_on(async move { ProvidedRecvFuture::new(fd).await }) {
        Ok(view) => view,
        Err(error) if error.kind() == ErrorKind::Unsupported => return,
        Err(error) => panic!("the provided recv must resolve: {error}"),
    };
    // The view left its run as the root output. The pool registration is
    // cleared, so byte access must refuse by panicking -- never a read
    // through a slot the run-loop no longer brackets.
    let access = catch_unwind(AssertUnwindSafe(|| view.as_slice().len()));
    assert!(
        access.is_err(),
        "an escaped view must refuse access outside its run",
    );
    // Its drop skips the recycle quietly; the pool entry is a bounded loss.
    drop(view);
}
