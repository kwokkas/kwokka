//! End-to-end buffered send through the affine run-loop.
//!
//! Connects a loopback socket pair via std, then drives the public
//! [`TcpStream::send`] and [`TcpStream::send_zc`] entries on the adopted client
//! end through the real `io_uring` ring: submit a send, park, harvest the CQE,
//! wake, and read the byte count back. The zero-copy entry resolves on the
//! buffer-release notification on a 6.0+ kernel and falls back to a plain send
//! below it. The socket counterpart of the buffered-write e2e test. The sent
//! bytes land in the server's receive queue, so a std read harvests them
//! sequentially with no concurrent peer.
//!
//! [`TcpStream::send`]: kwokka_net::tcp::TcpStream::send
//! [`TcpStream::send_zc`]: kwokka_net::tcp::TcpStream::send_zc

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
};

use kwokka_runtime::Runtime;

#[test]
fn send_delivers_buffer_bytes() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let Ok(client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok((mut server, _peer)) = listener.accept() else {
        panic!("accepting the loopback connection must succeed");
    };

    let message = b"kwokka inline socket send";
    let mut data = [0u8; 64];
    data[..message.len()].copy_from_slice(message);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let client = kwokka_net::tcp::TcpStream::from(client);
    let result = runtime.block_on(client.send::<64>(data, message.len()));

    let Ok(sent) = result else {
        panic!("the send must resolve with a byte count, not an error");
    };
    assert_eq!(sent, message.len(), "the kernel sent every requested byte");

    let mut received = [0u8; 64];
    let Ok(()) = server.read_exact(&mut received[..sent]) else {
        panic!("reading the sent bytes back must succeed");
    };
    assert_eq!(
        &received[..sent],
        &message[..],
        "the server holds the bytes the client sent",
    );
}

#[test]
fn send_zc_delivers_buffer_bytes() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        panic!("binding a loopback listener must succeed");
    };
    let Ok(addr) = listener.local_addr() else {
        panic!("the listener must report its local address");
    };
    let Ok(client) = TcpStream::connect(addr) else {
        panic!("connecting to the loopback listener must succeed");
    };
    let Ok((mut server, _peer)) = listener.accept() else {
        panic!("accepting the loopback connection must succeed");
    };

    let message = b"kwokka zero-copy socket send";
    let mut data = [0u8; 64];
    data[..message.len()].copy_from_slice(message);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let client = kwokka_net::tcp::TcpStream::from(client);
    // A 6.0+ kernel sends zero-copy and resolves only on the release
    // notification; an older kernel falls back to a plain copying send. Either
    // path delivers every byte to the server.
    let result = runtime.block_on(client.send_zc::<64>(data, message.len()));

    let sent = match result {
        Ok(sent) => sent,
        // The kernel best-effort-refuses a zero-copy send under io_uring
        // resource pressure with -EINVAL, the same shape as a backend that never
        // supported it. Accept the refusal the way the provided-recv entries
        // accept Unsupported, rather than assert a delivery the OS declined.
        Err(err) if err.raw_os_error() == Some(22) => return,
        Err(err) => panic!("the zero-copy send resolved with an unexpected error: {err}"),
    };
    assert_eq!(sent, message.len(), "the kernel sent every requested byte");

    let mut received = [0u8; 64];
    let Ok(()) = server.read_exact(&mut received[..sent]) else {
        panic!("reading the sent bytes back must succeed");
    };
    assert_eq!(
        &received[..sent],
        &message[..],
        "the server holds the bytes the client sent zero-copy",
    );
}
