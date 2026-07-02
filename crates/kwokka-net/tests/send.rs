//! End-to-end buffered send through the affine run-loop.
//!
//! Connects a loopback socket pair via std, then drives the public
//! [`TcpStream::send`] entry on the adopted client end through the real
//! `io_uring` ring: submit a send, park, harvest the CQE, wake, and read the
//! byte count back. The socket counterpart of the buffered-write e2e test.
//! The sent bytes land in the server's receive queue, so a std read harvests
//! them sequentially with no concurrent peer.
//!
//! [`TcpStream::send`]: kwokka_net::tcp::TcpStream::send

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
