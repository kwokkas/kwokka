//! End-to-end buffered send through the affine run-loop.
//!
//! Connects a loopback socket pair via std, then drives [`SendFuture`] on the
//! client fd through the real `io_uring` ring: submit a send from the future's
//! inline buffer, park, harvest the CQE, wake, and read the byte count back.
//! The socket counterpart of the buffered-write e2e test. The sent bytes land
//! in the server's receive queue, so a std read harvests them sequentially with
//! no concurrent peer.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
};

use kwokka_net::tcp::SendFuture;
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
    let result = runtime.block_on(SendFuture::<64>::new(
        client.as_raw_fd(),
        data,
        message.len(),
    ));

    assert!(result >= 0, "the send completed with an error: {result}");
    let Ok(sent) = usize::try_from(result) else {
        panic!("a non-negative send result fits usize");
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
