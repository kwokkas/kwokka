//! End-to-end UDP datagram roundtrip over the public entries.
//!
//! Binds two loopback [`UdpSocket`]s on the real `io_uring` ring and exchanges
//! a datagram: unconnected `send_to` / `recv_from` carrying the sender address,
//! then connected `send` / `recv` after `connect` fixes each peer. Proves the
//! `sendmsg` / `recvmsg` submit-and-complete path and the sender-address round
//! trip. The send precedes the recv in one task: the kernel buffers the
//! loopback datagram, so the recv finds it already delivered.
//!
//! [`UdpSocket`]: kwokka_net::udp::UdpSocket

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use kwokka_net::udp::UdpSocket;
use kwokka_runtime::Runtime;

#[test]
fn send_to_recv_from_roundtrips_with_sender() {
    let Ok(server) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the server socket must succeed");
    };
    let Ok(client) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the client socket must succeed");
    };
    let Ok(server_addr) = server.local_addr() else {
        panic!("the server reports its address");
    };
    let Ok(client_addr) = client.local_addr() else {
        panic!("the client reports its address");
    };

    let payload = *b"kwokka udp datagram";

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (sent, received, buf) = runtime.block_on(async move {
        let sent = client.send_to_buf(payload, server_addr).await;
        let (received, buf) = server.recv_from_buf([0u8; 64]).await;
        (sent, received, buf)
    });

    let Ok(sent) = sent else {
        panic!("the send_to must resolve with a byte count");
    };
    assert_eq!(sent, payload.len(), "the whole datagram is sent");
    let Ok((read, sender)) = received else {
        panic!("the recv_from must resolve with a count and sender");
    };
    assert_eq!(read, payload.len(), "the whole datagram is received");
    assert_eq!(sender, client_addr, "the sender address round-trips");
    assert_eq!(&buf[..read], &payload[..], "the payload arrives intact");
}

#[test]
fn connected_send_recv_roundtrips() {
    let Ok(server) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the server socket must succeed");
    };
    let Ok(client) = UdpSocket::bind("127.0.0.1:0") else {
        panic!("binding the client socket must succeed");
    };
    let Ok(server_addr) = server.local_addr() else {
        panic!("the server reports its address");
    };
    let Ok(client_addr) = client.local_addr() else {
        panic!("the client reports its address");
    };
    let Ok(()) = client.connect(server_addr) else {
        panic!("the client connects to the server");
    };
    let Ok(()) = server.connect(client_addr) else {
        panic!("the server connects to the client");
    };

    let payload = *b"connected datagram";

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (sent, received, buf) = runtime.block_on(async move {
        let sent = client.send_buf(payload).await;
        let (received, buf) = server.recv_buf([0u8; 64]).await;
        (sent, received, buf)
    });

    let Ok(sent) = sent else {
        panic!("the connected send must resolve with a byte count");
    };
    assert_eq!(sent, payload.len(), "the whole datagram is sent");
    let Ok(read) = received else {
        panic!("the connected recv must resolve with a byte count");
    };
    assert_eq!(read, payload.len(), "the whole datagram is received");
    assert_eq!(&buf[..read], &payload[..], "the payload arrives intact");
}
