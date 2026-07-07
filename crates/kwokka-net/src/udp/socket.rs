//! Datagram UDP socket -- the bound endpoint driving `sendmsg` / `recvmsg`.
//!
//! Binding names the socket in one synchronous syscall; the datagram
//! transfers wait on the kernel and arrive as futures the runtime drives
//! through its completion backend against this socket's raw fd. `send_to`
//! and `recv_from` carry a per-datagram peer address through a `msghdr`;
//! `connect` fixes a default peer so the plain `send` / `recv` ops apply,
//! sharing the stream socket futures.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::{
    io,
    net::{self, SocketAddr, ToSocketAddrs},
    os::fd::{AsRawFd, RawFd},
};

use kwokka_io::{
    addr::SockAddr,
    operation::{IoBuf, IoBufMut, RecvFuture, RecvMsgFuture, SendFuture, SendMsgFuture},
};

/// A bound UDP socket sending and receiving datagrams over the completion
/// backend.
///
/// Owns the bound socket for its lifetime; dropping the socket closes the fd.
/// Unconnected datagrams travel through [`send_to_buf`](Self::send_to_buf) and
/// [`recv_from_buf`](Self::recv_from_buf), which carry the peer or sender
/// address per call. After [`connect`](Self::connect) fixes a default peer,
/// [`send_buf`](Self::send_buf) and [`recv_buf`](Self::recv_buf) exchange
/// datagrams with it and need no address.
pub struct UdpSocket {
    /// The bound socket, owned through the std socket.
    inner: net::UdpSocket,
}

impl UdpSocket {
    /// Binds a UDP socket to `addr`.
    ///
    /// Resolution may yield several addresses; the first that binds wins,
    /// matching the std contract. Binding to port 0 lets the OS assign a port,
    /// readable through [`local_addr`](Self::local_addr).
    ///
    /// # Errors
    ///
    /// Returns the OS error when no resolved address can be bound -- the port
    /// is taken, the address is not local, or resolution itself failed.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # // no_run: binds a live socket the doctest host may lack.
    /// use kwokka_net::udp::UdpSocket;
    ///
    /// let socket = UdpSocket::bind("127.0.0.1:0")?;
    /// let _local = socket.local_addr()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let inner = net::UdpSocket::bind(addr)?;
        Ok(Self { inner })
    }

    /// Returns the local address the socket is bound to.
    ///
    /// The OS-assigned port shows here after binding port 0.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket name cannot be read -- the fd was
    /// invalidated outside this type's control.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Fixes `addr` as the default peer for [`send_buf`](Self::send_buf) and
    /// [`recv_buf`](Self::recv_buf).
    ///
    /// A connected UDP socket sends to and receives from one peer, so its
    /// datagrams need no per-call address and reuse the stream send / recv
    /// ops. `send_to_buf` and `recv_from_buf` still work on a connected socket.
    ///
    /// # Errors
    ///
    /// Returns the OS error when no resolved address can be set as the peer.
    pub fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
        self.inner.connect(addr)
    }

    /// Returns the peer address a prior [`connect`](Self::connect) fixed.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket has no connected peer, or the name
    /// cannot be read.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Hands out the future sending `buf`'s initialized bytes as one datagram
    /// to `addr`.
    ///
    /// The kernel stages the address and a copy of the bytes in the worker's
    /// in-flight slot, so dropping the future mid-flight is safe. Awaiting it
    /// resolves to an [`io::Result`] byte count. The caller supplies any
    /// [`IoBuf`] source; `buf` is a pre-submit source the future drops on
    /// resolve, not handed back. Await it directly on a runtime task: polling
    /// it through a waker the runtime did not build panics.
    ///
    /// # Errors
    ///
    /// Resolves to the [`io::Error`] the kernel maps the send to, or an
    /// `-EINVAL` error when the datagram exceeds the in-flight slot payload
    /// capacity (standard-MTU datagrams fit; jumbo frames are a follow-up).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # // no_run: drives io_uring against a live socket at runtime.
    /// use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    ///
    /// use kwokka_net::udp::UdpSocket;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let socket = UdpSocket::bind("127.0.0.1:0")?;
    /// let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9000));
    /// let _sent = runtime.block_on(socket.send_to_buf(*b"ping", peer))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn send_to_buf<B: IoBuf>(
        &self,
        buf: B,
        addr: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + use<B> {
        SendMsgFuture::new(self.inner.as_raw_fd(), SockAddr::from(addr), buf)
    }

    /// Hands out the future receiving one datagram into `buf`, returning the
    /// sender address alongside the byte count.
    ///
    /// A later poll copies the datagram into `buf` and returns the sender by
    /// value paired with the count, or the mapped [`io::Error`]; `buf` moves
    /// out with the result. The slot bytes are worker-owned, so dropping the
    /// future before the datagram arrives is safe. A buffer larger than the
    /// in-flight slot payload capacity resolves as unsupported rather than
    /// truncating the datagram. Await it directly on a runtime task: polling it
    /// through a waker the runtime did not build panics.
    ///
    /// # Errors
    ///
    /// The paired result is the [`io::Error`] the kernel maps the receive to,
    /// an `-EINVAL` error when `buf` exceeds the slot payload capacity, or an
    /// [`io::ErrorKind::InvalidData`] error when the completion carried no IPv4
    /// or IPv6 sender address.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # // no_run: drives io_uring against a live socket at runtime.
    /// use kwokka_net::udp::UdpSocket;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let socket = UdpSocket::bind("127.0.0.1:0")?;
    /// let (result, _buf) = runtime.block_on(socket.recv_from_buf([0u8; 64]));
    /// let (_read, _sender) = result?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn recv_from_buf<B: IoBufMut>(
        &self,
        buf: B,
    ) -> impl Future<Output = (io::Result<(usize, SocketAddr)>, B)> + use<B> {
        RecvFromFuture {
            inner: RecvMsgFuture::new(self.inner.as_raw_fd(), buf),
        }
    }

    /// Hands out the future sending `buf`'s initialized bytes to the connected
    /// peer.
    ///
    /// The connected sibling of [`send_to_buf`](Self::send_to_buf): after
    /// [`connect`](Self::connect), the datagram goes to the default peer with
    /// no per-call address, reusing the stream send op. Awaiting it resolves to
    /// an [`io::Result`] byte count; `buf` is a pre-submit source the future
    /// drops on resolve. Await it directly on a runtime task.
    ///
    /// # Errors
    ///
    /// Resolves to the [`io::Error`] the kernel maps the send to (for example
    /// no connected peer), or an `-EINVAL` error when the datagram exceeds the
    /// in-flight slot stride.
    pub fn send_buf<B: IoBuf>(&self, buf: B) -> impl Future<Output = io::Result<usize>> + use<B> {
        SendFuture::new(self.inner.as_raw_fd(), buf)
    }

    /// Hands out the future receiving one datagram from the connected peer into
    /// `buf`.
    ///
    /// The connected sibling of [`recv_from_buf`](Self::recv_from_buf): after
    /// [`connect`](Self::connect), the datagram arrives from the default peer
    /// with no sender address to report, reusing the stream recv op. A later
    /// poll copies the bytes into `buf` and returns the count paired with it;
    /// `buf` moves out with the result. Await it directly on a runtime task.
    ///
    /// # Errors
    ///
    /// The paired result is the [`io::Error`] the kernel maps the receive to,
    /// or an `-EINVAL` error when `buf` exceeds the in-flight slot stride.
    pub fn recv_buf<B: IoBufMut>(
        &self,
        buf: B,
    ) -> impl Future<Output = (io::Result<usize>, B)> + use<B> {
        RecvFuture::new(self.inner.as_raw_fd(), buf)
    }
}

/// The future [`UdpSocket::recv_from_buf`] returns.
///
/// Wraps the io-layer recvmsg future and maps its optional sender into a
/// standard [`SocketAddr`] paired with the byte count, staying compact enough
/// for the runtime's task slot rather than nesting an `async` state machine.
struct RecvFromFuture<B: IoBufMut> {
    /// The io-layer recvmsg future carrying the caller's buffer.
    inner: RecvMsgFuture<B>,
}

impl<B: IoBufMut> Future for RecvFromFuture<B> {
    type Output = (io::Result<(usize, SocketAddr)>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Poll::Ready((result, sender, buf)) = Pin::new(&mut self.get_mut().inner).poll(cx)
        else {
            return Poll::Pending;
        };
        let paired = result.and_then(|count| match sender {
            Some(SockAddr::V4(addr)) => Ok((count, SocketAddr::V4(addr))),
            Some(SockAddr::V6(addr)) => Ok((count, SocketAddr::V6(addr))),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recvmsg completion carried no IP sender address",
            )),
        });
        Poll::Ready((paired, buf))
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    #[test]
    fn bind_reports_its_local_addr() {
        let Ok(socket) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding a loopback UDP socket must succeed");
        };
        let Ok(local) = socket.local_addr() else {
            panic!("a bound socket reports its local address");
        };
        assert_ne!(local.port(), 0, "binding port 0 assigns a real port");
    }

    #[test]
    fn connect_fixes_the_peer_address() {
        let Ok(peer) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the peer socket must succeed");
        };
        let Ok(peer_addr) = peer.local_addr() else {
            panic!("the peer reports its address");
        };
        let Ok(socket) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the client socket must succeed");
        };
        let Ok(()) = socket.connect(peer_addr) else {
            panic!("connecting to a bound peer must succeed");
        };
        assert_eq!(
            socket.peer_addr().ok(),
            Some(peer_addr),
            "peer_addr reports the connected peer",
        );
    }
}
