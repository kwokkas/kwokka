//! Connected TCP socket -- the owned endpoint an accept or connect lands.

use std::{
    io,
    net::{self, SocketAddr},
    os::fd::{AsRawFd, OwnedFd, RawFd},
};

use crate::tcp::{RecvFuture, SendFuture};

/// A connected TCP socket.
///
/// Owns the socket for its lifetime; dropping the stream closes the fd
/// and the connection with it. Address inspection delegates to the std
/// socket; [`recv`](Self::recv) and [`send`](Self::send) hand out the
/// completion futures conversing over it.
pub struct TcpStream {
    /// The connected socket, owned through the std stream.
    inner: net::TcpStream,
}

impl TcpStream {
    /// Returns the local address this end of the connection is bound to.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket name cannot be read -- the fd
    /// was invalidated outside this type's control.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the peer address this connection is established to.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the peer name cannot be read -- the
    /// connection reset, or the fd was invalidated outside this type's
    /// control.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Hands out the future receiving up to `CAP` bytes from this socket.
    ///
    /// Awaiting it resolves to an [`io::Result`] byte count paired with the
    /// filled buffer. See [`RecvFuture`] for the await-to-completion contract.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring at runtime.
    /// use kwokka_net::tcp::TcpListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let (result, _buf) = runtime.block_on(stream.recv::<64>());
    /// let _read = result?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn recv<const CAP: usize>(&self) -> RecvFuture<CAP> {
        RecvFuture::new(self.inner.as_raw_fd())
    }

    /// Hands out the future sending the first `len` bytes of `data` (clamped
    /// to `CAP`) over this socket.
    ///
    /// `data` is a `CAP`-byte array and `len` marks how many of its leading
    /// bytes to send; the rest is ignored. Awaiting it resolves to an
    /// [`io::Result`] byte count. See [`SendFuture`] for the
    /// await-to-completion contract.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring at runtime.
    /// use kwokka_net::tcp::TcpListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let mut data = [0u8; 64];
    /// data[..5].copy_from_slice(b"hello");
    /// let _sent = runtime.block_on(stream.send::<64>(data, 5))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn send<const CAP: usize>(&self, data: [u8; CAP], len: usize) -> SendFuture<CAP> {
        SendFuture::new(self.inner.as_raw_fd(), data, len)
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<OwnedFd> for TcpStream {
    /// Adopts an owned connected-socket descriptor as a stream.
    fn from(fd: OwnedFd) -> Self {
        Self {
            inner: net::TcpStream::from(fd),
        }
    }
}

impl From<net::TcpStream> for TcpStream {
    /// Adopts an already-connected std stream, taking ownership of its fd.
    fn from(inner: net::TcpStream) -> Self {
        Self { inner }
    }
}
