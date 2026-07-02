//! Connected TCP socket -- the owned endpoint an accept or connect lands.

use core::future::Future;
use std::{
    io,
    net::{self, SocketAddr},
    os::fd::{AsRawFd, OwnedFd, RawFd},
};

use kwokka_io::operation::{ProvidedBuf, ProvidedRecvFuture, RecvFuture, SendFuture};

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
    /// filled buffer: the bytes received (a short count on a partial read, or
    /// `0` at end of stream), or the mapped error. The bytes live in a
    /// worker-owned registry for the op lifetime, so dropping the future
    /// mid-flight is safe. Await it directly on a runtime task: polling it
    /// through a waker the runtime did not build panics.
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
    pub fn recv<const CAP: usize>(
        &self,
    ) -> impl Future<Output = (io::Result<usize>, [u8; CAP])> + use<CAP> {
        RecvFuture::new(self.inner.as_raw_fd())
    }

    /// Hands out the future receiving into a kernel-selected provided buffer.
    ///
    /// Awaiting it resolves to an [`io::Result`] holding a [`ProvidedBuf`]: a
    /// borrowed, zero-copy view over the worker's provided-buffer ring, spanning
    /// the bytes received (an empty view at end of stream), or the mapped error.
    /// No byte is copied between the kernel write and the caller's read. The
    /// view borrows the worker's pool, so it is `!Send` and stays valid only
    /// within the runtime run that produced it. Await it directly on a runtime
    /// task: polling it through a waker the runtime did not build panics.
    ///
    /// The pool owns the bytes for the op lifetime, so dropping the future
    /// mid-flight is safe. A task that drops an in-flight provided recv must not
    /// issue another before it settles -- two ops sharing one task token cannot
    /// be told apart by the completion drain.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer, io_uring, and a registered buf_ring.
    /// use kwokka_net::tcp::TcpListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let received = runtime.block_on(stream.recv_provided());
    /// let buf = received?;
    /// let _bytes: &[u8] = &buf;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Resolves [`io::ErrorKind::Unsupported`] when the backend registered no
    /// provided-buffer ring (a kernel without `buf_ring`, or a ring that failed
    /// to register); the caller falls back to [`recv`](Self::recv). A pool
    /// exhausted of free buffers surfaces `-ENOBUFS` as its raw OS error, the
    /// caller's signal to re-arm; other negative completions map to their
    /// `-errno`.
    pub fn recv_provided(&self) -> impl Future<Output = io::Result<ProvidedBuf>> + use<> {
        ProvidedRecvFuture::new(self.inner.as_raw_fd())
    }

    /// Hands out the future sending the first `len` bytes of `data` (clamped
    /// to `CAP`) over this socket.
    ///
    /// `data` is a `CAP`-byte array and `len` marks how many of its leading
    /// bytes to send; the rest is ignored. Awaiting it resolves to an
    /// [`io::Result`] byte count (a short count when the socket send buffer
    /// fills). The kernel reads a worker-owned copy of the bytes, so dropping
    /// the future mid-flight is safe. Await it directly on a runtime task:
    /// polling it through a waker the runtime did not build panics.
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
    pub fn send<const CAP: usize>(
        &self,
        data: [u8; CAP],
        len: usize,
    ) -> impl Future<Output = io::Result<usize>> + use<CAP> {
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
