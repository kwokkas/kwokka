//! Connected TCP socket -- the owned endpoint an accept or connect lands.

use core::future::Future;
use std::{
    io,
    net::{self, SocketAddr},
    os::fd::{AsRawFd, OwnedFd, RawFd},
};

use kwokka_io::operation::{
    FixedBuf, ProvidedBuf, ProvidedRecvFuture, RecvFuture, SendFuture, SendZcFuture,
};

use crate::tcp::RecvStream;

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
        RecvFuture::new(self.inner.as_raw_fd(), [0u8; CAP])
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

    /// Streams received chunks into kernel-selected provided buffers.
    ///
    /// Returns a [`RecvStream`] bound to this connection: `next().await` yields
    /// `Some(Ok(buf))` for each received chunk, `Some(Err(_))` for a per-recv
    /// error, or `None` once a multishot op ends. On a kernel with multishot
    /// recv, one submitted op streams a completion per chunk; without it, the
    /// stream degrades to one single-shot provided recv per item. Each `Ok` item
    /// is a [`ProvidedBuf`] borrowing the worker's pool, recycled to the ring on
    /// drop, and an empty view marks end of stream. The stream borrows `self`, so
    /// it cannot outlive the connection and observe a closed fd.
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
    /// runtime.block_on(async move {
    ///     let mut recv = stream.recv_multishot();
    ///     while let Some(chunk) = recv.next().await {
    ///         let buf = chunk?;
    ///         if buf.is_empty() {
    ///             break;
    ///         }
    ///         let _bytes: &[u8] = &buf;
    ///     }
    ///     Ok::<(), std::io::Error>(())
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// A backend with no provided-buffer ring resolves the first item as
    /// [`io::ErrorKind::Unsupported`]: the caller falls back to
    /// [`recv`](Self::recv). A pool exhausted of free buffers surfaces `-ENOBUFS`
    /// as its raw OS error, the caller's signal to re-arm; other negative
    /// completions map to their `-errno`.
    pub fn recv_multishot(&self) -> RecvStream<'_> {
        RecvStream::new(self.inner.as_raw_fd())
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
    ///
    /// # Errors
    ///
    /// Resolves to the [`io::Error`] the kernel maps the send to (for example a
    /// reset or closed connection), or an `-EINVAL` error when `len` exceeds the
    /// in-flight slot stride the worker copies the bytes through.
    pub fn send<const CAP: usize>(
        &self,
        data: [u8; CAP],
        len: usize,
    ) -> impl Future<Output = io::Result<usize>> + use<CAP> {
        SendFuture::new(self.inner.as_raw_fd(), FixedBuf::new(data, len))
    }

    /// Hands out the future sending the first `len` bytes of `data` (clamped to
    /// `CAP`) over this socket, zero-copy when the kernel supports it.
    ///
    /// Like [`send`](Self::send), but a supporting kernel (6.0 and up) sends the
    /// bytes without copying them into kernel space and posts a second
    /// completion once it has released the buffer; the future resolves on that
    /// notification, so the awaited byte count arrives when the buffer is free
    /// to reuse. A kernel without zero-copy send falls back to a plain copying
    /// send. Awaiting it resolves to an [`io::Result`] byte count (a short count
    /// when the socket send buffer fills). The kernel reads a worker-owned copy
    /// of the bytes, so dropping the future mid-flight is safe. Await it
    /// directly on a runtime task: polling it through a waker the runtime did
    /// not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring 6.0+ at runtime.
    /// use kwokka_net::tcp::TcpListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let mut data = [0u8; 64];
    /// data[..5].copy_from_slice(b"hello");
    /// let _sent = runtime.block_on(stream.send_zc::<64>(data, 5))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn send_zc<const CAP: usize>(
        &self,
        data: [u8; CAP],
        len: usize,
    ) -> impl Future<Output = io::Result<usize>> + use<CAP> {
        SendZcFuture::new(self.inner.as_raw_fd(), data, len)
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
