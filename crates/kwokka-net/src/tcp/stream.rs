//! Connected TCP socket -- the owned endpoint an accept or connect lands.

use core::{future::Future, time::Duration};
use std::{
    io,
    net::{self, SocketAddr, ToSocketAddrs},
    os::fd::{AsRawFd, OwnedFd, RawFd},
};

use kwokka_io::{
    MAX_INLINE_CAP,
    addr::SockAddr,
    boundary::create_stream_socket,
    operation::{
        FixedBuf, IoBuf, IoBufMut, ProvidedBuf, ProvidedRecvFuture, RecvFuture, SendFuture,
        SendZcFuture,
    },
};

use crate::tcp::{RecvStream, connect::ConnectFuture};

/// Fails to compile when `CAP` exceeds [`MAX_INLINE_CAP`].
///
/// The buffered socket futures keep their kernel-facing bytes in the worker's
/// in-flight slot registry, [`MAX_INLINE_CAP`] bytes wide per slot, so a `CAP`
/// past that stride cannot be satisfied at submit time. Each `CAP`-generic
/// convenience method calls this in a `const` block, turning an oversized `CAP`
/// into a compile error instead of the registry's runtime `-EINVAL`.
///
/// # Panics
///
/// Panics during `const` evaluation -- a compile error at the call site -- when
/// `CAP` exceeds [`MAX_INLINE_CAP`]; it has no runtime effect.
const fn assert_cap_fits<const CAP: usize>() {
    assert!(
        CAP <= MAX_INLINE_CAP,
        "CAP exceeds kwokka_io::MAX_INLINE_CAP -- the in-flight slot stride",
    );
}

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
    /// Connects to `addr`, resolving to the connected stream.
    ///
    /// Resolves `addr` through the standard library, then drives a connect op
    /// per resolved address through the runtime's completion backend until one
    /// succeeds. A fresh socket of the peer's family is created for each
    /// attempt; the connected socket lands owned, so dropping the stream closes
    /// it. Address resolution is synchronous, matching
    /// [`TcpListener::bind`](crate::tcp::TcpListener::bind).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: opens a real connection through io_uring at runtime.
    /// use kwokka_net::tcp::TcpStream;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let stream = runtime.block_on(TcpStream::connect("127.0.0.1:8080"))?;
    /// let _peer = stream.peer_addr()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics when awaited outside a runtime task or through a combinator that
    /// wraps the waker, per the connect future's contract.
    ///
    /// # Errors
    ///
    /// Returns the OS error the kernel reported for the last connect attempt, or
    /// an [`io::ErrorKind::InvalidInput`] error when `addr` resolves to no
    /// address. A refused peer surfaces the connect error rather than hanging.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut last_error = None;
        for socket_addr in addr.to_socket_addrs()? {
            let (socket, future) = match prepare_connect(socket_addr, None) {
                Ok(prepared) => prepared,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let result = future.await;
            if result >= 0 {
                return Ok(Self::from(socket));
            }
            last_error = Some(io::Error::from_raw_os_error(-result));
        }
        Err(last_error.unwrap_or_else(no_address_error))
    }

    /// Connects to `addr`, bounding the attempt by `timeout`.
    ///
    /// Mirrors [`connect`](Self::connect) for a single address, arming the
    /// connect with a native per-op deadline: when `timeout` elapses first the
    /// kernel cancels the connect and the attempt returns a cancellation error.
    /// A backend without a native deadline rejects the timeout rather than
    /// dropping the bound.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: opens a real connection through io_uring at runtime.
    /// use core::time::Duration;
    /// use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    ///
    /// use kwokka_net::tcp::TcpStream;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
    /// let timeout = Duration::from_secs(5);
    /// let stream = runtime.block_on(TcpStream::connect_timeout(&addr, timeout))?;
    /// let _peer = stream.peer_addr()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics when awaited outside a runtime task or through a combinator that
    /// wraps the waker, per the connect future's contract.
    ///
    /// # Errors
    ///
    /// Returns the OS error the kernel reported for the connect, including the
    /// cancellation error when the deadline elapses before the connection is
    /// established.
    pub async fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> io::Result<Self> {
        let deadline_ns = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
        let (socket, future) = prepare_connect(*addr, Some(deadline_ns))?;
        let result = future.await;
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        Ok(Self::from(socket))
    }

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
        const { assert_cap_fits::<CAP>() };
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
        const { assert_cap_fits::<CAP>() };
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
        const { assert_cap_fits::<CAP>() };
        SendZcFuture::new(self.inner.as_raw_fd(), FixedBuf::new(data, len))
    }

    /// Hands out the future receiving into the caller-owned buffer `buf`.
    ///
    /// The buffer-generic sibling of [`recv`](Self::recv): the caller supplies
    /// any [`IoBufMut`] buffer (today an `[u8; N]` array) and awaiting the
    /// future resolves to an [`io::Result`] byte count paired with that buffer
    /// handed back -- the bytes received (a short count on a partial read, or
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
    /// let (result, _buf) = runtime.block_on(stream.recv_buf([0u8; 64]));
    /// let _read = result?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn recv_buf<B: IoBufMut>(
        &self,
        buf: B,
    ) -> impl Future<Output = (io::Result<usize>, B)> + use<B> {
        RecvFuture::new(self.inner.as_raw_fd(), buf)
    }

    /// Hands out the future sending the initialized bytes of the caller-owned
    /// buffer `buf`.
    ///
    /// The buffer-generic sibling of [`send`](Self::send): the caller supplies
    /// any [`IoBuf`] source -- an `[u8; N]` array (all `N` bytes) or a
    /// [`FixedBuf`] carrying a partial length. Awaiting it resolves to an
    /// [`io::Result`] byte count (a short count when the socket send buffer
    /// fills); `buf` is a pre-submit source the future drops on resolve, not
    /// handed back. The kernel reads a worker-owned copy of the bytes, so
    /// dropping the future mid-flight is safe. Await it directly on a runtime
    /// task: polling it through a waker the runtime did not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring at runtime.
    /// use kwokka_net::tcp::{FixedBuf, TcpListener};
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let _sent = runtime.block_on(stream.send_buf(FixedBuf::new(*b"hello!", 5)))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Resolves to the [`io::Error`] the kernel maps the send to (for example a
    /// reset or closed connection), or an `-EINVAL` error when the buffer's
    /// initialized length exceeds the in-flight slot stride the worker copies
    /// the bytes through.
    pub fn send_buf<B: IoBuf>(&self, buf: B) -> impl Future<Output = io::Result<usize>> + use<B> {
        SendFuture::new(self.inner.as_raw_fd(), buf)
    }

    /// Hands out the future sending the initialized bytes of the caller-owned
    /// buffer `buf`, zero-copy when the kernel supports it.
    ///
    /// The buffer-generic sibling of [`send_zc`](Self::send_zc): like
    /// [`send_buf`](Self::send_buf), but a supporting kernel (6.0 and up) sends
    /// the bytes without copying them into kernel space and resolves on the
    /// buffer-release notification, so the awaited byte count arrives when the
    /// buffer is free to reuse. A kernel without zero-copy send falls back to a
    /// plain copying send. The caller supplies any [`IoBuf`] source; the kernel
    /// reads a worker-owned copy of the bytes, so dropping the future
    /// mid-flight is safe. Await it directly on a runtime task: polling it
    /// through a waker the runtime did not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring 6.0+ at runtime.
    /// use kwokka_net::tcp::{FixedBuf, TcpListener};
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let _sent = runtime.block_on(stream.send_zc_buf(FixedBuf::new(*b"hello!", 5)))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn send_zc_buf<B: IoBuf>(
        &self,
        buf: B,
    ) -> impl Future<Output = io::Result<usize>> + use<B> {
        SendZcFuture::new(self.inner.as_raw_fd(), buf)
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Creates a fresh socket for `socket_addr` and builds its connect future.
///
/// Synchronous so the address never lands in the awaiting frame twice: the
/// `SockAddr` moves into the returned [`ConnectFuture`], leaving the caller to
/// hold only the owned socket and the future across the await. A `deadline_ns`
/// of `Some` arms a native per-op timeout; `None` submits a plain connect.
///
/// The socket is returned alongside the future so the caller keeps it alive for
/// the op, then hands it to the stream on success or drops it on error. Closing
/// it after submission is sound whatever the close-versus-cancel order: the
/// kernel resolves the fd to a file reference when the connect op is submitted
/// and holds that reference for the op's lifetime (`io_uring_prep_connect.3`),
/// independent of the userspace fd table, so a mid-flight drop never races an
/// in-flight connect.
fn prepare_connect(
    socket_addr: SocketAddr,
    deadline_ns: Option<u64>,
) -> io::Result<(OwnedFd, ConnectFuture)> {
    let addr = SockAddr::from(socket_addr);
    let socket = create_stream_socket(addr.family())?;
    let raw = socket.as_raw_fd();
    let future = match deadline_ns {
        Some(deadline_ns) => ConnectFuture::with_deadline(raw, addr, deadline_ns),
        None => ConnectFuture::new(raw, addr),
    };
    Ok((socket, future))
}

/// The error a `connect` returns when its address resolution yields nothing.
fn no_address_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "could not resolve an address to connect to",
    )
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

#[cfg(all(target_os = "linux", not(any(miri, loom))))]
#[cfg(test)]
mod tests {
    use core::{
        pin::Pin,
        task::{Context, Poll},
    };

    use kwokka_runtime::Runtime;

    use super::*;
    use crate::tcp::TcpListener;

    // Polls the coupled socket and connect future once to submit the connect,
    // then drops both in flight -- firing the future's cancel and the socket's
    // close together, the drop path the public `connect` entry introduces.
    struct DropCoupledConnect(Option<(OwnedFd, ConnectFuture)>);

    impl Future for DropCoupledConnect {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if let Some((_socket, future)) = self.0.as_mut() {
                assert!(
                    Pin::new(future).poll(cx).is_pending(),
                    "the first poll submits the connect and leaves it in flight",
                );
            }
            self.0 = None;
            Poll::Ready(())
        }
    }

    #[test]
    fn dropped_coupled_connect_keeps_serving() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let Ok(addr) = listener.local_addr() else {
            panic!("the listener must report its local address");
        };
        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        let Ok(pair) = prepare_connect(addr, None) else {
            panic!("prepare_connect must create the client socket");
        };
        // Submit then drop the connect in flight: the socket closes and the
        // future queues its cancel on the owning worker.
        runtime.block_on(DropCoupledConnect(Some(pair)));
        // The worker survived the coupled drop -- a fresh connect still resolves.
        let Ok(_client) = runtime.block_on(TcpStream::connect(addr)) else {
            panic!("the runtime keeps serving after a dropped coupled connect");
        };
    }
}
