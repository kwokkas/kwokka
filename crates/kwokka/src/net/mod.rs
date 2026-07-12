//! Network endpoints -- TCP sockets over the completion backend.
//!
//! Gated behind the `net` feature. [`TcpListener`] binds a port and
//! accepts inbound connections; the accepted [`TcpStream`] converses
//! through the [`recv`](TcpStream::recv) and [`send`](TcpStream::send)
//! futures, which read and write the stream's own pinned inline buffers
//! with no heap allocation. [`recv_provided`](TcpStream::recv_provided)
//! instead receives into a kernel-selected provided buffer and resolves a
//! borrowed zero-copy [`ProvidedBuf`]. [`send_zc`](TcpStream::send_zc) sends
//! zero-copy on a supporting kernel, falling back to a plain copying send
//! otherwise. The buffer-generic [`recv_buf`](TcpStream::recv_buf),
//! [`send_buf`](TcpStream::send_buf), and [`send_zc_buf`](TcpStream::send_zc_buf)
//! take a caller-owned buffer instead of a fixed `CAP` array; [`FixedBuf`]
//! carries a partial send length. Client-side connect arrives with a stream
//! constructor in a later release.
//!
//! # Examples
//!
//! ```no_run
//! # // no_run: binds a live socket and drives io_uring, which a doctest host may lack.
//! use kwokka::{net::TcpListener, runtime::Runtime};
//!
//! let mut runtime = Runtime::affine()?;
//! let listener = TcpListener::bind("127.0.0.1:0")?;
//! let stream = runtime.block_on(listener.accept())?;
//! let (result, _buf) = runtime.block_on(stream.recv::<64>());
//! let _read = result?;
//! # Ok::<(), std::io::Error>(())
//! ```

pub use kwokka_net::tcp::{FixedBuf, ProvidedBuf, TcpListener, TcpStream};
#[cfg(unix)]
pub use kwokka_net::udp::UdpSocket;
#[cfg(unix)]
pub use kwokka_net::unix::{UnixListener, UnixStream};
