//! Owned file handle -- async-shaped opening, pinned futures for the bytes.
//!
//! Opening completes inline on the worker today: one blocking syscall,
//! microseconds on a local filesystem and unbounded on a networked one,
//! stalling the worker for the duration. The signatures are async from
//! the start, so lowering the open onto the ring later swaps the body
//! with no caller-visible change; the path-buffer ownership design that
//! lowering needs is tracked separately.

use core::future::Future;
use std::{
    fs, io,
    os::fd::{AsRawFd, OwnedFd, RawFd},
    path::Path,
};

use kwokka_io::operation::{FileReadFuture, FileWriteFuture, FixedBuf};

/// An open file on the runtime.
///
/// Owns the descriptor for its lifetime; dropping the handle closes it.
/// [`read`](Self::read) and [`write`](Self::write) hand out the pinned
/// completion futures conversing with the descriptor at an offset.
pub struct File {
    /// The open file, owned through the std handle.
    inner: fs::File,
}

impl File {
    /// Opens an existing file read-only.
    ///
    /// Async-shaped over a one-shot blocking syscall -- the 0.1.0
    /// exception this module's docs pin down. The signature stays put
    /// when the ring-lowered open lands, so the swap is not a breaking
    /// change.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the path cannot be opened -- absent,
    /// permission-denied, or not a file.
    #[expect(
        clippy::unused_async,
        reason = "async-shaped per the locked I/O principle; the 0.2.0 ring-lowered open swaps the body without a breaking change, and this expectation self-signals when it does"
    )]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let inner = fs::File::open(path)?;
        Ok(Self { inner })
    }

    /// Creates a file for writing, truncating it when present.
    ///
    /// Async-shaped over a one-shot blocking syscall, like
    /// [`open`](Self::open) and under the same 0.1.0 exception.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the file cannot be created -- the parent
    /// directory is absent, or permission is denied.
    #[expect(
        clippy::unused_async,
        reason = "async-shaped per the locked I/O principle; the 0.2.0 ring-lowered open swaps the body without a breaking change, and this expectation self-signals when it does"
    )]
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let inner = fs::File::create(path)?;
        Ok(Self { inner })
    }

    /// Hands out the future reading up to `CAP` bytes at byte `offset`.
    ///
    /// Awaiting it resolves to an [`io::Result`] byte count paired with the
    /// filled buffer. Await it directly on a runtime task; polling it through a
    /// waker the runtime did not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: opens a real file and drives io_uring at runtime.
    /// use kwokka_fs::file::File;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let file = runtime.block_on(File::open("Cargo.toml"))?;
    /// let (result, _buf) = runtime.block_on(file.read::<64>(0));
    /// let _read = result?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn read<const CAP: usize>(
        &self,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, [u8; CAP])> + use<CAP> {
        FileReadFuture::new(self.inner.as_raw_fd(), offset, [0u8; CAP])
    }

    /// Hands out the future writing `data`'s first `len` bytes at `offset`.
    ///
    /// `data` is a `CAP`-byte array and `len` marks how many of its leading
    /// bytes to write (clamped to `CAP`); the rest is ignored. Awaiting it
    /// resolves to an [`io::Result`] byte count. Await it directly on a runtime
    /// task; polling it through a waker the runtime did not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: creates a real file and drives io_uring at runtime.
    /// use kwokka_fs::file::File;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let file = runtime.block_on(File::create("scratch.bin"))?;
    /// let mut data = [0u8; 64];
    /// data[..5].copy_from_slice(b"hello");
    /// let _written = runtime.block_on(file.write::<64>(0, data, 5))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn write<const CAP: usize>(
        &self,
        offset: u64,
        data: [u8; CAP],
        len: usize,
    ) -> impl Future<Output = io::Result<usize>> + use<CAP> {
        FileWriteFuture::new(self.inner.as_raw_fd(), offset, FixedBuf::new(data, len))
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<OwnedFd> for File {
    /// Adopts an owned file descriptor as a handle.
    fn from(fd: OwnedFd) -> Self {
        Self {
            inner: fs::File::from(fd),
        }
    }
}

impl From<fs::File> for File {
    /// Adopts an already-open std file, taking ownership of its fd.
    fn from(inner: fs::File) -> Self {
        Self { inner }
    }
}
