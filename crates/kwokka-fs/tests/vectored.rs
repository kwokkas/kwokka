//! End-to-end vectored read and write through the affine run-loop.
//!
//! Drives [`VectoredWriteFuture`] and [`VectoredReadFuture`] on the real
//! `io_uring` ring: a `writev` gathers several source buffers into a file, and a
//! `readv` scatters the file's bytes back across several destination buffers.
//! Proves the vectored submit seam end to end -- the counterpart of the
//! single-buffer file read / write e2e tests.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::os::fd::AsRawFd;

use kwokka_io::operation::{FixedBuf, IoVec, IoVecMut, VectoredReadFuture, VectoredWriteFuture};
use kwokka_runtime::Runtime;

/// A short `writev` source: `bytes` in a sixteen-byte backing array, so three of
/// them plus the future's output stay inside the task-slot budget.
fn source(bytes: &[u8]) -> FixedBuf<16> {
    let mut data = [0u8; 16];
    data[..bytes.len()].copy_from_slice(bytes);
    FixedBuf::new(data, bytes.len())
}

#[test]
fn writev_gathers_every_buffer_into_the_file() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary path must resolve");
    };
    let Some(dir) = exe.parent() else {
        panic!("the test binary must have a parent directory");
    };
    let path = dir.join("kwokka-vectored-write.bin");
    let Ok(file) = std::fs::File::create(&path) else {
        panic!("creating the fixture file must succeed");
    };

    // Three source buffers with distinct bytes and distinct lengths, so a
    // wrongly ordered or short gather is visible in the concatenation. Small
    // arrays keep the future plus its `[FixedBuf; 3]` output inside the task-slot
    // budget.
    let first = source(b"kwokka");
    let second = source(b" gathers");
    let third = source(b" three buffers");
    let expected = b"kwokka gathers three buffers";

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (result, _bufs) = runtime.block_on(VectoredWriteFuture::new(
        file.as_raw_fd(),
        0,
        IoVec::new([first, second, third]),
    ));

    let Ok(written) = result else {
        panic!("the writev must resolve with a byte count, not an error");
    };
    assert_eq!(
        written,
        expected.len(),
        "the kernel wrote every source byte"
    );

    let Ok(contents) = std::fs::read(&path) else {
        panic!("reading the fixture file back must succeed");
    };
    assert_eq!(
        &contents[..],
        &expected[..],
        "the file holds the gathered buffers in order",
    );
}

#[test]
fn readv_scatters_the_file_across_every_buffer() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary path must resolve");
    };
    let Some(dir) = exe.parent() else {
        panic!("the test binary must have a parent directory");
    };
    let path = dir.join("kwokka-vectored-read.bin");
    let blob = b"kwokka scatters bytes across buffers";
    let Ok(()) = std::fs::write(&path, blob) else {
        panic!("writing the fixture file must succeed");
    };
    let Ok(file) = std::fs::File::open(&path) else {
        panic!("opening the fixture file must succeed");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    // Three destinations of ten bytes each: thirty bytes of capacity for a
    // thirty-six byte blob, so the read fills all three and stops short in the
    // last, and the tail six bytes are left for a follow-up read.
    let (result, bufs) = runtime.block_on(VectoredReadFuture::new(
        file.as_raw_fd(),
        0,
        IoVecMut::new([[0u8; 10], [0u8; 10], [0u8; 10]]),
    ));

    let Ok(read) = result else {
        panic!("the readv must resolve with a byte count, not an error");
    };
    assert_eq!(read, 30, "the read fills the whole thirty-byte capacity");
    // Each buffer holds its ten-byte slice of the blob, in order.
    assert_eq!(
        &bufs[0][..],
        &blob[0..10],
        "the first buffer holds bytes 0..10"
    );
    assert_eq!(
        &bufs[1][..],
        &blob[10..20],
        "the second buffer holds bytes 10..20"
    );
    assert_eq!(
        &bufs[2][..],
        &blob[20..30],
        "the third buffer holds bytes 20..30"
    );
}
