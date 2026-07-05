//! End-to-end buffered read through the affine run-loop.
//!
//! Writes a fixture file, then drives [`FileReadFuture`] on the real `io_uring`
//! ring: submit a read into the future's inline buffer, park, harvest the CQE,
//! wake, and read the result back. Proves the buffered submit seam end to end --
//! the buffer-carrying counterpart of the `block_on` self-wake test.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::os::fd::AsRawFd;

use kwokka_io::operation::FileReadFuture;
use kwokka_runtime::Runtime;

#[test]
fn file_read_returns_written_bytes() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary path must resolve");
    };
    let Some(dir) = exe.parent() else {
        panic!("the test binary must have a parent directory");
    };
    let path = dir.join("kwokka-inline-read.bin");
    let data = b"kwokka inline buffered read";
    let Ok(()) = std::fs::write(&path, data) else {
        panic!("writing the fixture file must succeed");
    };
    let Ok(file) = std::fs::File::open(&path) else {
        panic!("opening the fixture file must succeed");
    };

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let (result, buf) = runtime.block_on(FileReadFuture::new(file.as_raw_fd(), 0, [0u8; 64]));

    let Ok(read) = result else {
        panic!("the read must resolve with a byte count, not an error");
    };
    assert_eq!(
        &buf[..read],
        &data[..],
        "the inline buffer holds the file's bytes",
    );
}
