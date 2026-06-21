//! End-to-end buffered write through the affine run-loop.
//!
//! Drives [`FileWriteFuture`] on the real `io_uring` ring: submit a write from
//! the future's inline buffer, park, harvest the CQE, wake, and read the byte
//! count back. The kernel-reads-buffer counterpart of the buffered read e2e.

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use std::os::fd::AsRawFd;

use kwokka_fs::file::FileWriteFuture;
use kwokka_runtime::Runtime;

#[test]
fn file_write_persists_buffer() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary path must resolve");
    };
    let Some(dir) = exe.parent() else {
        panic!("the test binary must have a parent directory");
    };
    let path = dir.join("kwokka-inline-write.bin");
    let Ok(file) = std::fs::File::create(&path) else {
        panic!("creating the fixture file must succeed");
    };

    let message = b"kwokka inline buffered write";
    let mut data = [0u8; 64];
    data[..message.len()].copy_from_slice(message);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };
    let result = runtime.block_on(FileWriteFuture::<64>::new(
        file.as_raw_fd(),
        0,
        data,
        message.len(),
    ));

    assert!(result >= 0, "the write completed with an error: {result}");
    let Ok(written) = usize::try_from(result) else {
        panic!("a non-negative write result fits usize");
    };
    assert_eq!(
        written,
        message.len(),
        "the kernel wrote every requested byte"
    );

    let Ok(contents) = std::fs::read(&path) else {
        panic!("reading the fixture file back must succeed");
    };
    assert_eq!(
        &contents[..],
        &message[..],
        "the file holds the written bytes"
    );
}
