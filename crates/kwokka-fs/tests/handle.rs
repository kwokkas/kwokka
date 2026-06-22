//! End-to-end file conversation through the owned handle.
//!
//! Creates a file through [`File::create`], writes a payload through the
//! handle's write future on the real `io_uring` ring, reopens it through
//! [`File::open`], and reads the payload back through the read future --
//! the open-to-write-to-read loop the handle exists for.
//!
//! [`File::create`]: kwokka_fs::file::File::create
//! [`File::open`]: kwokka_fs::file::File::open

#![cfg(target_os = "linux")]
#![cfg(not(any(miri, loom)))]

use kwokka_fs::file::File;
use kwokka_runtime::Runtime;

#[test]
fn a_file_round_trips_through_the_handle() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary must know its own path");
    };
    let path = exe.with_extension("handle-fixture");

    let payload = b"kwokka file converses";
    let mut data = [0u8; 64];
    data[..payload.len()].copy_from_slice(payload);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };

    let written = runtime.block_on(async {
        let file = File::create(&path).await?;
        file.write::<64>(0, data, payload.len()).await
    });
    let Ok(written) = written else {
        panic!("the create-and-write must resolve with a byte count");
    };
    assert_eq!(written, payload.len(), "the kernel wrote every byte");

    let read_back = runtime.block_on(async {
        let file = File::open(&path).await?;
        let (received, buf) = file.read::<64>(0).await;
        Ok::<(usize, [u8; 64]), std::io::Error>((received?, buf))
    });
    let Ok((received, buf)) = read_back else {
        panic!("the reopen-and-read must resolve with a byte count");
    };
    assert_eq!(
        &buf[..received],
        &payload[..],
        "the handle reads back the bytes it wrote",
    );

    // IGNORE: fixture cleanup is best-effort; a leftover file in the test
    // target directory is harmless.
    let _ = std::fs::remove_file(&path);
}
