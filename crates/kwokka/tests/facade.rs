//! End-to-end test driving the public surface through the `kwokka` facade.
//!
//! A user reaches the runtime, structured scopes, the timer, the network,
//! and the filesystem entirely through the `kwokka` crate -- never an
//! internal workspace crate. One affine runtime per binary keeps worker 0
//! uncontended, the same reason the net and fs suites run one test per
//! binary.

#![cfg(all(target_os = "linux", feature = "fs", feature = "net"))]
#![cfg(not(any(miri, loom)))]

use std::time::Duration;

use kwokka::{fs::File, net::TcpListener, runtime::Runtime, task::scope, time::sleep};

#[test]
fn the_facade_composes_runtime_net_scope_time_and_fs() {
    let Ok(exe) = std::env::current_exe() else {
        panic!("the test binary must know its own path");
    };
    let path = exe.with_extension("facade-e2e-fixture");

    let payload = b"kwokka facade end to end";
    let mut data = [0u8; 64];
    data[..payload.len()].copy_from_slice(payload);

    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };

    let outcome = runtime.block_on(async {
        // The net surface binds a listener and reports its assigned port.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let local = listener.local_addr()?;

        // A scope fans out a child that parks on the timer; the scope stays
        // pending until that child settles, proving structured concurrency
        // and the timer compose through the facade.
        let spawned = scope(|scope| {
            scope
                .spawn(async {
                    sleep(Duration::from_millis(1)).await;
                })
                .is_ok()
        })
        .await;

        // The file round trip exercises the completion futures' io::Result.
        let file = File::create(&path).await?;
        let written = file.write::<64>(0, data, payload.len()).await?;
        let file = File::open(&path).await?;
        let (read, buf) = file.read::<64>(0).await;
        let read = read?;

        Ok::<_, std::io::Error>((local, spawned, written, read, buf))
    });

    let Ok((local, spawned, written, read, buf)) = outcome else {
        panic!("the facade composition must resolve without error");
    };
    assert_ne!(local.port(), 0, "the bound listener has a concrete port");
    assert!(spawned, "the scope accepted the child task");
    assert_eq!(written, payload.len(), "the write reports every byte");
    assert_eq!(read, payload.len(), "the read returns the written length");
    assert_eq!(
        &buf[..read],
        &payload[..],
        "the facade reads back the bytes it wrote",
    );

    // IGNORE: fixture cleanup is best-effort; a leftover file in the test
    // target directory is harmless.
    let _ = std::fs::remove_file(&path);
}
