//! Opening a file and reading a few bytes through the facade.
//!
//! Reads this crate's own manifest from offset zero. Gated on the `fs`
//! feature. The read future resolves to a byte count paired with the buffer.

use kwokka::{fs::File, runtime::Runtime};

fn main() -> std::io::Result<()> {
    let mut runtime = Runtime::affine()?;
    runtime.block_on(async {
        let file = File::open("Cargo.toml").await?;

        // read resolves to (io::Result<usize>, [u8; CAP]); the prefix up to
        // the returned count holds the bytes the kernel delivered.
        let (read, buf) = file.read::<64>(0).await;
        let read = read?;
        core::hint::black_box(&buf[..read]);

        Ok::<(), std::io::Error>(())
    })
}
