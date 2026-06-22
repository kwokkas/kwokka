<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="../../.github/images/banner-dark.png">
    <img src="../../.github/images/banner-light.png" alt="kwokka">
  </picture>
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-fs.svg)](https://crates.io/crates/kwokka-fs)
[![docs.rs](https://docs.rs/kwokka-fs/badge.svg)](https://docs.rs/kwokka-fs)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-fs

Asynchronous file I/O for kwokka, built on the completion-based I/O
driver. It provides file open, read, and write, surfaced as `kwokka::fs`.
Directories, paths, and pipes arrive in a later release.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Depend on the `kwokka` facade rather than this crate
directly.

## Example

```rust
use kwokka::fs::File;

#[kwokka::main(affine)] // or `stealing`
async fn main() -> std::io::Result<()> {
    let file = File::open("Cargo.toml").await?;
    let (read, buf) = file.read::<1024>(0).await;
    let len = read?;
    // the first `len` bytes are in `buf`
    Ok(())
}
```

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
