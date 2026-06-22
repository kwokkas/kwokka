<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="../../.github/images/banner-dark.png">
    <img src="../../.github/images/banner-light.png" alt="kwokka">
  </picture>
</p>

[![crates.io](https://img.shields.io/crates/v/kwokka-net.svg)](https://crates.io/crates/kwokka-net)
[![docs.rs](https://docs.rs/kwokka-net/badge.svg)](https://docs.rs/kwokka-net)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# kwokka-net

TCP networking for kwokka, built on the completion-based I/O driver. It
provides the listener and stream that surface as `kwokka::net`. UDP and
Unix sockets arrive in a later release.

This is an internal crate of the [`kwokka`](https://crates.io/crates/kwokka)
async framework. Depend on the `kwokka` facade rather than this crate
directly.

## Example

```rust
use kwokka::net::TcpListener;

#[kwokka::main(affine)] // or `stealing`
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    let stream = listener.accept().await?;

    let (read, buf) = stream.recv::<1024>().await;
    let len = read?;
    stream.send(buf, len).await?;

    Ok(())
}
```

## License

Licensed under either of Apache License 2.0
([LICENSE-APACHE](LICENSE-APACHE)) or the MIT license
([LICENSE-MIT](LICENSE-MIT)), at your option.
