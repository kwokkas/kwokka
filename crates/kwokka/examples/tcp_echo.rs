//! Server-side TCP echo flow through the facade: bind, accept, recv, send.
//!
//! Single-threaded by design -- no peer thread (`std::thread::spawn` is banned).
//! The flow compiles and type-checks without a live client; running it would
//! block on accept until a connection arrives. Gated on the `net` feature.

use kwokka::net::TcpListener;
use kwokka::runtime::Runtime;

fn main() -> std::io::Result<()> {
    let mut runtime = Runtime::affine()?;
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let stream = listener.accept().await?;

        // recv resolves to a byte count paired with the filled buffer; send
        // echoes the received prefix back and resolves to a byte count.
        let (received, buf) = stream.recv::<64>().await;
        let received = received?;
        let _sent = stream.send::<64>(buf, received).await?;

        Ok::<(), std::io::Error>(())
    })
}
