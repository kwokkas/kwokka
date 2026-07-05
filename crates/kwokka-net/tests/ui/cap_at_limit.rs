//! `recv::<4096>` sits exactly at `MAX_INLINE_CAP`, so the `const` guard accepts
//! it and the file compiles. The `black_box(false)` branch forces `recv::<4096>`
//! to be monomorphized -- evaluating the guard at the ceiling -- without running
//! it, so a `<` off-by-one in the guard would fail this case.

fn main() {
    if std::hint::black_box(false) {
        receive(sink());
    }
}

fn receive(stream: kwokka_net::tcp::TcpStream) {
    let _future = stream.recv::<4096>();
}

fn sink() -> kwokka_net::tcp::TcpStream {
    panic!("boundary fixture: never constructed")
}
