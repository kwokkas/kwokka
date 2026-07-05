//! `recv::<8192>` exceeds `MAX_INLINE_CAP` (4096), so the in-method `const`
//! guard fails to evaluate and the file does not compile. The call chain from
//! `main` forces `recv::<8192>` to be monomorphized, evaluating the guard.

fn main() {
    receive(sink());
}

fn receive(stream: kwokka_net::tcp::TcpStream) {
    let _future = stream.recv::<8192>();
}

fn sink() -> kwokka_net::tcp::TcpStream {
    panic!("compile-fail fixture: never constructed")
}
