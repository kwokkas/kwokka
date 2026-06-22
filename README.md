# Kwokka

A completion-based async runtime for Rust.

Kwokka submits I/O operations to the kernel and reacts when they
complete, instead of polling file descriptors for readiness. On Linux
that maps directly onto io_uring. An epoll fallback and a kqueue
backend for macOS and BSD expose the same completion API everywhere
else.

> [!IMPORTANT]
> Early development (0.1.0). The runtime, structured concurrency, TCP,
> and file I/O below are implemented. The public API is settled but
> pre-1.0 and can still change. Orchestration and Tokio compatibility
> are planned for later releases.

## Design

Four rules the codebase holds itself to:

- Completion-based I/O at the core. The readiness backends, epoll and
  kqueue, are adapted to completion internally, so user code sees one
  model on every platform.
- Zero-cost abstraction. Dispatch is enum-based. There are no trait
  objects and no vtables in the runtime.
- Zero-copy data flow. I/O runs through pinned inline buffers that the
  operation's future owns, with no per-call heap allocation. Bytes are
  copied only when they cross a real ownership boundary.
- Index-based ownership. Tasks live in per-worker generational slabs
  and are addressed by index rather than reference counting.

## Scheduler modes

There are two scheduler modes, and you pick one explicitly:
`#[kwokka::main]` takes the choice as a bare scheduler argument, and
there is no default. A phantom `Mode` type parameter (`Affine` or
`Stealing`) turns a cross-mode mistake into a compile error rather than
a runtime panic.

| Argument   | Scheduler       | Tasks                                 |
| ---------- | --------------- | ------------------------------------- |
| `affine`   | thread-per-core | `!Send`, pinned to the calling thread |
| `stealing` | work-stealing   | `Send`, relocated toward idle workers |

Work-stealing migration is gated on the default-off `stealing` cargo
feature. Without it `stealing` mode runs but does not migrate tasks
across workers.

```rust
use kwokka::fs::File;

#[kwokka::main(affine)]
async fn main() -> std::io::Result<()> {
    let file = File::open("Cargo.toml").await?;
    let (read, buf) = file.read::<1024>(0).await;
    let read = read?;
    // the first `read` bytes are now in `buf`
    Ok(())
}
```

To embed the runtime instead of using the attribute, build
`Runtime::affine()` or `Runtime::stealing()` and drive it with
`block_on`. `RuntimeBuilder` sets custom worker capacities.

## Structured concurrency

Tasks fan out through scopes rather than a free-standing spawn, so
every child settles before its scope resolves. `task::scope` runs its
children on the affine worker. `task::scope_send` is the `Send`-bounded
twin whose children may migrate across the stealing crew.
`task::yield_now` hands the worker back to the scheduler for one pass.

A `scope_send` fans a batch of Send tasks across the stealing crew, and
the scope resolves once they all finish:

```rust
use kwokka::task::scope_send;

#[kwokka::main(stealing)]
async fn main() {
    // fan out eight Send tasks for the crew to relocate toward idle cores
    scope_send(|crew| {
        for _ in 0..8 {
            crew.spawn(async {
                // a unit of work
            }).ok();
        }
    })
    .await;
}
```

## Network and filesystem

Endpoints are feature-gated behind `net`, `fs`, or `full`, so a minimal
build pulls in neither. Reads and writes run through the stream's own
pinned inline buffer of const-generic size, with no heap allocation.

`net::TcpListener` binds a port and accepts connections. The accepted
`net::TcpStream` converses through `recv` and `send`. A client-side
connect arrives in a later release.

`fs::File` opens a handle and reads or writes it at an offset. Opening
is async-shaped over a one-shot blocking syscall for now. The
ring-lowered open swaps in later with no change a caller can see.

## Minimum supported Rust version

Rust 1.85.0, edition 2024.

## License

Licensed under either the Apache License 2.0 or the MIT license, at
your option.
