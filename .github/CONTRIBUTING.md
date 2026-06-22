# Contributing to kwokka

Thanks for your interest in kwokka. It is an early-stage project (0.1.x),
so the most useful help right now is bug reports, small focused fixes,
and discussion about where the API should go.

## Before you start

kwokka is maintained by one person for now, and the public API is still
settling ahead of 1.0. For anything past a small fix, please open an
issue first so we can agree on the approach before you write code. It
saves you from building something that does not fit the design.

- Bug reports and feature ideas go through the
  [issue tracker](https://github.com/kwokkas/kwokka/issues/new/choose).
  For a feature or an API change, open the issue first.
- Security issues do not belong in public issues. See
  [SECURITY.md](SECURITY.md).

## Development

kwokka targets Rust 1.85.0 on edition 2024. The primary platform is
Linux with io_uring; epoll and kqueue are the fallbacks.

Build and test the workspace:

```bash
cargo build --workspace
cargo test --workspace
```

Before you open a PR, run what CI runs:

```bash
cargo +nightly fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
```

The format check needs nightly because the rustfmt config uses
nightly-only options.

## Pull requests

- Keep each PR to one logical change. Small PRs are easier to review and
  land sooner.
- Give the PR a [conventional-commit](https://www.conventionalcommits.org)
  title, such as `fix: handle short reads in the recv future`.
- Fill in the template: what the change completes, and how it is proven.
  New `unsafe` carries a `// SAFETY:` comment, and a new concurrency
  primitive comes with a loom model or a note on why it does not need one.

## License

kwokka is dual licensed under Apache-2.0 and MIT. By contributing, you
agree that your work is licensed under both, as described in the
Apache-2.0 license.

## Code of conduct

Taking part in kwokka means following the
[Contributor Covenant](CODE_OF_CONDUCT.md).
