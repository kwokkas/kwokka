# Contributing to kwokka

Thanks for your interest in kwokka. It is an early-stage project (0.1.x),
so the most useful help right now is bug reports, small focused fixes,
and discussion about where the API should go.

> [!IMPORTANT]
> kwokka officially allows AI-assisted contributions. Tools that generate
> or help write code are fine to use. What is not optional is that you,
> the author, understand the code you submit and can explain how it works
> and why it is correct. You are responsible for every line in your patch
> as if you had written it by hand: its correctness, its license
> compatibility (a tool may reproduce code under a license that is not
> compatible with Apache-2.0 or MIT, and catching that is on you), and how
> it fits the design. A patch the author cannot explain will not be merged.

## Before you start

kwokka is maintained by one person for now, and the public API is still
settling ahead of 1.0. For anything past a small fix, please open an
issue first so we can agree on the approach before you write code. It
saves you from building something that does not fit the design.

- Bug reports and feature ideas go through the
  [issue tracker](https://github.com/kwokkas/kwokka/issues/new/choose).
  Use the bug report or feature request template. The tracking and work
  item templates are for maintainers.
- For a feature or an API change, open the issue first.
- Security issues do not belong in public issues. See
  [SECURITY.md](SECURITY.md).

## Development

kwokka targets Rust 1.85.0 on edition 2024. The primary platform is Linux
with io_uring; epoll and kqueue are the fallbacks. CI checks formatting,
clippy with warnings denied, the test suite, and the docs, so it pays to
run those locally before you push.

## Pull requests

Keep each PR to one logical change. Smaller PRs are easier to review and
land sooner.

**Title.** The PR title follows [conventional commits](https://www.conventionalcommits.org):
a `type:` prefix and a short summary, with no scope in parentheses and 50
characters or fewer. For example:

```
fix: handle short reads in the recv future
```

Common types are `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, and
`chore`.

**Commits.** Commit messages inside the PR are your call. A title-only
commit is fine, and a body or footer is welcome wherever it adds context.
The PR description is what gets reviewed, so the reasoning belongs there.

**Description.** Fill in the PR template: what the change completes, and
how it is proven. A pull request that does not follow the template will
not be accepted. New `unsafe` carries a `// SAFETY:` comment, and a new
concurrency primitive comes with a loom model or a note on why it does
not need one.

## License

kwokka is dual licensed under Apache-2.0 and MIT. By contributing, you
agree that your work is licensed under both, as described in the
Apache-2.0 license.

## Code of conduct

Taking part in kwokka means following the
[Contributor Covenant](CODE_OF_CONDUCT.md).
