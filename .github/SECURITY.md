# Security Policy

## Supported versions

kwokka is pre-1.0 and under active development. Security fixes land on
the latest 0.1.x release; older versions are not maintained.

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |
| < 0.1   | No        |

## Reporting a vulnerability

Please do not report security issues through public issues, pull
requests, or discussions.

Use GitHub's private vulnerability reporting instead: open the
[Security tab](https://github.com/kwokkas/kwokka/security/advisories) and
choose "Report a vulnerability". That opens a private advisory only the
maintainers can see.

Include enough to understand and reproduce the issue: the affected crate
and version, the platform and kernel, and a minimal example if you have
one.

## What counts as a security issue

kwokka is a completion-based runtime, so it carries `unsafe` at its FFI
boundaries (io_uring, epoll, kqueue) and in its slab, arena, and waker
internals. The reports that matter most:

- memory safety or undefined behavior (use-after-free, out-of-bounds
  access, uninitialized reads)
- unsound `unsafe` or a violated safety invariant
- data races or other concurrency unsoundness
- untrusted input that can corrupt memory

A panic or a logic bug with no soundness angle is better as a normal bug
report.

## What to expect

This is a solo-maintained project, so responses are best-effort rather
than on a fixed schedule. A confirmed issue is fixed on the latest 0.1.x
line, credited to the reporter unless you prefer otherwise, and disclosed
through a GitHub advisory once a fix is ready.
