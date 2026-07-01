# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2](https://github.com/kwokkas/kwokka/compare/kwokka-io-v0.0.1...kwokka-io-v0.0.2) - 2026-07-01

### Bug fixes

- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- fall back to single-shot on a full registry ([#211](https://github.com/kwokkas/kwokka/pull/211))
- size the cancel inbox to every droppable op ([#209](https://github.com/kwokkas/kwokka/pull/209))
- settle a multishot task's stray in-flight op ([#208](https://github.com/kwokkas/kwokka/pull/208))
- stop arena tokens aliasing the cancel marker ([#200](https://github.com/kwokkas/kwokka/pull/200))
- [**breaking**] match the -EALREADY cancel state to its ABI ([#199](https://github.com/kwokkas/kwokka/pull/199))

### Features

- [**breaking**] build the provided-buffer recv submit path ([#216](https://github.com/kwokkas/kwokka/pull/216))
- stand up the per-worker provided-buffer ring ([#214](https://github.com/kwokkas/kwokka/pull/214))
- stream backlog accepts via multishot ([#204](https://github.com/kwokkas/kwokka/pull/204))
- hold multishot completions off the task slot ([#202](https://github.com/kwokkas/kwokka/pull/202))
- drain pending cancels to free buffered slots ([#196](https://github.com/kwokkas/kwokka/pull/196))
- make buffered futures drop-safe via the slab ([#194](https://github.com/kwokkas/kwokka/pull/194))
- wire the drop-safe buffer through the worker ([#188](https://github.com/kwokkas/kwokka/pull/188))
- hold dropped buffered-op cancels per worker ([#186](https://github.com/kwokkas/kwokka/pull/186))
- move io buffers off the future into a slab ([#184](https://github.com/kwokkas/kwokka/pull/184))
- expose io_uring kernel ops as cargo features ([#181](https://github.com/kwokkas/kwokka/pull/181))
