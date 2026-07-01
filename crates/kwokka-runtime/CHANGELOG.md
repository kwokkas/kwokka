# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/kwokkas/kwokka/compare/kwokka-runtime-v0.0.2...kwokka-runtime-v0.0.3) - 2026-07-01

### Bug fixes

- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- settle a multishot task's stray in-flight op ([#208](https://github.com/kwokkas/kwokka/pull/208))
- stop arena tokens aliasing the cancel marker ([#200](https://github.com/kwokkas/kwokka/pull/200))

### Features

- stream backlog accepts via multishot ([#204](https://github.com/kwokkas/kwokka/pull/204))
- hold multishot completions off the task slot ([#202](https://github.com/kwokkas/kwokka/pull/202))
- drain pending cancels to free buffered slots ([#196](https://github.com/kwokkas/kwokka/pull/196))
- wire the drop-safe buffer through the worker ([#188](https://github.com/kwokkas/kwokka/pull/188))

## [0.0.2](https://github.com/kwokkas/kwokka/compare/kwokka-runtime-v0.0.1...kwokka-runtime-v0.0.2) - 2026-06-24

### Refactor

- extract the crew and relocate the probe ([#138](https://github.com/kwokkas/kwokka/pull/138))
- regroup the runtime module tree ([#136](https://github.com/kwokkas/kwokka/pull/136))

### Testing

- prove flat pip issuance stays unique ([#143](https://github.com/kwokkas/kwokka/pull/143))
