# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/kwokkas/kwokka/compare/kwokka-runtime-v0.0.2...kwokka-runtime-v0.0.3) - 2026-07-15

### Bug fixes

- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- settle a multishot task's stray in-flight op ([#208](https://github.com/kwokkas/kwokka/pull/208))
- stop arena tokens aliasing the cancel marker ([#200](https://github.com/kwokkas/kwokka/pull/200))

### Features

- stop a dropped connect from waking its task ([#252](https://github.com/kwokkas/kwokka/pull/252))
- bound an io op with a native kernel deadline ([#248](https://github.com/kwokkas/kwokka/pull/248))
- send a msg_ring wake from the source worker ([#245](https://github.com/kwokkas/kwokka/pull/245))
- publish the worker ring fd for msg_ring wake ([#243](https://github.com/kwokkas/kwokka/pull/243))
- reclaim the send-zc slot on its notification ([#237](https://github.com/kwokkas/kwokka/pull/237))
- drain multishot recv completions per worker ([#228](https://github.com/kwokkas/kwokka/pull/228))
- lay down the multishot recv completion store ([#224](https://github.com/kwokkas/kwokka/pull/224))
- borrow kernel-picked recv buffers zero-copy ([#218](https://github.com/kwokkas/kwokka/pull/218))
- stream backlog accepts via multishot ([#204](https://github.com/kwokkas/kwokka/pull/204))
- hold multishot completions off the task slot ([#202](https://github.com/kwokkas/kwokka/pull/202))
- drain pending cancels to free buffered slots ([#196](https://github.com/kwokkas/kwokka/pull/196))
- wire the drop-safe buffer through the worker ([#188](https://github.com/kwokkas/kwokka/pull/188))

### Refactor

- build, crew, and drive replace ten files ([#312](https://github.com/kwokkas/kwokka/pull/312))
- the origin store both sides reach for ([#311](https://github.com/kwokkas/kwokka/pull/311))
- one pass, and the drains it calls out to ([#310](https://github.com/kwokkas/kwokka/pull/310))
- routing a stale handle is not the move ([#309](https://github.com/kwokkas/kwokka/pull/309))
- who knows the future type and who cannot ([#308](https://github.com/kwokkas/kwokka/pull/308))
- the last mixed directory in the runtime ([#307](https://github.com/kwokkas/kwokka/pull/307))
- the mode markers are not a join concern ([#306](https://github.com/kwokkas/kwokka/pull/306))
- the scheduler keeps only what schedules ([#305](https://github.com/kwokkas/kwokka/pull/305))
- land the timer inbox among worker queues ([#304](https://github.com/kwokkas/kwokka/pull/304))
- give every io buffer file a domain home ([#294](https://github.com/kwokkas/kwokka/pull/294))
- give the core and ir modules a code leaf ([#288](https://github.com/kwokkas/kwokka/pull/288))

## [0.0.2](https://github.com/kwokkas/kwokka/compare/kwokka-runtime-v0.0.1...kwokka-runtime-v0.0.2) - 2026-06-24

### Refactor

- extract the crew and relocate the probe ([#138](https://github.com/kwokkas/kwokka/pull/138))
- regroup the runtime module tree ([#136](https://github.com/kwokkas/kwokka/pull/136))

### Testing

- prove flat pip issuance stays unique ([#143](https://github.com/kwokkas/kwokka/pull/143))
