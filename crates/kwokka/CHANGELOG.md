# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/kwokkas/kwokka/compare/kwokka-v0.1.1...kwokka-v0.2.0) - 2026-07-15

### Features

- open Unix domain stream sockets on io_uring ([#276](https://github.com/kwokkas/kwokka/pull/276))
- surface a UdpSocket over the datagram ops ([#274](https://github.com/kwokkas/kwokka/pull/274))
- expose the owned-buffer recv and send on tcp ([#265](https://github.com/kwokkas/kwokka/pull/265))
- [**breaking**] hide the file future types from the surface ([#261](https://github.com/kwokkas/kwokka/pull/261))
- land the zero-copy send future on the stream ([#239](https://github.com/kwokkas/kwokka/pull/239))
- reach the zero-copy recv from the tcp stream ([#222](https://github.com/kwokkas/kwokka/pull/222))
- [**breaking**] strip the named futures off the net surface ([#220](https://github.com/kwokkas/kwokka/pull/220))

### Refactor

- give every crate root a lib.rs-only tree ([#286](https://github.com/kwokkas/kwokka/pull/286))

## [0.1.1](https://github.com/kwokkas/kwokka/compare/kwokka-v0.1.0...kwokka-v0.1.1) - 2026-06-25

### Documentation

- align the readme intro and features to 0.1.0 ([#159](https://github.com/kwokkas/kwokka/pull/159))

## [0.1.0](https://github.com/kwokkas/kwokka/compare/kwokka-v0.1.0-rc.1...kwokka-v0.1.0) - 2026-06-24

### Build

- cut the facade to 0.1.0 and arm release-plz ([#146](https://github.com/kwokkas/kwokka/pull/146))

### Refactor

- drop Pip from the public facade surface ([#141](https://github.com/kwokkas/kwokka/pull/141))
- regroup the runtime module tree ([#136](https://github.com/kwokkas/kwokka/pull/136))
