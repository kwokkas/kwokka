# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/kwokkas/kwokka/compare/kwokka-fs-v0.0.2...kwokka-fs-v0.0.3) - 2026-07-15

### Features

- real readv and writev over the inflight slot ([#316](https://github.com/kwokkas/kwokka/pull/316))
- [**breaking**] hide the file future types from the surface ([#261](https://github.com/kwokkas/kwokka/pull/261))
- back the file futures with owned io buffers ([#259](https://github.com/kwokkas/kwokka/pull/259))
