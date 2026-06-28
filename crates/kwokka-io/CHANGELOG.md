# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2](https://github.com/kwokkas/kwokka/compare/kwokka-io-v0.0.1...kwokka-io-v0.0.2) - 2026-06-28

### Features

- wire the drop-safe buffer through the worker ([#188](https://github.com/kwokkas/kwokka/pull/188))
- hold dropped buffered-op cancels per worker ([#186](https://github.com/kwokkas/kwokka/pull/186))
- move io buffers off the future into a slab ([#184](https://github.com/kwokkas/kwokka/pull/184))
- expose io_uring kernel ops as cargo features ([#181](https://github.com/kwokkas/kwokka/pull/181))
