# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/kwokkas/kwokka/compare/kwokka-net-v0.0.2...kwokka-net-v0.0.3) - 2026-07-01

### Bug fixes

- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- fall back to single-shot on a full registry ([#211](https://github.com/kwokkas/kwokka/pull/211))

### Features

- stream backlog accepts via multishot ([#204](https://github.com/kwokkas/kwokka/pull/204))
