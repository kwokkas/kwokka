# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/kwokkas/kwokka/compare/kwokka-net-v0.0.2...kwokka-net-v0.0.3) - 2026-07-15

### Bug fixes

- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- fall back to single-shot on a full registry ([#211](https://github.com/kwokkas/kwokka/pull/211))

### Features

- open Unix domain stream sockets on io_uring ([#276](https://github.com/kwokkas/kwokka/pull/276))
- surface a UdpSocket over the datagram ops ([#274](https://github.com/kwokkas/kwokka/pull/274))
- connect a client TcpStream to a peer address ([#269](https://github.com/kwokkas/kwokka/pull/269))
- retry a refused zero-copy send as plain send ([#266](https://github.com/kwokkas/kwokka/pull/266))
- expose the owned-buffer recv and send on tcp ([#265](https://github.com/kwokkas/kwokka/pull/265))
- run the zero-copy send on the buffer traits ([#263](https://github.com/kwokkas/kwokka/pull/263))
- put owned buffers behind the socket futures ([#257](https://github.com/kwokkas/kwokka/pull/257))
- stop a dropped connect from waking its task ([#252](https://github.com/kwokkas/kwokka/pull/252))
- thread a native deadline through tcp connect ([#250](https://github.com/kwokkas/kwokka/pull/250))
- land the zero-copy send future on the stream ([#239](https://github.com/kwokkas/kwokka/pull/239))
- receive a stream of provided buffers on tcp ([#231](https://github.com/kwokkas/kwokka/pull/231))
- reach the zero-copy recv from the tcp stream ([#222](https://github.com/kwokkas/kwokka/pull/222))
- [**breaking**] strip the named futures off the net surface ([#220](https://github.com/kwokkas/kwokka/pull/220))
- stream backlog accepts via multishot ([#204](https://github.com/kwokkas/kwokka/pull/204))
