# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2](https://github.com/kwokkas/kwokka/compare/kwokka-io-v0.0.1...kwokka-io-v0.0.2) - 2026-07-15

### Bug fixes

- every io test now reserves its own worker id ([#315](https://github.com/kwokkas/kwokka/pull/315))
- surface a recv error end behind a filled FIFO ([#267](https://github.com/kwokkas/kwokka/pull/267))
- close the fd of a dropped single-shot accept ([#212](https://github.com/kwokkas/kwokka/pull/212))
- fall back to single-shot on a full registry ([#211](https://github.com/kwokkas/kwokka/pull/211))
- size the cancel inbox to every droppable op ([#209](https://github.com/kwokkas/kwokka/pull/209))
- settle a multishot task's stray in-flight op ([#208](https://github.com/kwokkas/kwokka/pull/208))
- stop arena tokens aliasing the cancel marker ([#200](https://github.com/kwokkas/kwokka/pull/200))
- [**breaking**] match the -EALREADY cancel state to its ABI ([#199](https://github.com/kwokkas/kwokka/pull/199))

### Features

- real readv and writev over the inflight slot ([#316](https://github.com/kwokkas/kwokka/pull/316))
- send and receive datagrams through io_uring ([#272](https://github.com/kwokkas/kwokka/pull/272))
- connect a client TcpStream to a peer address ([#269](https://github.com/kwokkas/kwokka/pull/269))
- retry a refused zero-copy send as plain send ([#266](https://github.com/kwokkas/kwokka/pull/266))
- expose the owned-buffer recv and send on tcp ([#265](https://github.com/kwokkas/kwokka/pull/265))
- run the zero-copy send on the buffer traits ([#263](https://github.com/kwokkas/kwokka/pull/263))
- back the file futures with owned io buffers ([#259](https://github.com/kwokkas/kwokka/pull/259))
- put owned buffers behind the socket futures ([#257](https://github.com/kwokkas/kwokka/pull/257))
- carry the in-flight copy onto the io buffers ([#255](https://github.com/kwokkas/kwokka/pull/255))
- stop a dropped connect from waking its task ([#252](https://github.com/kwokkas/kwokka/pull/252))
- bound an io op with a native kernel deadline ([#248](https://github.com/kwokkas/kwokka/pull/248))
- introduce the io-side msg_ring wake surface ([#241](https://github.com/kwokkas/kwokka/pull/241))
- land the zero-copy send future on the stream ([#239](https://github.com/kwokkas/kwokka/pull/239))
- submit send-zc requests through the op path ([#235](https://github.com/kwokkas/kwokka/pull/235))
- guard send-zc buffers until the notif lands ([#233](https://github.com/kwokkas/kwokka/pull/233))
- receive a stream of provided buffers on tcp ([#231](https://github.com/kwokkas/kwokka/pull/231))
- drain multishot recv completions per worker ([#228](https://github.com/kwokkas/kwokka/pull/228))
- drive multishot recv through its submit path ([#226](https://github.com/kwokkas/kwokka/pull/226))
- lay down the multishot recv completion store ([#224](https://github.com/kwokkas/kwokka/pull/224))
- borrow kernel-picked recv buffers zero-copy ([#218](https://github.com/kwokkas/kwokka/pull/218))
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

### Refactor

- split the io seam by what each part does ([#303](https://github.com/kwokkas/kwokka/pull/303))
- give each io request family its own file ([#299](https://github.com/kwokkas/kwokka/pull/299))
- give the uring files a domain directory ([#296](https://github.com/kwokkas/kwokka/pull/296))
- give every io buffer file a domain home ([#294](https://github.com/kwokkas/kwokka/pull/294))
- carve the cancel axis out of the io seam ([#280](https://github.com/kwokkas/kwokka/pull/280))
- give kwokka-io a driver module directory ([#278](https://github.com/kwokkas/kwokka/pull/278))
