# Download Manager

A planned Rust download manager for Linux, macOS, and Windows, with adaptive
transfer tuning, a persistent background service, a minimal desktop application,
and Chromium browser integration.

**Status: initial command-line downloader.**

The Rust CLI streams HTTP/HTTPS downloads to explicit output paths. It stages
each file beside its destination, syncs the data, and publishes without
overwriting an existing file. Publication requires a filesystem that supports
hard links. Completed files survive ordinary CLI errors on other jobs.

```sh
cargo run --locked -p downloadctl -- https://example.com/ -o example.html
cargo run --locked -p downloadctl -- --jobs 1 \
    https://example.com/ https://example.org/ -o example.html -o example-org.html
cargo test --locked --workspace
```

The default is up to three active files. Use `--jobs 1` for sequential transfers.
Each file currently uses one request at a time. Persistent queues, resume,
adaptive range transfers, the service, browser integration, desktop UI, and
installers and updates remain planned features. The CLI has been tested on Linux.

The [implementation plan](docs/implementation-plan.md) defines the architecture,
installation modes, browser limitations, adaptive algorithm, release milestones,
commit sequence, performance benchmarks, and 90% unit-test coverage policy.

The [technical design](docs/technical-design.md) projects that plan into component
contracts, IPC examples, database schema, transfer and scheduling algorithms,
browser handoff recovery, platform installation, and transactional updates.

The native application will use Rust. The Chromium Manifest V3 extension will
use a small TypeScript adapter for browser APIs. Downloads will continue while
the desktop application is closed.

The first release targets HTTP/HTTPS downloads. Browser-only installation will
provide a reduced browser-managed mode; native acceleration requires the service.

Project name, distribution identity, license, and publishing accounts are to be
finalized before public distribution. This repository has not been published.
