# Download Manager

A planned Rust download manager for Linux, macOS, and Windows, with adaptive
transfer tuning, a persistent background service, a minimal desktop application,
and Chromium browser integration.

**Status: command-line downloader and persistent queue worker.**

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
Each file uses one request by default. For large files from URLs that can be
safely requested again, use `--ranges 4` to allow up to four concurrent byte ranges
per file. The engine requires a strong ETag and validates every partial response
before publication. If the initial range request returns a full response, the
engine replaces its staged prefix with that full response.

The foreground queue worker runs independently of submission commands. Use the
same private state directory for each invocation.

```sh
cargo run --locked -p download-managerd -- --state-dir ./queue-state add https://example.com/ -o example.html
cargo run --locked -p download-managerd -- --state-dir ./queue-state work --once
cargo run --locked -p download-managerd -- --state-dir ./queue-state list
```

Queue submissions accept public URLs without query strings or embedded credentials
and store those URLs locally in SQLite. Keep the worker running by omitting
`--once`. Use `work --jobs 1` for sequential processing. If a worker exits during
a job, its successor marks the job interrupted. Use `retry JOB_ID` to requeue it
from the beginning after checking the destination. The worker currently uses
single-stream transfers and queue clients access the local database directly.

Restart resume, the service IPC protocol, adaptive tuning, browser integration,
desktop UI, installers, and updates remain planned. The CLI and worker have
been tested on Linux.

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
