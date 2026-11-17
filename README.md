# Download Manager

A planned Rust download manager for Linux, macOS, and Windows, with adaptive
transfer tuning, a persistent background service, a minimal desktop application,
and Chromium browser integration.

**Status: command-line downloader and persistent queue worker with local IPC and restart recovery on Unix.**

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

The foreground queue worker runs independently of the CLI. Start it in one
terminal and use the same private state directory in another. On Linux the
directory must be owned by the current user with mode `0700` or stricter.

```sh
cargo run --locked -p download-managerd -- --state-dir ./queue-state work
cargo run --locked -p downloadctl -- queue --state-dir ./queue-state add https://example.com/ -o example.html
cargo run --locked -p downloadctl -- queue --state-dir ./queue-state list
```

`work --jobs 1` processes one file at a time. A submission prints its job ID.
If a submission's reply is lost, repeat the command with the original
`--idempotency-key` to retrieve its result without creating a duplicate job.
The receipt lasts seven days.

The original queue commands also support offline setup and batch runs.

```sh
cargo run --locked -p download-managerd -- --state-dir ./queue-state add https://example.com/ -o example.html
cargo run --locked -p download-managerd -- --state-dir ./queue-state work --once
cargo run --locked -p download-managerd -- --state-dir ./queue-state list
```

Queue submissions accept public HTTP(S) URLs without query strings, fragments, or
embedded credentials. The worker stores these URLs locally in SQLite and uses a
single stream per file. On Unix, transfers with a known size and a strong ETag
can resume automatically after a crash when the server confirms the saved
representation. Redirected transfers currently have no restart checkpoints. Jobs
interrupted without a usable checkpoint require an explicit `retry JOB_ID`.
Use `retry JOB_ID --restart` to start over if resume or digest validation fails.
Supply `--sha256 HEX_DIGEST` when adding a queued job to verify the complete file
before publication.
If publication encounters an existing file, move it out of the way before
retrying. The worker retains the prepared download for that retry.

On Unix, use `downloadctl queue --state-dir ./queue-state pause JOB_ID` to stop a
transfer and save a validated prefix. Use `resume JOB_ID` once the job is
`paused`. Transfers without a usable checkpoint start from the beginning.
`cancel JOB_ID` stops the job and retains its staged bytes. The command
acknowledges the stop request, and `list` shows when the worker has stopped.

The socket and recovery paths have been tested on Linux. Windows still uses the
original SQLite queue commands. Named pipes, platform qualification, adaptive
tuning, browser integration, desktop UI, installers, and updates remain planned.

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
