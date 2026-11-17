# Download Manager

A planned Rust download manager for Linux, macOS, and Windows, with adaptive
transfer tuning, a persistent background service, a minimal desktop application,
and Chromium browser integration.

**Status: implementation planning. No application code exists yet.**

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
