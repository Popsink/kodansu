# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kodansu is a stateless, Apache Kafka-compatible broker written in Rust. It is a drop-in replacement for Apache Kafka backed by an object store: S3, Google Cloud Storage, or in-memory (for tests). This fork of [tansu](https://github.com/tansu-io/tansu) is deliberately **object-store only** — the PostgreSQL/libSQL/SlateDB/Turso backends and the schema-registry validation + Iceberg/Delta/Parquet lakehouse have been removed (see #96).

The product is named **Kodansu** in documentation, but the binary, the crates and the
container image (`ghcr.io/popsink/tansu`) are all still named `tansu` — renaming those
is a separate breaking change. Use `tansu` for every code identifier and `Kodansu` in
prose.

- Rust edition 2024, toolchain pinned in `rust-toolchain.toml`
- License: Apache-2.0
- `unsafe_code` is forbidden workspace-wide

## Build & Test Commands

The project uses `just` as a task runner (loads `.env` automatically).

```shell
just                 # default: fmt, build, test, clippy
just build           # build (dev profile)
just test            # nextest + doc tests
just test-workspace  # cargo nextest run --workspace --all-targets --all-features
just test-doc        # cargo test --workspace --doc --all-features
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings
just fmt             # cargo fmt --all --check
just check           # cargo check --workspace --all-features --all-targets
just coverage        # line coverage summary (needs cargo-llvm-cov)
just coverage-html   # browsable coverage report
```

Note: the `fuzz` crate depends on a C++ libfuzzer toolchain that is not always
present locally. When building/checking the whole workspace by hand, exclude it:
`cargo check --workspace --exclude fuzz --all-targets`.

Run a single test with nextest:
```shell
cargo nextest run --workspace --all-features -E 'test(test_name_here)'
```

### Local Development Environment

```shell
cp example.env .env    # then edit .env as needed (AWS_ENDPOINT for local minio, etc.)
just ci                # starts minio via docker compose
just broker            # build + start broker with full infrastructure
just broker-memory     # broker with in-memory backend only
just broker-s3         # broker with S3/minio backend only
```

Note: when running tansu directly (not via docker compose), set `AWS_ENDPOINT="http://localhost:9000"` in `.env`.

## Architecture

Cargo workspace producing a single binary (`tansu`) with subcommands: `broker` (default), `topic`, `user`, `perf`, `proxy`.

### Key Crates

| Crate | Role |
|-------|------|
| `tansu` | Binary entry point, subcommand dispatch |
| `tansu-broker` | Kafka API broker: `Broker<G, S>` generic over Coordinator + Storage |
| `tansu-sans-io` | **Code-generated** Kafka wire protocol (pure serde, no I/O) |
| `tansu-service` | Network service layers built on `rama` (Layer/Service composition) |
| `tansu-storage` | Storage abstraction: `Box<dyn Storage>` over the object store (`DynoStore`) and the `Null` engine, selected by `StorageContainer::builder` |
| `tansu-client` | Async Kafka protocol client (rama service layers) |
| `tansu-model` | Kafka JSON protocol definitions (used in build.rs) |
| `tansu-cli` | Clap-based CLI argument parsing |
| `tansu-auth` | SASL/SCRAM authentication |
| `tansu-otel` | OTLP metric/trace export (there is no Prometheus scrape endpoint) |
| `tansu-topic` | Topic administration used by `tansu topic` |
| `tansu-perf` | Produce-side benchmark used by `tansu perf` |

### Sans-I/O Code Generation (`tansu-sans-io`)

`tansu-sans-io/build.rs` reads ~185 official Kafka JSON message descriptors from `tansu-sans-io/message/*.json` and generates typed Rust structs for every request/response pair. **Do not manually edit generated files.** The message JSON files are from upstream Apache Kafka.

### Service Layer Pattern (`tansu-service`)

Uses `rama` crate for Layer/Service composition:
- `TcpBytesLayer` (TCP) -> `BytesFrameLayer` (bytes -> Kafka Frame) -> `FrameRouteService` (route to typed handlers) -> `FrameBytesLayer` -> `BytesTcpService`
- Same layering pattern used for broker, proxy, and CLI clients

### Storage (`tansu-storage`)

Runtime dispatch through `Box<dyn Storage>`. `StorageContainer` is the builder
entry point and nothing more — the enum that used to dispatch here was never
constructed and was deleted in #279. There is a single real backend — the object
store (`DynoStore`) — plus a `Null` engine. The engine is selected from the URL
scheme:
- `memory://` - in-memory (tests / ephemeral)
- `s3://` - S3 / MinIO
- `gs://` - Google Cloud Storage

Object layout is create-only / immutable: coalesced segments carry a
self-describing footer index, and readers locate a sub-stream from the footer
alone (never from the filename). See `docs/` for the segment/coalescing and
multi-writer design notes.

### Broker Specifics

- Node ID is always **111** (single-node, stateless design - this is intentional)
- Group coordination in `tansu-broker/src/coordinator/group/`
- `EnvVarExp<T>` wrapper allows CLI args with `${VAR}` references expanded at parse time
- All `Error` types implement `Clone` (non-Clone errors wrapped in `Arc`)

## Feature Flags

Default: `dynostore` (the object store). There are no alternate-backend or
lake feature flags any more.

## Testing Notes

- Tests use `cargo-nextest` (not `cargo test` for workspace tests)
- Test logs go to `logs/<crate-name>/` (one file per test thread, dirs must exist)
- Tests load `.env` via `dotenv().ok()`
- **No test needs a running service.** Every one builds its own
  `StorageContainer` over `memory://`, so `just test` works with no Docker and no
  minio. The corollary is that the S3 and GCS object stores are untested, and
  conditional put is exactly where `InMemory` and S3 differ — see
  `docs/testing.md`.

## CI Pipeline

`pr.yml` is the only workflow that runs on pull requests: fmt, clippy,
check-no-default-features, build-storage, test and coverage in parallel, summed
up by `all-green`. `ci.yml` is disabled (upstream's, kept for reference) and
`publish.yml` pushes `ghcr.io/popsink/tansu` on `v*` tags.

There is no `check` job — `clippy` runs over the same selection and type-checks
before it lints. Branch protection still requires `test` alone, which is why
`test` has a `needs: [fmt, clippy]` gate; `docs/testing.md` has the one command
that moves it to `all-green` and lets the gate go.

There are no smoke tests. The upstream `smoke` job was gated on `github.actor == 'shortishly'`, so it never ran in this fork; #282 deleted it.

## Key Files

| File | Purpose |
|------|---------|
| `justfile` | All build/test/run tasks |
| `example.env` | Template for local `.env` config |
| `compose.yaml` | Docker Compose: minio, grafana, jaeger, prometheus |
| `tansu-sans-io/message/` | Kafka JSON protocol descriptors (upstream, ~185 files) |
| `tansu-sans-io/build.rs` | Code generator: JSON descriptors -> Rust types |

## Lint Configuration

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors).
