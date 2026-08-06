# Testing and CI

## Running the suite

```shell
just test              # nextest over the workspace, then the doc tests
just test-workspace    # nextest only
just test-doc          # doc tests only
```

Nothing in the suite needs a running service. Every test builds its own
`StorageContainer` over `memory://`, so `just test` works on a laptop with no
Docker, no minio and no `.env`.

Run one test by name:

```shell
cargo nextest run --workspace --all-features -E 'test(test_name_here)'
```

Test logs are written per crate under `logs/`, one file per test thread. The
directories are tracked (each holds a `.gitignore`) because the test harness
creates the file, not the directory, and a missing directory fails the test
rather than the write.

## Coverage

```shell
just coverage          # summary in the terminal
just coverage-html     # browsable report, opens in a browser
just coverage-ci 70    # what CI runs: lcov + HTML + summary, floored at 70%
```

All three need `cargo-llvm-cov` and the `llvm-tools-preview` component:

```shell
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
```

The `coverage` job on every PR publishes the line rate to the run summary and
attaches `lcov.info` plus the HTML report as the `coverage` artifact. It also
uploads to Codecov, but only if a `CODECOV_TOKEN` secret exists — the artifact is
the fallback and needs no third-party account.

`just coverage-ci` takes a floor and fails under it. The floor is a **ratchet**:
raise it as the real number rises, and never lower it to turn a red build green.
Lowering it is the one edit that makes the job decorative, because a change that
deletes the tests covering the code it touches is exactly what the floor is for.

Coverage is measured without `--all-targets`, unlike `test-workspace`. With it,
cargo builds the benchmark and example targets too and llvm-cov counts their
lines as uncovered source — a number that moves when you add a benchmark.

Doc tests are not counted. `cargo llvm-cov` can only instrument them on nightly,
and this workspace is pinned to a stable toolchain.

## What is not covered

**The S3 and GCS object stores.** Every test runs against `memory://`, which is
`object_store`'s `InMemory`. The design leans on conditional put — create-only,
immutable objects, multi-writer CAS — and `InMemory` emulates that locally while
S3 implements it with `If-None-Match` and GCS with a generation precondition.
Those are the semantics most worth testing and the ones nothing tests.

The CI test job used to start minio via `just ci` and set `AWS_ENDPOINT`,
`STORAGE_ENGINE=s3://tansu/` and the rest, which made it look as though the S3
path was covered. No test read any of it. The service was started, waited for and
ignored, so it was removed.

Closing this gap means parameterising the storage URL the `tansu-storage` tests
build — it is hardcoded as `memory://tansu/` in about fifty places — and giving
each test a unique prefix, since they would otherwise share one bucket and
collide on topic names. Then the suite can run twice: once on `memory://`, once
on `s3://` against minio.

**`tansu-topic`, `tansu-perf`, `tansu-otel`.** The `tansu topic` subcommand, the
produce benchmark and the OTLP exporter are at or near 0%. They are all
thin shells over code that is covered, but none of them has a test that would
notice if the shell stopped delegating.

## CI layout

`pr.yml` is the only workflow that runs on pull requests. `ci.yml` is disabled
(it is upstream's, kept for reference), and `publish.yml` runs on `v*` tags.

| job | what it establishes |
|-----|--------------------|
| `fmt` | `cargo fmt --check` |
| `clippy` | lints, and by construction type-checks everything `cargo check` would |
| `check-no-default-features` | every crate still compiles with its optional features off |
| `build-storage` | the binary links |
| `test` | nextest over the workspace, plus doc tests |
| `coverage` | line coverage, floored |
| `all-green` | one status summarising the six above |

There is no `check` job: `clippy` runs over the same `--workspace --all-features
--all-targets` selection and type-checks before it lints, so a separate
`cargo check` was a second full compile of the workspace that could not fail on
its own.

### The required check

Branch protection on `main` requires `test` and nothing else. That is why `test`
has a `needs: [fmt, clippy]` gate — a required check is the only thing branch
protection enforces, so anything not upstream of it can go red and still merge.

`all-green` exists to replace that arrangement. Point branch protection at it and
every job is enforced at once, at which point `test`'s gate can be deleted and
the tests start at T+0 instead of ~1m35s in:

```shell
gh api -X PATCH \
  repos/Popsink/tansu/branches/main/protection/required_status_checks \
  -f 'checks[][context]=all-green'
```

### Nextest profiles

`.config/nextest.toml` leaves the default profile alone — that is what `just
test` uses locally, where a contended laptop inflates wall-clock across the board
and a timeout would fire on contention rather than on a real problem.

CI selects `NEXTEST_PROFILE=ci`, which names every test over 30s on every run and
prints slow and failing tests rather than the hundreds of passing ones.

### Where the time goes

Measured on a green PR (8-core runner), `just test` is 197s:

| phase | time |
|-------|------|
| compile | 102s |
| nextest run (813 tests) | 64s |
| doc tests | 30s |

The nextest phase is already close to its floor: the slowest single test is 58s,
so the 813 tests cost 64s wall. Sharding across runners would buy at most ~35s
and cost a full compile per shard.

That 58s floor is four tests — `new_cg::consumer_next_action_{08,16,24,32}c` —
which drive a consumer group simulation through `LatencyIntroducingLayer`, i.e.
real `tokio::time::sleep` calls. `#[tokio::test(start_paused = true)]` takes
`consumer_next_action_08c` from 48s to 0.14s, but it then fails: the group
coordinator ages member sessions off `SystemTime::now()`, which does not advance
under a paused clock, so transient members are never evicted and the assertion on
a settled generation fails. Wall-clock is load-bearing there until session expiry
measures on a clock the test can move — and `SystemTime` is the deliberate choice
for state shared between stateless brokers, so that is not a small change.
