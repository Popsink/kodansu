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

## Running the storage suite against a real object store

`memory://` is the default, not the only option. Every test in `tansu-storage`
builds its store from `TANSU_TEST_STORAGE_URL`, which defaults to
`memory://tansu/` (#357):

```shell
just test-storage                          # memory://tansu/, same as just test
just test-storage-minio                    # starts minio in compose, runs on s3://tansu/
just test-storage s3://my-bucket/          # any bucket, credentials from the AWS_* env
just test-conditional-put s3://tansu/      # only the conformance target
```

Two helpers in `tansu-storage/tests/common/mod.rs` make that work, and a new test
should use both rather than hardcoding a URL:

| helper | what it gives |
|---|---|
| `storage_url()` / `storage_url_with_query(q)` | the configured URL, with `q` merged into whatever query it already carries |
| `cluster_id()` | a fresh uuid per `build()` |

`cluster_id()` is the isolation. Every object the engine writes is keyed under
`clusters/{cluster}/`, so the cluster id *is* the per-test prefix — without it,
two tests that both create a topic called `pqr` are the same object in a shared
bucket. One id per `build()` and not per test, because that is exactly the
isolation `InMemory` gives today: a test that builds two containers keeps seeing
two unrelated stores.

`TANSU_TEST_STORAGE_URL` is deliberately not `STORAGE_ENGINE`. That is the
broker's own variable, `example.env` sets it to `s3://tansu/`, and the tests load
`.env` — so reusing the name would silently point the suite at a store that is
not running.

Running against minio by hand needs `AWS_ENDPOINT="http://localhost:9000"` in
`.env` (`example.env` ships the in-compose hostname, which does not resolve from
the host). `just test-storage-minio` overrides it for you.

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

## Conditional put

Everything that makes the broker stateless is a conditional put: offset
assignment is a create-only segment-sequence CAS, a group's composition is the
etag CAS in `update_group_generation` and its assignment a create-only write,
maintenance work-splitting is a per-prefix lease. `InMemory`
*emulates* those semantics behind a mutex; S3 implements them with
`If-None-Match`. `tansu-storage/tests/conditional_put.rs` is the conformance
target that pins them, and it runs against whichever store
`TANSU_TEST_STORAGE_URL` names.

Verified green on both `memory://` and `s3://` against minio (`object_store`
0.14.1, `quay.io/minio/minio`):

| property | how it is pinned |
|---|---|
| create-only | 16 concurrent creators of one key: exactly one wins, every loser is `AlreadyExists` |
| etag CAS | 16 writers race one version: one wins, every loser is `Precondition`, and the spent version stays spent |
| CAS is not a create | an invented etag, and a CAS against an absent key, are both `Precondition` — never a silent write |
| etag stability | an unchanged object keeps its etag across repeated `head`/`get` and across unrelated writes in the same prefix, and a CAS against it still succeeds (the #111 GET-first skip depends on this) |
| conditional read | `If-None-Match` is `NotModified` on an unchanged object and returns the body once it changes |
| `UpdateError::Outdated` | a stale `update_group_generation` carries the value that won and its version, which is what the coordinator re-derives from rather than retrying blindly (#157) |

The whole `tansu-storage` suite — 324 tests, not just the conformance target —
also passes on `s3://` against minio, twice in a row against an already-populated
bucket, which is what establishes that the per-test prefixing actually isolates.

### One divergence between `InMemory` and S3

`InMemory`'s etag is a per-object **write counter**; S3's is the **MD5 of the
body**. Measured:

| | rewrite the same bytes | two keys, same bytes |
|---|---|---|
| `InMemory` | `"2"` becomes `"3"` | `"2"` vs `"4"` |
| S3 (minio) | `ee0cbdba…` stays `ee0cbdba…` | equal |

So on S3 an etag is content-addressed, and "the etag changed" is not evidence
that a write happened. The consequence is ABA: an object driven X to Y and back
to a byte-identical X presents its original etag again, and a CAS still holding
that etag succeeds despite two intervening writes, where `InMemory` would refuse.

Nothing in the engine depends on the difference today — `GroupDetail` carries
`inception` and `generation_id`, so byte-identical group state is not a state the
coordinator returns to — and the conformance target asserts the invariant both
stores do honour: the version a put hands back is usable for the next CAS
whichever way the store derives it. It is written down here because the
group-state decomposition adds per-generation CAS'd objects, and a small
`{generation, leader}` object is much likelier to cycle back to a previous
byte-identical value than `GroupDetail` is.

## Group scale

`just test-group-scale` drives `GROUPS × MEMBERS` consumers through the full
KIP-394 dance across `REPLICAS` coordinators over one shared store, and asserts
convergence, a formation write budget, and a **steady-state window that costs
nothing**. It is the exit criterion for the group-state decomposition (#359) —
`cg_forward` proves *one* group converges, and the deployment that wedged had
many.

`#[ignore]`d in the suite: it is wall clock rather than a regression gate, and
`.github/workflows/storage.yml` runs it nightly. The size is environment-driven,
so a laptop can run a slice of production:

```shell
TANSU_SCALE_GROUPS=8 just test-group-scale
```

| Variable | Default | |
|---|---|---|
| `TANSU_SCALE_GROUPS` | 64 | |
| `TANSU_SCALE_MEMBERS` | 16 | the group size that never converged |
| `TANSU_SCALE_REPLICAS` | 10 | the deployment's broker count |
| `TANSU_SCALE_FORWARDING` | true | false runs the same consumers with no owner replica |
| `TANSU_SCALE_PUT_BUDGET_PER_MEMBER` | 6 | writes of `generation.json` a group may cost to form |
| `TANSU_SCALE_DEADLINE_SECS` | 120 | per-member, only reached on failure |

The object the budget counts is `generation.json`, and it counts **attempts**:
a conditional PUT the store rejects on its precondition is still a request it
charged for. `{group}.json` is not written at all any more, so counting it would
be asserting zero of nothing.

Measured at the default size, both arrangements converging 1024 members:

| | formation attempts | landed | steady-state writes |
|---|---|---|---|
| `TANSU_SCALE_FORWARDING=true` | ~1100 | 1024 | 0 |
| `TANSU_SCALE_FORWARDING=false` | ~3830 | 1024 | 0 |

1024 landed writes is 64 groups × 16 members — one CAS per member, which is the
floor for admitting them one at a time. Scattered, the members race for those
1024 slots and are rejected ~2800 times on the way; forwarded, the owner
serializes them in-process and almost none are. **Formation conflicts are
budgeted rather than forbidden**: N members racing to add themselves to one
document is a race, and the CAS is what resolves it.

What is asserted as an exact zero is the *steady state*. After convergence every
member heartbeats several times from the replica it entered through, and across
all 64 groups that must produce no write of `generation.json` beyond the sweep's
own stamp, no CAS conflict, and no listing of member documents. That is the
regime a deployment lives in, and it is the property that used to require an
owner: `TANSU_SCALE_FORWARDING=false` is the arrangement #360 wants to make the
only one, and it now holds all three assertions.

## What is not covered

**GCS.** `gs://` is a supported target and every test in the conformance target
would run against it unchanged — that is the whole point of the URL being a
parameter — but none of them ever has. There is no GCS emulator in
`compose.yaml` and `GoogleCloudStorageBuilder::from_env` needs real credentials,
so generation preconditions remain assumed rather than observed. Nothing fakes
them: there is no GCS test rather than a GCS test that proves nothing, which is
the mistake the removed minio service made.

Pointing the `object-store` job in `.github/workflows/storage.yml` at a `gs://`
bucket is what closes it.

**Real S3, as opposed to minio.** The `object-store` job is
`workflow_dispatch`-only and skips itself unless `STORAGE_TEST_AWS_*` secrets
exist, so as things stand the S3 evidence is minio's. minio is a real HTTP
implementation of `If-None-Match` rather than an emulation in the same process,
and `object_store` maps the `412` it returns exactly as it maps AWS's, so this is
a much smaller gap than the one before it — but it is not zero, notably around
throttling and read-after-write on a bucket under load.

**The engine's own unit tests.** Three sites inside `tansu-storage/src` still
name `memory://` and should: two in `lib.rs` assert that the `memory://` scheme
parses and that deprecated URL keys still build, which is a statement about URL
handling rather than about a store. The one exception is a unit test in
`src/service/delete_groups.rs`, which builds a real store and cannot reach
`tests/common`; it runs on `memory://` only.

**`tansu-topic`.** The `tansu topic` subcommand is at or near 0%. It is a thin
shell over code that is covered, but it has no test that would notice if the
shell stopped delegating.

`tansu-perf` and `tansu-otel` used to sit in this paragraph alongside it, and
`tansu-proxy` — 2 800 lines with no integration test — should have. All three
were deleted instead: nothing in this fork deployed, documented or benchmarked
with them, so the honest fix for their coverage was removal, not tests.

## CI layout

`pr.yml` is the only workflow that runs on pull requests. `ci.yml` is disabled
(it is upstream's, kept for reference), `publish.yml` runs on `v*` tags, and
`storage.yml` runs nightly and on `workflow_dispatch`.

`storage.yml` is where the object store gets a service. It is off the PR path on
purpose: `test` in `pr.yml` is the required check, so a Docker-dependent job
upstream of it would add minutes to every PR to establish something that only
changes when `object_store` or the store itself does. The conformance target
still runs on every PR — against `memory://`, as an ordinary workspace test
costing ~30ms — and `storage.yml` is what re-runs it where the semantics are
real.

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
