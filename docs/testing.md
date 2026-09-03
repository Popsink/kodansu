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
just test-conditional-put-azurite          # starts Azurite in compose, runs on az://tansu/
```

`test-conditional-put-azurite` is a recipe of its own rather than
`just test-conditional-put az://tansu/`, because the emulator needs
`AZURE_STORAGE_USE_EMULATOR=true` and the only place that could come from
silently is `.env`. It rewrites the account to Azurite's development pair and
the endpoint to localhost, so a stray copy in an operator's environment would
point a real deployment at nothing — `AWS_ENDPOINT` fails loudly when it is
wrong, this does not.

For the broker rather than the suite, `just broker-az` is the `broker-s3` shape:
Azurite in compose, the `tansu` container created, and
`--storage-engine=az://tansu/`.

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

Verified green on `memory://`, on `s3://` against minio and on `az://` against
Azurite (`object_store` 0.14.1, `quay.io/minio/minio`,
`mcr.microsoft.com/azure-storage/azurite`):

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

**Azurite is the only one of the three that runs on every PR** (#420). minio's
run is nightly (`storage.yml`) and GCS has never had one, so before this the
per-PR evidence for conditional put was `InMemory` emulating it behind a mutex.
`just test-conditional-put-azurite` needs nothing but Docker, and the
`conditional-put-azurite` job in `pr.yml` feeds `all-green`.

That is a real gain and it is also the *whole* gain. What a green Azurite run
does not prove is below, and it is not a short list.

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
nothing**. Every replica drives every group: there is no owner and no
configuration (#360). It is the exit criterion for the group-state
decomposition (#359), on the shape that made the 1500-topic deployment wedge —
many groups at once, not one.

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
| `TANSU_SCALE_PUT_BUDGET_PER_MEMBER` | 8 | writes of `generation.json` a group may cost to form |
| `TANSU_SCALE_DEADLINE_SECS` | 120 | per-member, only reached on failure |

The object the budget counts is `generation.json`, and it counts **attempts**:
a conditional PUT the store rejects on its precondition is still a request it
charged for. `{group}.json` is not written at all any more, so counting it would
be asserting zero of nothing.

Measured at the default size, 1024 members converging: **~5010 formation
attempts, 1024 landed, 0 steady-state writes.**

1024 landed writes is 64 groups × 16 members — one CAS per member, which is the
floor for admitting them one at a time. The members race for those 1024 slots
and are rejected ~4000 times on the way. **Formation conflicts are budgeted
rather than forbidden**: N members racing to add themselves to one document is a
race, and the CAS is what resolves it. Nothing serializes them any more — the
per-group in-process lock went with the rest of the per-group state (#360), so
members served by the same replica race exactly as members on different ones do.
That is the trade the autoscaling story is bought with, and it is paid once per
rebalance rather than continuously.

What is asserted as an exact zero is the *steady state*. After convergence every
member heartbeats several times from the replica it entered through, and across
all 64 groups that must produce no write of `generation.json` beyond the sweep's
own stamp, no CAS conflict, and no listing of member documents. That is the
regime a deployment lives in, and it is the property that used to require an
owner.

## What is not covered

**Azure, as opposed to Azurite — three exclusions, and the first is the sharp
one.** The `conditional-put-azurite` job in `pr.yml` is the only per-PR evidence
we have of a conditional put against a real implementation, which makes it
tempting to read a green badge as "Azure is tested". It is not.

1. **`list_with_offset` is not exercised at all.** `object_store` detects the
   emulator and bypasses `startFrom`, falling back to client-side filtering over
   a full listing (`azure/mod.rs`, citing Azurite#2619). So the tail-offset
   listing `scan_from` depends on for its O(new) prefix-index refresh is the one
   thing the emulator structurally cannot check — and a client-side filter
   returns the *same set*, so nothing goes red. Answered against a real HNS
   account instead (#417): `startFrom` is honoured server-side and is
   **inclusive**, which `object_store` compensates for by dropping the leading
   exact match.
2. **Azurite has no hierarchical namespace**, so nothing here covers ADLS Gen2
   as such: not directory entries in listings, not the directory residue a
   delete leaves behind, not Blob Batch on HNS, not the `/`-sorts-lowest
   ordering. All four were answered by hand, once, against a real HNS account
   (#417, findings in `docs/rfc-adls.md` §4 and §5) — and "once, by hand" is
   exactly as durable as it sounds. **That is now permanent**, per the decision
   above: those four answers describe `object_store` 0.14.1 against a real
   account on 3 September 2026, and no job will notice if one changes. When the
   Azure client changes, or before promoting the backend past experimental,
   reproduce the spike from `docs/rfc-adls.md` §4 and §5 — they record the
   method as well as the results, because the harness was throwaway and is not
   in the tree.
3. **It does not throttle.** Azure throttles at account and storage-partition
   level, which is the failure shape the `abfss` arm's retry budget is written
   against (`lib.rs`, the S3-shaped 32/300 s). Nothing here tests that budget.

A fourth, from #419 and worth stating because it changes what a smoke test is
worth: a broker **reads its own writes from its own `PrefixIndex`** and never
reads a segment footer back. So a single-process produce-then-consume passes on
Azure even with the suffix-range translation removed entirely. Any read-path
check has to involve a second broker over the same container, or a restarted one
— `just broker-az`, produce, restart, consume is the smallest version that
actually exercises it.

**GCS.** `gs://` is a supported target and every test in the conformance target
would run against it unchanged — that is the whole point of the URL being a
parameter — but none of them ever has. Generation preconditions remain assumed
rather than observed.

An emulator was tried and does not work, which is worth recording so nobody
tries it twice (#357):

- `object_store` **does** support pointing at one with no code change: a service
  account file containing `{"gcs_base_url": "...", "disable_oauth": true}` is
  read by `GoogleCloudStorageBuilder::from_env`, so `GOOGLE_SERVICE_ACCOUNT`
  alone would redirect the engine.
- `fake-gcs-server` gained generation preconditions in July 2026 (upstream
  fsouza/fake-gcs-server#2260, #2308), so the semantics under test are no longer
  the obstacle.
- **The obstacle is the API.** `object_store` 0.14's GCS client writes through
  the **XML** API — `PUT /{bucket}/{object}` with `x-goog-if-generation-match` —
  and `fake-gcs-server` answers every such write `400 invalid uploadType`. It
  implements the JSON upload API. Measured against
  `fsouza/fake-gcs-server:latest` on 2026-08-09: 10 of 10 conformance tests
  fail, all on the write, none of them reaching a precondition.

So an emulator would not fake the assertions — it cannot run them at all. The
only route that closes this is pointing the `object-store` job in
`.github/workflows/storage.yml` at a real `gs://` bucket.

**Decided 3 September 2026: that is not happening.** No nightly runs against
real credentials, on `gs://` or `az://`. It is a decision rather than a backlog
item, so `gs://` generation preconditions stay assumed for as long as the arm
exists, and `STORAGE_TEST_AWS_*` stays unset — the `object-store` job is there
for an operator who brings their own bucket to a `workflow_dispatch`, and skips
otherwise.

Worth re-checking if `object_store` moves its GCS writes to the JSON API, or if
`fake-gcs-server` implements the XML upload path. Either would reopen the
emulator route, which the decision above does not close — it closes the
*credentials* route.

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

**No client library, of any language, is driven against a broker.** The suite
calls the services directly through `rama`, so what it can check is that a
response matches this repo's reading of the message definition — not that a
client accepts it. #515 is what that costs: `GetTelemetrySubscriptionsResponse`
documents `ClientInstanceId` as "assigned client instance id if ClientInstanceId
was 0 in the request, **else 0**", the service implemented that sentence, and
`tansu-storage/tests/get_telemetry_subscriptions.rs` asserted the 0. It read as
a conformance test. It was a transcription of the same misreading, so it would
have passed forever — while every Java client in the fleet, from `alpha.5` on,
took an `IllegalArgumentException` out of `poll()` 300 s after start, because
the reference client rejects that field non-zero unconditionally.

The lesson is narrower than "add a Java client to CI": a test written from the
same document as the code under test is one statement, asserted twice. Where the
reference implementation and the message definition can disagree — and KIP-714
is not the only place they do — read `apache/kafka`, and say in the test which
one is the contract.

Driving a real client is still the only thing that would have caught it, and it
does not need CI to be useful. #515 was verified by hand this way, and the recipe
is short enough to repeat:

```shell
cargo build --bin tansu --features dynostore
target/debug/tansu broker --storage-engine=memory://tansu/ &
# a JDK, kafka-clients on the classpath, a KafkaConsumer polling in a loop,
# org.apache.kafka.common.telemetry at DEBUG, left running for 340 s
```

Two subscription intervals is the shortest run that proves anything here,
because the first `GetTelemetrySubscriptions` carries a zero id and the second
carries the assigned one — the whole defect is in the difference. `340 s`
rather than `340 ms` is why no unit test was ever going to be the thing that
found it.

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
| `conditional-put-azurite` | the conformance target against a real `If-None-Match`, not an emulation of one |
| `coverage` | line coverage, floored |
| `all-green` | one status summarising the seven above |

`conditional-put-azurite` is the one exception to the paragraph above, and it is
worth being precise about why. It is Docker-dependent, so it is deliberately not
upstream of `test` — it feeds `all-green` only, and becomes enforced when branch
protection moves. What earns it a place on the PR path at all is that no other
job can establish what it establishes: every other test in the workspace runs on
`memory://`, and conditional put is precisely where `InMemory` and a real store
diverge. It is also cheap, because Azurite needs no credentials and no bucket
setup beyond one signed REST call (`etc/azurite-container.py`).

Read the Azure exclusions under *What is not covered* before treating it as
evidence about ADLS Gen2. It is evidence about conditional put.

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
  repos/Popsink/kodansu/branches/main/protection/required_status_checks \
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
