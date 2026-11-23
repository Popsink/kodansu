# Validating `gs://` against a real bucket

The runbook for acceptance items 1–3 of Popsink/data-plane#6179 (DEV-485):
a broker deployed from the chart on GKE with Workload Identity alone, a
sustained load test, and the offset-commit bound. **All four steps were run
once, on 2026-11-23, on the sandbox GKE cluster** (results under each step).
Steps 1 and 2 pass; step 3 passes on produce and fails on compaction; step 4
fails. The failures are DEV-662, DEV-663 and DEV-664.
It needs a GCP project, a bucket and a GKE cluster, which this repository's CI
does not hold and, by decision (`docs/testing.md`, 2026-09-03), will not hold
on a schedule. Everything that can be measured without them is measured in the
tests cited below. This page is what remains, in the order to run it, with the
number each step has to produce.

Write the results into [docs/gcs.md](gcs.md), next to the *assumed* claims they
replace, and say which build they came from.

## 0. Prerequisites

| What | How |
|---|---|
| A dedicated bucket in the cluster's region, configured to the contract | Operator docs, "Helm chart reference" → *Google Cloud Storage* → *Bucket settings* (`gcloud storage buckets create … --soft-delete-duration 0 …`, and the `describe \| jq` check). Do not skip the check: a fresh bucket has soft delete on. |
| A Google service account with `roles/storage.objectAdmin` **on that bucket only** | Same page, *The Google side* |
| A GKE cluster with Workload Identity enabled, NetworkPolicy enforcement on | NetworkPolicy on, so the chart's metadata-server egress rule is exercised rather than bypassed |
| A broker image containing Popsink/kodansu#758 and #760 | Without #760, step 4 measures the per-commit wall |
| A chart containing Popsink/data-plane#6574 | The `kodansu.storage.gcp` block |

**Credentials that do not work**, so as not to lose an afternoon to them:
`object_store` 0.14.1 reads `service_account` and `authorized_user` ADC files,
not `external_account` (`src/gcp/credential.rs`, `ApplicationDefaultCredentials`).
Workload Identity *Federation* — GitHub OIDC, AWS → GCP, the file
`google-github-actions/auth` writes — therefore cannot authenticate the broker.
GKE Workload Identity can, because it goes through the metadata server. Outside
GKE, use a service-account key.

## 1. Conformance: the storage suite on the bucket

From a workstation with a key, or through the `storage` workflow's `gcs`
dispatch (`.github/workflows/storage.yml`):

```shell
export GOOGLE_SERVICE_ACCOUNT_KEY="$(cat key.json)"
just test-storage gs://<bucket>/
```

**Pass**: the suite is green. It includes `conditional_put`, which is the
generation-CAS question (`docs/gcs.md`, *Conditional writes key on the
generation*) asked of GCS itself rather than of `dynostore::tests::gcs_generation`'s
model of it. A `MissingVersion` anywhere in the log is a failure even if the
test passed. Every run uses its own cluster id, so the bucket can be shared.

Then run the audit over what the suite left behind, **twice**:

```shell
tansu audit --storage-engine gs://<bucket>/ --cluster-id <one of the suite's cluster ids>
```

**Pass**: both runs complete and agree (#758 is what gives the audit the
broker's retry budgets on GCS).

**Run on 2026-11-23**, against `gs://kodansu-dev485-validation/` (sandbox
project, europe-west1, created with the operator docs' command; its `describe |
jq` check returned exactly the expected values). The build was `main` with
#758, #760 and this branch merged, run from a workstation and authenticated as
a user (`authorized_user` ADC). Results:

- 757 of 758 passed. About 2,200 objects were written, with no `MissingVersion`
  and no `429`.
- The one failure was `produce_fan_out::partitions_are_written_at_once`, a
  wall-clock bound calibrated for `memory://`: eight partitions must finish
  inside two lingers (500 ms). Over GCS from a workstation they took 520–600 ms
  across four runs, which is one linger plus ~300 ms of round trips. A serial
  write would be at least 2 s, so this is latency and not a regression. That
  bound cannot hold against a remote store.
- `tansu audit`, two passes on each of three of the suite's clusters: the passes
  were identical, 0 unreadable, no records lost.


## 2. Acceptance 1 — deployed from the chart with Workload Identity only

```yaml
kodansu:
  storage:
    engine: gs://<bucket>/
    gcp:
      workloadIdentityServiceAccount: <gsa>@<project>.iam.gserviceaccount.com
  networkPolicy:
    enabled: true
    allowExternalEgress: true
kotatsu:
  enabled: true
  networkPolicy:
    enabled: true
```

No `serviceAccount.annotations`, no `extraEnvVars*`, no `extraEgress`.

**Pass**:

- the broker reaches `ready` and its log has no `conditional put` probe failure
  (`skip_conditional_put_check` must not be set);
- `kafka-topics --create`, a produce and a consume round-trip through it;
- Kotatsu lists the topic and shows the records;
- `kubectl exec` into the broker pod and `curl -s -H 'Metadata-Flavor: Google'
  http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/email`
  prints the GSA. This proves the annotation and the NetworkPolicy rule, not
  just the absence of an error.

Then repeat once with `networkPolicy.allowExternalEgress: false` and an
`extraEgress` rule for `storage.googleapis.com`'s ranges, to confirm the
metadata rule is what Workload Identity was using.

**Run on 2026-11-23: pass.** The broker (3 replicas plus the maintainer) and
Kotatsu were rendered from Popsink/data-plane#6574 and applied as-is. The
data-plane backend Deployment cannot be switched off, so only the `tansu/*`
and `kotatsu/*` templates went in. Results:

- The metadata server, queried from inside a broker pod, answered with the
  GSA, and the token request returned `200`.
- With `allowExternalEgress: false` and an `extraEgress` rule allowing only
  port 443, the token request still returned `200`. Without the chart's
  metadata rule (`metadataServerEgress: false`), the metadata server was
  unreachable. So that rule is what Workload Identity uses.
- An idempotent `acks=all` round trip worked, and the group's offset was
  committed.
- Kotatsu, on `KOTATSU_STORAGE_PROVIDER=gcs` with the same identity, listed
  the topics and read the records.

## 3. Acceptance 2 — sustained load

The Debezium profile: idempotent, `acks=all`, many producers, many partitions,
large consumer groups committing. From pods in the same region:

```shell
# per producer pod; run N of them against distinct topics and one shared one
kafka-producer-perf-test --topic load-$i --num-records 50000000 --record-size 500 \
  --throughput -1 --producer-props bootstrap.servers=<broker>:9092 \
  acks=all enable.idempotence=true max.in.flight.requests.per.connection=5 \
  linger.ms=50 batch.size=524288 compression.type=lz4

# per consumer group: G members, auto-commit at the client default (5 s)
kafka-consumer-perf-test --bootstrap-server <broker>:9092 --topic load-$i \
  --group load-group-$i --messages 50000000 --timeout 600000
```

Shape to reach, and hold for at least an hour, **ramping the producer count no
faster than doubling every 20 minutes** (a new bucket starts near 1,000 object
writes/s and redistributes over minutes, `docs/gcs.md`):

- ≥ 20 producers, ≥ 50 topics × 16 partitions;
- ≥ 3 consumer groups of ≥ 16 members each;
- 3 broker replicas.

Collect, per minute:

| Signal | Source | Pass |
|---|---|---|
| produce latency p99 | the producer's own `request-latency-avg/max` and perf-test's percentiles | **< 30 s** |
| unbounded retry | `tansu_opticon_with_mut_retries` histogram; broker log `OptiCon with_mut contending` | no growing tail |
| 429 storm | `tansu_object_store_request_error{reason="throttle"}` by `class` (`docs/gcs.md` table) and the `object_store` INFO line `status 429` | exhausted budgets at zero; absorbed 429s only during the ramp |
| object-store latency | `tansu_object_store_request_duration` by `method`, `class` | no sustained climb at a constant load |
| data loss | `tansu audit`, run twice after the load stops | no gap the BEFORE baseline does not have |
| offsets advance | consumer lag per group, and `kafka-consumer-groups --describe` | committed offsets advance at the commit interval |

Then sweep `coalesce_linger` (50 ms, 100 ms, 200 ms) and `coalesce_bytes` one
at a time, as `docs/storage-tuning.md` prescribes. Record GCS class A
operations per second against produce p99. The GCS-specific defaults that
acceptance item 2 asks to document come from this sweep and from nothing else.

**Run on 2026-11-23, scaled to the sandbox cluster: produce passes,
compaction fails.**

- **Shape.** 7 producers (6 topics × 16 partitions at 2 000 × 500 B
  records/s, plus `load-commit` with 64 partitions at 500/s), 3 groups × 16
  members on librdkafka auto-commit, 3 broker replicas plus the maintainer, 46
  minutes. The bucket was not ramped, since the load is far below its cold
  write limit.
- **Produce.** Mean window latency was ~450 ms and the worst window max 6.5 s,
  out of ~545 five-second windows per producer. No producer restarted. This is
  well inside the 30 s bound. kafka-producer-perf-test 4.1 has no
  `--reporting-interval`, so p99 comes only from the end-of-run line, which a
  stopped run never prints; the worst window max is the bound to quote.
- **Compaction never landed.** Every pass ended in `segment sequence
  assignment exhausted retries … role="compaction" conflicts=64`, and prefixes
  reached 30 000 – 38 000 segments. The cause is that the 16 MiB merged create
  loses the tail-sequence race to producer creates arriving every ~70 ms.
  Setting `prefix_compact_target_bytes=1m` did not fix it: compaction won ~1 %
  of attempts and the count kept growing. **DEV-662.**
- **Kotatsu (pin 0.12.1).** 92 of 113 probe requests on topic endpoints
  returned 504 at its 30 s timeout. They kept doing so after the producers
  stopped, because of the segment backlog. **DEV-664.**

Add a Kotatsu probe to every run of this step: four loops requesting
`/api/clusters/{c}/topics`, `…/topics/{t}`, `…/topics/{t}/messages`,
`…/groups/{g}` every 5 s, recording code and `time_total`. Pass when they
return 200 within 30 s throughout.

## 4. Acceptance 3 — offset commits of a large group

Measured locally over the `gs` arm's limiter (`tansu-storage/tests/offset_commit_cap.rs`):

| case | before #760 | after |
|---|---|---|
| 16 members committing at once, 1 replica | 65.7 s (p50 39 s) | 2.04 s |
| 128 members, 1 replica | — | 2.07 s |
| 48 members across 3 replicas | 147 s | 6.2 s |

What only a bucket can say is how request latency and a real `429` add to
that. On the cluster from step 3:

```shell
kafka-consumer-perf-test … --group commit-bound --consumer.config <(echo 'auto.commit.interval.ms=1000')
```

with 64 members in the group, spread over the 3 replicas. **Pass**: the
`OffsetCommit` p99 (`tansu_request_duration{api_key="8"}`, or the
client's `commit-latency-max`) stays under 10 s, and no commit fails with a
timeout. Record the measured bound in `docs/gcs.md` either way. Acceptance
accepts "the limit is documented with a measured bound".

**Run on 2026-11-23: fail.** The group had 64 members, committing
synchronously every 1 s from one pod of `confluent-kafka` consumers, spread
over the 3 replicas, on a build including #760. Results:

- Commit p50 stayed ~2 s.
- p99 per one-minute window was a median of 29 s and a max of 80 s, over 52
  windows.
- 381 commits failed with `ILLEGAL_GENERATION`.
- The broker logged `OptiCon with_mut contending on a hot object` on every
  group's `offsets.json`, up to 105 retries.

#760 removed the contention within a replica. Across replicas, the three still
fight over the one object GCS updates about once per second. **DEV-663.**

## What to file if a step fails

One issue per failing step, with the build, the bucket's `describe` output, the
metric series above for the window, and the broker log at `info`. A `429` in
`class="segment"` is the bucket ramp, and a `429` in any other class is a hot
key: `docs/gcs.md` says which knob each one points at.
