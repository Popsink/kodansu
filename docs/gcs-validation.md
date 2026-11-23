# Validating `gs://` against a real bucket

The runbook for acceptance items 1–3 of Popsink/data-plane#6179 (DEV-485):
a broker deployed from the chart on GKE with Workload Identity alone, a
sustained load test, and the offset-commit bound. **Only step 1 has been run**
(results under it).
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

Not yet run: steps 2–4.

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

## What to file if a step fails

One issue per failing step, with the build, the bucket's `describe` output, the
metric series above for the window, and the broker log at `info`. A `429` in
`class="segment"` is the bucket ramp, and a `429` in any other class is a hot
key: `docs/gcs.md` says which knob each one points at.
