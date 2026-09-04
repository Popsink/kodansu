# Google Cloud Storage

Operator guide for `gs://`. [docs/storage-tuning.md](storage-tuning.md) is the
tuning and cost reference for the object-store backend generally; this page is
what is different about GCS.

**Support level: the same as S3, with less evidence.** `gs://` has been a
supported target since before this fork and it has never been run against a real
bucket in CI. That is a decision rather than a backlog item — no nightly against
real credentials, on `gs://` or `az://` (rfc-adls.md §4.3) — so everything below
that is marked *observed* comes from `object_store`'s source or from a local
model of GCS, and everything marked *assumed* comes from Google's documentation
and has never been seen from this code.

[docs/testing.md](testing.md) is precise about the shape of the gap, including
why the emulator route is still closed and exactly how far it now gets.

## The URL

`gs://<bucket>/`. The bucket is the URL host; the path is ignored, as it is for
`s3://`. Every storage-URL query parameter is scheme-independent and applies
unchanged — `coalesce_*`, `batch_min_size`, `batch_max_delay`, `segment_format`
and the rest of [docs/storage-tuning.md](storage-tuning.md).

## Credentials

From the environment, via `GoogleCloudStorageBuilder::from_env`:

| Mechanism | Variables |
|---|---|
| Service-account file | `GOOGLE_SERVICE_ACCOUNT` / `GOOGLE_SERVICE_ACCOUNT_PATH` / `SERVICE_ACCOUNT` |
| Service-account key, inline JSON | `GOOGLE_SERVICE_ACCOUNT_KEY` |
| Application default credentials | `GOOGLE_APPLICATION_CREDENTIALS` |
| **Workload Identity** | nothing — the GKE metadata server is the fallback |

**Workload Identity is the GKE path**, and the analogue of IRSA on EKS and of
workload identity federation on AKS. Prefer it: it is the one mechanism with
nothing to rotate. `object_store` reaches it through the instance metadata
server, which is what it falls back to when none of the variables above is set.

The identity needs **`roles/storage.objectAdmin`** on the bucket. `objectViewer`
and `objectCreator` are both insufficient: the layout is create-only but
retention and compaction are implemented by delete, and the read path lists.
Project-level `Owner` does grant this, but a dedicated bucket-scoped binding is
the right shape.

**Requester-pays buckets do not work.** `object_store` never sends
`userProject`, so every request to one is rejected. There is no error message
that says so.

## Bucket configuration is part of the contract

The layout is create-only and immutable, and retention is implemented by delete.
So these settings are not preferences:

| Setting | Required | Why |
|---|---|---|
| **Soft delete** | **retention `0`** | On by default, at 7 days, and billed |
| Object versioning | off | Every deleted segment would be retained as a noncurrent version |
| Retention policy / bucket lock | off | Would refuse the deletes retention and compaction depend on |
| Autoclass | off | Nearline/Coldline carry early-delete minimums; segments live minutes |
| Lifecycle class transitions | off | Same reason |
| Lifecycle deletion rules | off | Retention is the broker's, and a rule that races it deletes live data |
| Uniform bucket-level access | on (recommended) | Nothing here sets an object ACL |
| Location | single region, near the brokers | Every request is on the hot path |

**Soft delete is the one that costs money quietly.** Google enabled it on all
new and existing buckets in March 2024 with a seven-day default, and it has been
billed since 1 September 2024: a deleted object keeps being charged at its
storage price for the whole retention window. That is a modest overhead on a
bucket whose objects are long-lived and a large one here, where compaction's
whole job is to write a merged segment and delete the originals — the churned
bytes are billed twice for a week. Set the bucket's soft-delete retention to `0`
(and consider the org-level tag, so new buckets inherit it).

None of these is detectable from the data plane, so the broker cannot warn about
any of them at startup — the same conclusion as ADLS Gen2 (#421). Unlike a fresh
Azure account, a fresh GCS bucket does **not** already satisfy them: soft delete
is on.

## What GCS does that S3 does not

### Conditional writes key on the generation, not the etag

S3 and Azure both condition `PutMode::Update` on the etag. GCS conditions it on
the object's **generation**, sent as `x-goog-if-generation-match`, and
`object_store` reads that generation out of `UpdateVersion::version` — a field S3
and Azure ignore and `InMemory` never populates. A conditional update whose
`version` is empty does not lose a race on GCS; it returns
`Generic { MissingVersion }`, which is not `Precondition`, so no CAS loop in the
engine retries it.

The invariant that follows — *every `Version` handed to a conditional update came
from a GET or a PUT of that object, never from a listing* — holds, and
`dynostore::tests::gcs_generation` is what keeps it holding. A listed
`ObjectMeta` carries an etag and no generation on every backend, so it looks like
a usable version and is not one here. See `tansu-storage/src/os.rs`.

**Observed** against `object_store` 0.14.1's source and modelled over `InMemory`.
Not observed against a bucket.

### One write per second, to the same object name

Google documents *"Maximum rate of writes to the same object name: one write per
second. Writing to the same object name at a rate above the limit might result in
throttling errors."* The `gs` arm therefore wraps the store in a client-side
`PutRateLimiter` at one put per second per key.

This reaches exactly one object. The data plane is create-only — it issues no
conditional update at all, asserted in `dynostore::tests::gcs_generation` — so
produce and fetch never write one key twice and never meet the cap. What does
meet it is a consumer group's `generation.json`, where every member's admission
is its own CAS: **16 members racing to form one group take ~54 seconds** under
the cap, against 3 ms without it, which is past a Kafka client's 45 s default
session timeout. That is #427, it is a real inability to form a group of any size
on GCS, and it is open.

Two things bound it honestly: it is per group, not fleet-wide (different groups
write different objects), and the limiter is a **local delay**, so the cost is
paid whether or not a real bucket would have rejected the burst. Nothing has
observed what GCS actually does under this pattern.

### The bucket ramps, and the retry budget is not sized for it (#519)

A GCS bucket starts at roughly **1,000 object writes/s and 5,000 reads/s** and
scales from there by redistributing load, which *"typically takes on the order of
minutes"*; Google asks that you ramp no faster than doubling every 20 minutes.
S3, by contrast, scales per key prefix and needs no warm-up.

The `gs` arm's retry budget is **5 retries over 15 s**, against the `s3` arm's
32 over 300 s. That was chosen for the per-object cap — #13's symptom was a 30 s
produce latency with no log lines, and a short budget turns it into a fast
failure. It is the wrong budget for the per-bucket ramp, which produces the
*other* failure shape: a fleet-wide 429 storm during a scale-up, which is what
S3's long budget exists to ride out. A cold bucket meeting an autoscaled fleet is
the case #364 creates.

### Deletes are serial, ten at a time (#518)

`object_store` implements bulk delete for S3 (`DeleteObjects`, 1,000 per request,
20 requests in flight) and for Azure (Blob Batch, 256 per request, 20 in flight).
For GCS it issues **one `DELETE` per object, ten in flight** — the XML API has no
batch delete and `object_store` does not use the JSON batch endpoint.

So the retention and compaction delete path is two orders of magnitude narrower
on GCS than on S3 in objects per wave, and each of those deletes counts against
the bucket's object-write rate. Deletes themselves are free of charge, as on S3
and Azure; what they cost here is wall clock and ramp headroom.

### Object names should be random, and ours are sequential

Google's request-rate guidance still says to *"avoid using sequential names"*
and that *"completely random object names give you the best load distribution"*,
because auto-scaling splits a bucket by key range.

Segment names are `{seq:0>20}.seg` and are deliberately sequential: the whole
incremental prefix-index refresh is a `start-after` listing that depends on
lexicographic order, and so does the tail probe. This is not a naming choice that
can be reversed.

What bounds it is that the sequence is *per prefix*: the entropy lives in the
cluster/topic/partition components above it, so a fleet of many topics spreads
across many key ranges. A single very hot partition is the case that does not
spread, and there is no measurement of it.

## Costs

GCS and S3 price operations the same to two significant figures — Class A (write,
list) at $0.05 per 10,000 and Class B (read) at $0.004 per 10,000 for Standard
storage, against S3's $0.005 per 1,000 PUT and $0.0004 per 1,000 GET. Deletes are
free on both. Storage is $0.020/GB/month against S3's $0.023 in comparable
regions. Check the current numbers for your region before sizing anything; the
point here is the *shape*, which is that unlike ADLS Gen2 there is no per-request
premium and no different list classification.

So the GCS bill for this workload is the S3 bill, and
[docs/storage-tuning.md](storage-tuning.md)'s request-bill section applies
unchanged. The two GCS-specific line items are both structural rather than
per-request:

- **soft delete**, which bills churned bytes for seven days unless it is turned
  off — the largest single avoidable cost on this backend;
- **delete concurrency**, which is throughput rather than money, but it is what
  decides whether retention keeps up.

## Running it locally

There is nothing to run. `memory://` covers the engine and minio covers the S3
shape; neither says anything about GCS, and no GCS emulator serves
`object_store`'s client today — though Google's own is close, and #520 is the
route.  [docs/testing.md](testing.md) has the current state of that, measured
rather than assumed.

What can be run without a bucket:

```shell
just test-gcs   # the per-object write cap and the read-path shape, over InMemory
cargo nextest run -p tansu-storage -E 'test(gcs_generation)'   # GCS CAS semantics
```
