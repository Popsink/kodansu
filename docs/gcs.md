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
`s3://`. Nearly every storage-URL query parameter is scheme-independent and
applies unchanged — `coalesce_*`, `batch_min_size`, `batch_max_delay`,
`segment_format` and the rest of
[docs/storage-tuning.md](storage-tuning.md).

The one exception is **`delete_concurrency`**, which is read on this arm and
nowhere else, because GCS is the one backend with no bulk delete to widen. See
[Deletes are one request per object](#deletes-are-one-request-per-object-518)
below.

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
produce and fetch never write one key twice and never meet the cap. What used to
meet it is a consumer group's `generation.json`.

Every member's admission was its own CAS, and the members race: **16 members
forming one group took ~54 seconds** under the cap, against 3 ms without it,
which is past a Kafka client's 45 s default session timeout. Not a slow group but
one that cannot form — the sweep evicts members that have not been admitted yet
and the group re-forms into the same wall. Only 16 s of that was the writes that
had to land; the rest was ~3.4 conflicting attempts per member, each waiting out
a full second before being told it lost.

**Fixed in #427 by batching admission.** A member that the generation does not
name is mid-join, and so is every other member whose document is fresh and
unnamed; the lowest id among them does the one CAS that admits them all, and the
others wait for it. The election is decided from the persisted documents, so
every replica reaches the same verdict, and it is bounded — a member that waits
two seconds for a peer that never writes admits itself, which is the old
behaviour arrived at late rather than never. Measured through `Controller::join`
over this exact store shape (`tansu-broker/tests/group_formation_cap.rs`): **two
writes and 4.2 s for 16 members**, three seconds of which is the join window
every group pays on every backend.

It also pays on S3, where the same per-member CAS was not a wall but a bill:
#406 put consumer-group PUTs at 67% of the PUT spend.

Two things bounded the original problem honestly and are worth keeping in mind
for the next one: it was per group, not fleet-wide (different groups write
different objects), and the limiter is a **local delay**, so the cost was paid
whether or not a real bucket would have rejected the burst. Nothing has observed
what GCS actually does under this pattern.

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

### Deletes are one request per object (#518)

`object_store` implements bulk delete for S3 (`DeleteObjects`, 1,000 per request,
20 requests in flight) and for Azure (Blob Batch, 256 per request, 20 in flight).
For GCS it issues **one `DELETE` per object** — the XML API has no batch delete,
and `object_store` does not use the JSON batch endpoint (`POST
/batch/storage/v1`, 100 sub-requests), which Google's own guidance discourages
for Storage anyway. That is structural and this fork cannot change it.

What it could change is the fan-out. Upstream's was **ten**
(`object_store-0.14.1/src/gcp/mod.rs:187`), the same ten the `ObjectStore` doc
example uses — inherited by every delete this engine issues, and unreachable,
since `GoogleCloudStorageBuilder` has no option for it. It is now
`gcs::limit::PutRateLimiter`'s, which the `gs` arm already wraps the store in,
and it defaults to **16**:

| | objects per request | requests in flight | objects in flight |
|---|---|---|---|
| S3 | 1,000 | 20 | 20,000 |
| Azure | 256 | 20 | 5,120 |
| GCS, upstream | 1 | 10 | 10 |
| **GCS, here** | 1 | **`delete_concurrency`, default 16** | 16 |

Sixteen because that is the number this engine already picked for the identical
shape — `dynostore`'s `delete_each`, the per-key fallback it drops to when S3
throttles a bulk delete, wide enough to make progress and narrow enough not to
re-create the burst that caused the throttle. On GCS every delete is that shape.

```
gs://my-bucket/?delete_concurrency=64
```

**Before raising it**, the arithmetic, none of which has been measured against a
bucket:

- at a nominal 30 ms per `DELETE`, 16 in flight is ~530 deletes/s per
  `delete_stream`;
- a maintainer runs up to **four** prefixes concurrently
  (`PREFIX_MAINTENANCE_CONCURRENCY`) and each of them deletes, so the per-replica
  ceiling is ~4× that — ~2,100 deletes/s;
- a bucket starts at ~1,000 **object writes**/s, deletes count against that
  budget, and it ramps by redistribution rather than instantly (see the section
  above);
- and the fleet multiplies all of it by the replica count.

So there is not 60× of headroom here, whatever the S3 column suggests. What a
wider fan-out buys is a **shorter delete wave, not a higher sustained delete
rate** — the sustained rate is whatever retention and compaction have to retire,
which is set by the write rate and not by this number. The case that needs it is
a maintenance tick that retires more than it can drain, and the signal for that
already exists: `tansu_maintenance_duration` approaching the maintenance
interval, or `tansu_prefix_drain_stops{reason!="drained"}` above zero. Raise it
then, on one replica first, and not before.

A value below ten is legitimate and is why the key parses freely rather than
clamping upwards: a large fleet against a cold bucket has more replicas than it
has ramp. `0` is rejected — it would be a `delete_stream` that never yields — and
falls back to the default with a warning, as an unparseable value does.

Deletes themselves are free of charge, as on S3 and Azure. What they cost here is
wall clock and ramp headroom — and, until the bucket's soft-delete retention is
set to `0`, seven days of storage for every byte retired.

Measured in `dynostore::tests::delete_fan_out`, whose control arm is
`gcp/mod.rs:187` copied verbatim, so the ten and the sixteen are both observed
rather than quoted.

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
  decides whether retention keeps up — `?delete_concurrency=`, default 16.

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
