# RFC: Stateless Metadata Coordinator for Multi-Replica Tansu

Status: Draft
Audience: tansu maintainers
Scope: produce/fetch/offset/idempotence/transaction metadata in a multi-replica deployment

## 1. Problem

In a multi-replica deployment the hot metadata path is coordinated *through
object-storage latency*. The `#13` fix already removed the capped hot-`watermark`
object: ordinary topics now assign offsets by creating immutable, create-only
batch objects (`records/{offset:0>20}.batch`) and derive the high watermark by
listing them (`DynoStore::assign_and_create`, `refresh_high` in
`tansu-storage/src/dynostore.rs`). That moved the bottleneck rather than removing
it. Three costs remain:

1. **Create-race retry storms.** Two replicas producing to the same partition
   race the same offset via `PutMode::Create`; the loser re-`LIST`s the tail and
   retries. More hot replicas on a partition ⇒ more collisions ⇒ more LISTs.
   `assign_and_create` warns past 8 retries and bails at 64.
2. **LIST latency on the read/offset path.** `refresh_high`, `offset_stage` and
   `list_offsets` derive boundaries by listing batch objects. LIST is the
   slowest, most rate-limited object-store op.
3. **No fast mutable store.** Offsets, consumer-group offsets and producer
   sequences are all coordinated via object-store round-trips. Consumer offsets
   are last-write-wins today (a correctness gap).

These are one symptom: high-frequency, small, mutable metadata living in object
storage, which object storage is bad at.

## 2. Approach

Split the planes, exactly as Aiven Inkless (Postgres batch coordinator) and
KafScale (etcd) do, but keep tansu's "object storage is the source of truth,
pods are disposable" identity:

- **Data plane** — record batches stay immutable in object storage (we keep the
  `#13` win). The S3 object key becomes a UUID; the offset is a *logical overlay*
  held by the coordinator, not encoded in the key.
- **Metadata plane** — a dedicated **metadata coordinator** holds the hot mutable
  state (offset counters, batch index, group offsets, producer sequences,
  transaction state). It is backed by **SlateDB → object storage**, so the
  coordinator pod itself is **disposable**: durability lives in S3, not on a
  precious PVC. This is the constraint that ruled out Postgres/etcd/Redis as the
  authority (all hold the durable copy on a stateful disk).

Offsets are assigned **at commit time** (not pre-reserved ranges): the offset
advances only when a batch commits, so a crash between the S3 upload and the
commit leaves an orphan object (GC'd by `maintain`/retention), never an offset
gap, and never a high-watermark stall.

Brokers stay stateless and symmetric: they are RPC *clients* of the coordinator,
own no partition, and hold no precious state.

## 3. Hot/cold split (the key to fast recovery)

Recovery time is bounded by the size of the mutable state to restore. We classify
metadata so that the recoverable working set is tiny:

| Class | Contents | Size | On recovery |
|-------|----------|------|-------------|
| **Hot-mutable** | per-partition `high_watermark`/`last_stable_offset`/`log_start`, active producer epochs+sequences, in-flight txn state | tiny (a few KB × partitions) | must be restored instantly ⇒ frequent SlateDB checkpoints |
| **Cold-append** | batch index (offset → S3 object), aborted-txn index | unbounded but **immutable** | not needed in memory ⇒ served from SSTs on S3 via block cache, warmed lazily |

Only the hot-mutable set gates restart time. The append-only batch index, never
mutated, does not slow a relaunch.

## 4. Topology and fast failover

> **Revised after the Phase-0 spike** (see #18). The original design assumed the
> standby would be a SlateDB *reader* warm-tailing the WAL, so that promotion was
> near-instant. The spike showed a `DbReader` in SlateDB 0.10.1 does **not** track
> a live writer (its view freezes near its first-established snapshot). So the
> standby cannot be assumed near-current, promotion is a **cold `Db::open`**, and
> the failover budget is governed by **checkpoint cadence + WAL coalescing**, not
> by a warm reader. This actually simplifies the design — no tail machinery.

```
   brokers (stateless) ──RPC──► coordinator ACTIVE ──writer──► SlateDB / S3
        (clients)                      │                            ▲
                                       │                            │ Db::open on
                                 coordinator STANDBY ────────────────┘ promotion
                                 (idle, fence-ready)
```

- **Active**: single SlateDB writer, serves *all* RPCs (including fresh reads,
  from its in-memory state), flushes to S3, **checkpoints frequently** (this is
  the failover lever — see §6.0 and §8).
- **Standby**: an idle pod ready to `Db::open`-as-writer on failover. It does
  **not** tail; it holds no live view. Its only job is to take over fast and let
  SlateDB fence the old writer. (A warm block/object cache helps the cold open a
  little but is not required for correctness.)
- **Two independent mechanisms**:
  - *Liveness / election* — a **Kubernetes Lease** (`coordination.k8s.io/Lease`,
    already in the API server, no new infra). The active renews it; the standby
    takes over on expiry.
  - *Safety / anti-split-brain* — SlateDB's **manifest epoch fencing**, **verified
    in the spike**: after the standby `Db::open`s, the old writer's next `write`
    *and* `flush` both error. Even if the Lease is wrong, SlateDB guarantees a
    single writer.

**Failover sequence**

1. Active dies (T0).
2. Lease expires (TTL, e.g. 1–2 s); standby detects it.
3. Standby `Db::open` as writer ⇒ epoch++ ⇒ old writer fenced.
4. Replay the WAL since the last durable checkpoint (bounded — see below).
5. Standby serves RPCs.

**Recovery budget (measured on minio, extrapolated to GCS at ~20 ms/GET):**
`Db::open` replays one object-store GET per WAL object since the last checkpoint.
With **group commit** (§6.0) coalescing many commits per WAL object, plus frequent
checkpoints, the replay stays small: `open_GCS(k) ≈ 0.2 s + 20 ms × k` where `k` is
*WAL objects* since the checkpoint. Bounding `k` to a few dozen keeps failover
within **~1–3 s** (vs the current ~30 s collapse). During the window, produce
*blocks and retries* — no data loss, because offsets advance only at commit, and
group commit only acks after the group is durable.

## 5. SlateDB key schema

SlateDB is an ordered (LSM) KV; we encode keys so lexicographic order = numeric
order. Conventions: 1-char namespace prefix; `topic` = 16-byte topic UUID;
`partition` = `i32` big-endian (4 bytes); offsets/timestamps = `i64` big-endian
(8 bytes). All values are `bincode`/`serde` structs unless noted.

```
# --- HOT-MUTABLE (checkpointed frequently) ---

# per-partition offsets  (the hot write: one put per batch commit)
key:  'w' | topic(16) | partition(4)
val:  { log_start_offset: i64, high_watermark: i64, last_stable_offset: i64 }
      # high_watermark = LEO (all records); last_stable_offset = LSO (read_committed)

# producer epoch registry (idempotent + transactional producers)
key:  'pe' | producer_id(8)
val:  { epoch: i16, transaction_id: Option<String>, last_seen_ms: i64 }

# per (producer, partition) sequence window for idempotent dedup
key:  'ps' | producer_id(8) | topic(16) | partition(4)
val:  { last_seq: i32, window: VecDeque<(base_seq: i32, base_offset: i64, count: i32)> }
      # keep last 5 entries (Kafka's dedup window) to answer duplicate retries

# transaction coordinator state
key:  'xt' | transaction_id(var)
val:  { producer_id: i64, epoch: i16, state: Empty|Ongoing|PrepareCommit|
        PrepareAbort|CompleteCommit|CompleteAbort, timeout_ms: i32,
        started_ms: i64, partitions: Set<Topition>, pending_group_offsets: ... }

# --- COLD-APPEND (lazy from S3, not gating recovery) ---

# batch index: offset -> S3 object. range-scannable by offset.
key:  'b' | topic(16) | partition(4) | base_offset(8)
val:  { last_offset: i64, object_key: Uuid(16), byte_size: u32,
        base_ts: i64, max_ts: i64 }

# batch-by-timestamp index (only if list_offsets-by-timestamp is needed)
key:  't' | topic(16) | partition(4) | max_ts(8) | base_offset(8)
val:  ()   # key-only; seek to first batch with max_ts >= target

# aborted-transaction ranges (for read_committed FetchResponse)
key:  'ab' | topic(16) | partition(4) | first_offset(8)
val:  { producer_id: i64, last_offset: i64 }

# --- GROUPS ---

# committed consumer-group offsets
key:  'go' | group_len(2) | group(var) | topic(16) | partition(4)
val:  { committed_offset: i64, leader_epoch: i32, metadata: String, commit_ms: i64 }

# group metadata/state (membership, generation) — versioned for update_group CAS
key:  'gm' | group(var)
val:  { detail: GroupDetail, version: u64 }

# --- CLUSTER / TOPIC (low frequency) ---
key:  'mt' | topic(16)            -> TopicMetadata { name, partitions, configs, id }
key:  'mn' | name(var)            -> topic UUID            # name -> id index
key:  'mb' | broker_id(4)         -> DescribeClusterBroker
key:  'mc' | resource(var)        -> configs
key:  'mu' | user(var) | mech(1)  -> ScramCredential
```

**Range-scan patterns**

- `locate_batches(tp, fetch_offset, max)`: seek to `'b'|tp|fetch_offset`, step
  back one key to the batch whose `base_offset <= fetch_offset` (covers it iff
  `last_offset >= fetch_offset`), then forward-scan up to `max`. No LIST.
- `offset_stage(tp)`: single get of `'w'|tp` ⇒ `(log_start, high_watermark,
  last_stable_offset)`. No LIST.
- `list_offsets` earliest/latest: from `'w'|tp`. By timestamp: range-scan `'t'|tp`.
- aborted txns for a fetch: range-scan `'ab'|tp` over `[fetch_offset, hwm]`.

## 6. Commit protocol (idempotence + EOS)

SlateDB is a single writer and supports atomic multi-key `WriteBatch`. With a
single writer there are no locks/MVCC to design: the active coordinator processes
commands serially (single-threaded command loop or a mutex), so offset assignment
is an in-memory counter increment persisted atomically. This is *simpler* than
the Postgres `SELECT ... FOR NO KEY UPDATE` path.

### 6.0 Group commit (the write path) — added after Phase 0

The spike (#18) showed the two naive options are both wrong:

- `await_durable=true` (SlateDB default): every commit blocks on the next WAL
  flush tick (~`flush_interval`, measured **~52 ms/commit**) and writes ~1 WAL
  object per commit → slow produce *and* slow recovery (every commit-since-
  checkpoint is one GET to replay).
- `await_durable=false`: commit returns in ~0 ms and the flush coalesces many
  commits per WAL object (**13× faster recovery** in the spike) — **but** it acks
  before the data is durable, so a crash loses acked commits whose offsets were
  already handed to producers. Unacceptable for the coordinator.

**The coordinator therefore uses group commit:** accumulate the commits that
arrive within a ~`flush_interval` window, persist them in one `WriteBatch`, do
**one** durable flush, then ack the whole group. This captures the coalesced
speed and small WAL-object count (fast recovery) *and* durable-on-ack safety.
Produce latency ≈ the (tunable) flush window, amortized across the group rather
than paid per commit. It also relaxes the checkpoint cadence by ~the group size
(N commits → 1 WAL object), which is what keeps the §4 failover budget in range.

### 6.1 RPC contract (broker → coordinator)

```
# hot path
commit_batch(tp, record_count, object_key, byte_size, base_ts, max_ts,
             producer_id?: i64, producer_epoch?: i16, base_seq?: i32)
    -> Result<assigned_base_offset: i64, ProduceError>

# read path
locate_batches(tp, fetch_offset, max) -> [(object_key, base_offset, last_offset, byte_size)]
offset_stage(tp)                       -> (log_start, high_watermark, last_stable_offset)
list_offsets(isolation, [(tp, ListOffset)]) -> [(tp, ListOffsetResponse)]

# groups (port of the pg.rs semantics onto SlateDB)
offset_commit / offset_fetch / committed_offset_topitions
update_group(group, detail, version) -> Result<version, UpdateError>

# producers / transactions
init_producer(transaction_id?, timeout_ms, producer_id?, producer_epoch?)
txn_add_partitions / txn_add_offsets / txn_offset_commit
txn_end(transaction_id, producer_id, producer_epoch, committed: bool)
```

Transport: reuse the `rama`-based service-layer pattern already used by
`tansu-service` / `tansu-client`, or `tonic`/gRPC. Brokers discover the active
coordinator via the Lease holder (or the K8s Service) and **retry transparently**
on `FENCED`/timeout during the failover window.

### 6.2 Idempotent produce

Broker uploads the batch object to S3 (UUID key) first, then calls
`commit_batch`. Coordinator, serially:

1. If `producer_id` present, validate against `'pe'|producer_id`:
   - `producer_epoch < stored.epoch` ⇒ `INVALID_PRODUCER_EPOCH` (fenced).
2. Validate sequence against `'ps'|producer_id|tp` (`expected = last_seq + last_count`):
   - `base_seq == expected` ⇒ accept.
   - `base_seq < expected` ⇒ **duplicate retry**: return the `base_offset` from
     the dedup window (idempotent); `OUT_OF_ORDER_SEQUENCE_NUMBER` only if it
     fell out of the 5-entry window.
   - `base_seq > expected` ⇒ `OUT_OF_ORDER_SEQUENCE_NUMBER` (reject).
3. `base_offset = w[tp].high_watermark` (in-memory authority).
4. Atomic `WriteBatch`:
   - put `'w'|tp` with `high_watermark += record_count` (and `last_stable_offset
     += record_count` for **non**-transactional batches only).
   - put `'b'|tp|base_offset` = `{ last_offset, object_key, byte_size, base_ts, max_ts }`.
   - put `'t'|tp|max_ts|base_offset` (if timestamp index enabled).
   - if producer: update `'ps'|producer_id|tp` (push to window, set `last_seq`).
5. Commit `WriteBatch` (durable to WAL/S3), then advance in-memory state.
6. Return `base_offset`.

Single-writer serialization guarantees contiguous, distinct offsets per
partition with no race. Crash before step 5 ⇒ orphan S3 object, no offset
consumed.

### 6.3 Transactions (EOS)

Transactional batches advance `high_watermark` (LEO) but **not**
`last_stable_offset` (LSO). `read_committed` consumers see up to LSO; the batch
index entries beyond LSO are withheld until the txn resolves.

- `init_producer(transaction_id)`: allocate/return `producer_id`, bump epoch in
  `'pe'`, set `'xt'` state `Empty` (fences previous producer of the same txn id).
- `txn_add_partitions` / `txn_add_offsets`: record partitions/group in `'xt'`,
  state ⇒ `Ongoing`.
- `commit_batch` with a transactional producer: as 6.2 but step 4 advances **only**
  `high_watermark` (LSO stays); the batch's offset range is implicitly pending via
  `'xt'.partitions`.
- `txn_end(committed = true)`: atomic `WriteBatch` — for each partition, write the
  commit control marker (its own batch object + `'b'` entry), advance
  `last_stable_offset` to include the txn's records, apply pending group offsets
  (`txn_offset_commit`), set `'xt'` ⇒ `CompleteCommit`.
- `txn_end(committed = false)`: atomic `WriteBatch` — write abort markers, insert
  `'ab'|tp|first_offset` for each partition's range, advance `last_stable_offset`
  past the resolved range (records remain but are filtered by `read_committed`),
  set `'xt'` ⇒ `CompleteAbort`.

Because `txn_end` is a single atomic `WriteBatch` under the single writer, commit
and abort are all-or-nothing across partitions.

`fetch` with `read_committed`: serve batches up to `last_stable_offset` and attach
the `'ab'` ranges in `[fetch_offset, lso]` to the FetchResponse so the client
filters aborted records.

## 7. Mapping onto the existing `Storage` trait

A new `StorageContainer` variant (e.g. `hybrid://` or `coordinator://`) implements
`Storage` (`tansu-storage/src/lib.rs:1340`) by delegating:

- `produce` ⇒ S3 PUT (DynoStore data half) + `commit_batch` RPC.
- `fetch` ⇒ `locate_batches` RPC + S3 GET(s).
- `offset_stage` / `list_offsets` ⇒ RPC (no LIST).
- `offset_commit` / `offset_fetch` / `update_group` / `init_producer` / `txn_*`
  ⇒ RPC (semantics already exist in `pg.rs`, ported to the SlateDB keyspace).

The coordinator itself reuses tansu's existing `slatedb://` backend integration
(feature `slatedb`) — it is not a net-new store.

## 8. Phasing

- **Phase 0 — Spike (decision gate). ✅ DONE (#18).** Validated on minio: fencing
  works (old writer fenced on 2nd open), `Db::open` recovery ≈ 0.2 s + 20 ms × k
  WAL-objects extrapolated to GCS, group commit is the right write path, and the
  `DbReader` does not usefully tail a live writer (so the standby is a cold open,
  not a warm tail). No conceptual blocker; §4 and §6.0 revised accordingly.
- **Phase 1 — Single coordinator, no standby.** Coordinator service + SlateDB
  keyspace (hot-mutable + batch index) + broker→coordinator RPC + `hybrid://`
  backend + **group commit** write path. Idempotent produce, fetch, `offset_stage`,
  group offsets. Kills the produce hot path; recovery = cold-open from S3 (accept
  brief restart downtime).
- **Phase 2 — Standby + fast failover.** Fence-ready standby pod + K8s Lease
  election + SlateDB epoch fencing + a checkpoint-cadence policy that bounds the
  cold-open replay. Delivers "relance très vite". (No warm-tailing reader — see §4.)
- **Phase 3 — Transactions / EOS.** LSO, aborted-txn index, control markers, txn
  coordinator state.
- **Phase 4 — Sharding by partition.** N coordinators, each owning a partition
  subset (its own SlateDB). Shrinks blast radius, speeds per-shard recovery,
  scales the metadata plane horizontally.

## 9. Trade-offs and open questions

- The active coordinator is a single writer ⇒ produce stalls cluster-wide during
  the (bounded) failover window. Sharding (Phase 4) shrinks the blast radius.
- A new internal RPC surface (broker→coordinator) is added; brokers must handle
  failover/`FENCED` transparently.
- Failover is a cold `Db::open` bounded by WAL-objects-since-checkpoint (Phase 0).
  The knobs are checkpoint cadence + group-commit coalescing; an over-long
  checkpoint interval pushes failover past the 1–3 s budget.
- `DbReader` not tailing a live writer (Phase 0) is taken as given here; if a
  future SlateDB version supports low-lag tailing, a warm standby could shave the
  cold-open further, but the design must not depend on it.
- Control-marker representation for EOS: reuse tansu's existing control-batch
  encoding vs synthesise markers in the coordinator — to be settled in Phase 3.
- `delete_records` / retention advances `log_start_offset` in `'w'` and trims the
  `'b'` index; orphan-object GC stays in `maintain`.
```
