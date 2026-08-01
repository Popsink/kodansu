# Design: leaseless multi-writer prefix-coalesced segments (SCOS)

> **North star.** A prefix's segment sequence is a dense, create-only name space in
> the object store; that create-only compare-and-set *is* the offset arbiter. No
> lease, no forwarding, no membership — coordination lives entirely in S3, and any
> pod may append to any prefix.

Status: draft / RFC. Supersedes the prefix-owner produce routing of #70 (PR #74).
Successor to the virtual-topics epic #56.

## Problem

Prefix coalescing (#56) enforces a **single writer per prefix** via an S3
conditional-write lease (`prefixes/{prefix}/lease.json`, etag CAS + epoch fence).
On a multi-replica deployment behind a load balancer, a producer whose connection
lands on a non-lease-holder cannot acquire the lease and is fenced persistently —
the `NotLeaderOrFollower` flush storm observed in production (#70).

The shipped fix (#70 / PR #74, "Option A") forwards produce to the deterministic
prefix owner. It works, but it imports three things that contradict Kodansu's
stateless model:

1. a **per-pod identity** (`routing_node_id`) — requires stable ordinals → StatefulSet;
2. a **static member list** (`members=id@addr,…`) — every pod must know every other, reconfigured on each scale;
3. **broker-to-broker awareness**, whereas Kodansu's design principle is that brokers coordinate *only* through the object store (logical node id fixed at 111, pods interchangeable behind an L4 LB).

Production runs a **ReplicaSet**, whose pods are ephemeral with no stable
ordinal or enumerable address. `members=` + `routing_node_id` do not template
cleanly onto it. So #70 is close to unusable as-is in the target environment.

## The impossibility that frames everything

A Kafka offset is, per `(topic, partition)`: **dense, contiguous, monotonic,
assigned at append time, returned in the produce response, and stable as an
identity forever** (consumers seek/commit by it). Under concurrent writers this
requires every append to be *totally ordered before acknowledgement* — i.e. a
linearizable primitive. No read-time / footer-only resolver avoids it: the
overlap resolver must *drop* one side of an offset-range overlap, and without a
fence both sides are already-acked data → data loss.

Conclusion: **some per-append serialization is unavoidable.** The design question
is only *how cheap and how membership-free* that primitive is. #70's forwarding
is one valid primitive (needs addressing); the segment-sequence CAS is another
(lives in S3, needs nothing).

## Decisions taken

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | The create-only **segment-sequence CAS is the offset arbiter**; delete the produce lease, forwarding, membership. | Purest stateless fit; deletes the #70 machinery; the primitive already exists (`assign_and_create_segment`). |
| D2 | **Single delivery on footer v2** (no interim v1 phase). | Ships exact log-based dedup + ambiguous-PUT safety at once; avoids a second format migration. Cost: the stateless fix lands with v2, not sooner. |
| D3 | **EOS / transactions: not supported at throughput; scope CDC-only.** | Transaction state lives in the global `meta.json` (~4–5 CAS/txn) → ~0.2 txn/s on GCS *regardless of this design*. Target workload is 100% non-transactional CDC. `meta.json` sharding is a separate follow-up, out of this program. |
| D4 | **Path routing is a static function of the topic**, never of batch attributes/size/state — one offset authority per sub-stream. | Two authorities on one sub-stream (segment vs legacy `records/`) mint colliding offsets across pods. |
| D5 | **Migration is quiesce-and-flip**, never RollingUpdate. | A mixed lease/leaseless fleet is *corrupt*, not merely degraded (see Migration). |

## The design (SCOS — sequence-CAS optimistic stamping)

### Object layout

Unchanged from #56: `clusters/{cluster}/prefixes/{prefix}/segments/{seq:020}.seg`,
create-only, monotonic sequence, self-describing footer. **`lease.json` is
deleted.** `compaction-lease.json` stays (compactor-vs-compactor politeness, not a
produce-path fence).

### Append protocol (per prefix, per flush window, on any pod)

Under the existing per-process per-prefix flush lock (kept — see below):

1. **Fold-before-claim.** Ingest the footers of every live segment `< candidate`
   into the in-memory prefix index **and** fold each entry's tail into the
   per-`(topic,partition)` next-offset hint. The candidate seq is `max folded
   seq + 1` (or the seq floor on a cold/empty prefix). *This ingest must bypass
   the index freshness TTL* (obligation O1).
2. **Derive bases** per buffered sub-stream from the folded hints
   (`max(index tail, persisted watermark floor)`).
3. **Encode** the segment body (batch wire bytes, verbatim — offsets live only in
   the footer) once; the footer carries the derived bases and a per-flush
   **nonce**.
4. **`PutMode::Create`** at `segments/{candidate}.seg`.
   - **Success** = the linearization point: fold own footer, advance hints, ack
     every parked producer with its assigned offset. Steady state: **1 PUT, no
     other request.**
   - **`AlreadyExists`**: read the winner's footer (2 ranged GETs — no LIST), fold
     it, **re-derive bases, re-encode the footer only** (body bytes and byte
     ranges are offset-independent and reused verbatim; only the footer's
     `base_offset` fields change), retry `candidate+1`. Bounded by `MAX_ATTEMPTS`.
   - **Ambiguous** (network error after send): probe the footer at `candidate`;
     if its nonce is ours the create succeeded — adopt it; else fold and advance.
5. **Never ack before the create succeeds**; a terminal failure fails the parked
   producers for client retry.

This is a strict generalization of the legacy `assign_and_create` (name collision
→ resync tail → retry at the next free slot), except one create claims offsets for
many sub-streams at once, and "tail" means "fold the winners' footers".

### Why the per-process flush lock stays

Not as the offset authority (the CAS is), but for **per-partition FIFO**: two
concurrent local flushes of one prefix hold disjoint buffers whose batches are in
producer-sequence order across them; serializing local claims keeps buffer order
= seq order = offset order on one pod. It also avoids guaranteed self-conflicts.

### Correctness (sketch)

Under: (A1) atomic conditional create — exactly one winner per name, a loser
writes nothing; (A2) strong list/read-after-write on S3 + GCS; (A3) immutability +
**no name reuse** (obligation O3); (A4) fold-before-claim; (A5)
durable-before-advance; (A6) single authority per sub-stream (D4):

- **Sequence density / total order.** A writer reaches seq `N` only after
  witnessing `N-1` (own create, listing, or an `AlreadyExists`). Created seqs form
  a dense, totally ordered spine; the create is the linearization point.
- **No overlap / no reuse.** For `i < j` both carrying `(t,p)`, `j`'s creator
  folded `i`'s footer before finalizing bases (A4 + the conflict loop), so
  `base_j ≥ tail_i`; ranges are disjoint. The TOCTOU window (compute at `c`, rival
  commits `c`) is caught by `AlreadyExists` on `c`.
- **No gap.** Bases derive from a monotone max-fold of durable tails + the
  persisted floor, which never exceeds the true tail → each base equals the prior
  tail exactly.
- **No duplicate.** Each batch enters one buffer, flushed by one task; a lost CAS
  wrote nothing; the ambiguous-PUT nonce prevents a double-write on retry.
- **Zombies are harmless** (not the main hazard, as under a lease): a GC-paused
  writer waking late simply loses the CAS, folds, and catches up before it can
  commit. Epoch fencing stops being load-bearing on the produce path.

`valid_substream_segments` becomes vacuous in steady state and stays as
defense-in-depth for the compaction write→delete window.

## Footer v2

`SEGMENT_FORMAT_VERSION` 1 → 2 (readers accept {1,2}; v1 entries fold as
"coordinate-less"). Per `SubstreamEntry`, append a per-idempotent-batch
producer-coordinate list, in region (offset) order:

```
pcoord_count  u16
pcoords[]:
    producer_id     i64
    producer_epoch  i16
    base_sequence   i32
    last_sequence   i32     # wrap-aware
    offset_delta    u32     # batch base_offset = entry.base_offset + offset_delta
```

- **`offset_delta`, not absolute offset** — invariant under the conflict-correction
  rebase (only `entry.base_offset` changes), so the re-encode stays coherent with
  zero recomputation.
- A per-flush **nonce** lives in the footer trailer (not by overloading
  `writer_epoch`) for ambiguous-PUT adoption. `writer_epoch` is written as an
  **era marker** (see Migration), and the overlap tie-break degenerates to seq.
- Coords are **folded into a derived `ProducerTable` at ingest and dropped** — not
  kept in the cached footer — so the in-memory index footprint stays at v1 levels.

External S3-direct readers (kotatsu#82) must accept v2 before any v2 write; a v2
segment reaching a v1 reader is a hard read failure by contract.

## Log-based idempotent dedup

The producer coordinates are *already* in the log (Kafka `RecordBatch` headers,
copied verbatim into segment bodies); v2 surfaces them at footer cost. Per prefix,
a derived

```
ProducerTable: Map<(ProducerId, Topition), Tail { epoch, last5 window, folded_thru_seq }>
```

is built by folding footers in sequence order over the winner set — a **pure
function of the footer set**, so every pod converges to the same table. This is
the point: the arbiter object (the segment CAS) and the dedup state are the same
artifact.

Produce-time validation classifies `(pid, epoch, base_seq)` against
`table ⊕ reservations`: admit / duplicate (ack with the *original* offset — Kafka's
last-5 window, restoring correct semantics) / out-of-order. On the error path only,
if the index is not provably fresh, force one refresh + reclassify — this closes
the false-`OutOfOrderSequenceNumber` on connection migration. On a CAS conflict,
any parked batch whose coords now appear in a folded winner (the same batch raced
in through another pod after an LB reshuffle) is dropped and acked with the
winner's offset — **this is what closes the cross-pod dedup window** that both
leaseless designs otherwise inherit.

`producers/{id}.json` (#48) is demoted from authority to: (1) registration + epoch
authority (eager, synchronous); (2) a write-behind, never-ahead-of-log backstop
for producers idle past segment retention; (3) authority for non-segment paths.

## Single-authority consolidation (D4)

The routing invariant: **path assignment is a static, pure function of the
topic.** Every batch of a segment-routed topic — data, transactional, control,
backfill — goes through segments; compacted topics (`cleanup.policy=compact`) stay
on the legacy `records/` path.

- **Txn / control batches → segments.** Free mechanically: the txn/control bits are
  in the batch wire bytes, preserved by the verbatim copy; a control marker is a
  1-record region, offset arithmetic stays contiguous. No format change beyond v2.
  Txn produce-offset registration moves after the flush ack.
- **Backfill fold-in (#75).** Delete the #62 size/state bypass; a large batch trips
  the byte threshold and flushes as ≈its own segment (1 PUT, cost parity). Requires
  `coalesce_bytes ≥ 32 MiB` for backfill-heavy prefixes (at the 1 MiB default it is
  a ~10–20× snapshot regression, because a prefix serializes on one segment-create
  RTT at a time). **Do not pipeline** seq `N+1` before `N`'s PUT completes (a
  failed `N` after `N+1` lands mints an offset gap). Bigger segments are the safe
  throughput lever, not pipelining.
- **Compacted topics** stay excluded (static exclusion is correct here). Guard:
  **reject `cleanup.policy` alterations** that change a segment-routed topic's path
  when it already has segments (silently broken today).

## Read path, high watermark, retention

- Read path unchanged (footer index + ranged GET; offsets from the footer).
- Cross-pod HWM visibility is bounded by the hint TTL (5 s). Fix a pre-existing
  compounding to ~2×TTL (stamp `mark_listed` with the index's `refreshed_at`, not
  `now`).
- **Seq floor (obligation O3).** Whole-segment retention — and compaction writing a
  merged segment at the *tail* seq with *old* data that can then expire before
  lower hot seqs — can free a seq *name* for re-creation while peers cache the old
  footer → stale ranged reads. Persist a per-prefix `next_seq_floor`, **write-ahead
  on the delete paths only** (retention + compaction), fold it into `tail_next_seq`.
  Mutation rate: once per maintenance tick per prefix that deleted something — off
  the hot path. **This is a live bug today, independent of this design.**

## Migration / cutover (D5)

A **mixed lease + leaseless fleet is corrupt**: a leaseless pod never touches the
lease, so it cannot fence an old lease-holder; the old pod's straggler flush wins
its seq CAS and its lease-era epoch beats the new pod's on the overlap tie-break —
**silently erasing data the new pod already acked**. Therefore:

1. **Quiesce** — scale the old ReplicaSet to zero (`Recreate`, or scale-to-0 then
   deploy). CDC producers buffer/retry through the gap.
2. **Drain** — wait past the object-store client request timeout (≥60–90 s) after
   the last old pod exits, so no old process can still have a PUT in flight. Lease
   *expiry* is **not** the gate.
3. **Epoch seeding** — the first leaseless flush of each prefix seeds
   `era_epoch = max(lease.json epoch, max footer epoch) + 1` (never 0) and stamps
   it as a constant era marker in every segment.
4. **Format** — v2 coexists with v1 per-object; readers (broker + external) must
   accept v2 first. Rollback after any v2 write requires a segment-rewrite tool —
   hence a soak window before v2 writes are enabled.
5. **Start** leaseless pods; delete #70 routing config.

Rollback: reverse flip with the same quiesce discipline; rewrite each active
prefix's `lease.json` epoch above `era_epoch` before restarting old pods.

## Scope / non-goals

- **EOS / transactions**: not supported at throughput (D3); the `meta.json` ceiling
  is documented in storage-tuning. Semantics (LSO/markers/commit-abort) are
  unchanged from today — including the pre-existing empty `aborted-transactions`
  read-committed gap, tracked separately.
- **Idempotent dedup** is exact (v2 log-based); the residual is producer state lost
  after a producer idles past segment retention (bounded, matched to Kafka's
  `producer.id.expiration`), covered by the backstop.
- Consumer-group coordination and fetch long-poll staleness are orthogonal.

## Proposed sub-issues

1. Seq floor: write-ahead per-prefix `next_seq_floor` on retention + compaction; `tail_next_seq` honours it. *(ships independently — fixes a live bug)*
2. Single-authority routing: static per-topic path; route txn/control through segments; drop the #62 backfill bypass; `cleanup.policy` alteration guard. *(ships independently — fixes a live bug)*
3. Fold-before-claim off a non-TTL'd ingest; conflict loop re-derives bases + re-encodes footer; drop the lease from the produce path.
4. Footer v2: pcoords + trailer nonce; encode/decode; kotatsu#82 reader update.
5. Log-based dedup: `ProducerTable`; produce-time validation; demote `producers/{id}.json`; producer-state expiry.
6. Ambiguous-PUT nonce adoption.
7. Backfill fold-in + adaptive `coalesce_bytes`.
8. HWM cross-pod TTL fix; conflict-path `tail_next_seq` LIST removal; jittered linger.
9. Migration tooling + runbook (quiesce-and-flip, epoch seeding, rollback).
10. Delete #70 (`routing_node_id`, `members`, `PrefixRouter`, `route_produce`, `prefix_owner_node`).

## Open questions

- **Epoch fencing without a leader.** A warm producer-epoch cache can admit a
  stale-epoch batch for up to one flush before a folded footer reveals the bump;
  an admitted-and-committed batch is acked and cannot be retracted. Interim: TTL
  the epoch cache + fence-at-conflict. A full fix (fencing marker through the
  prefix CAS) is unscoped.
- **Sequence wraparound** (i32) is mishandled today (`advance` + max-merge
  `reconcile`); the fold/last-5 comparisons must be wrap-aware and the backstop
  needs a schema that survives wrap.
- **Conflict economics without owner affinity.** If the LB truly sprays one
  prefix across many pods, every flush eats a conflict. Consider keeping rendezvous
  as an *advisory* append-local-else-hint (no membership correctness required).
- **`InitProducerId` herd** on `meta.json` under mass reconnect — same class the
  design removes from produce; needs create-only/sharded id allocation.

## References

- #56 — virtual-topics epic. #59 — single-writer lease + epoch fencing. #60 —
  footer read index. #61 — per-prefix segment retention. #64 — segment footer
  format. #66 — segment compaction. #70 / PR #74 — prefix-owner routing (superseded
  by this design). #75 — backfill fold-in (subsumed by sub-issue 7). #48 — producer
  checkpoint. kotatsu#82 — external S3-direct reader contract.
- `docs/virtual-topics-format.md` — segment/footer wire contract (extended to v2).
- `docs/design-metadata-coordinator.md` — related multi-replica RFC.
