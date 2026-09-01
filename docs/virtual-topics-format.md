# Virtual-topics segment format (external-reader contract)

Prefix-coalesced "virtual topics" (#56) store many topics' records in shared,
immutable **segment** objects:

```
clusters/{cluster}/prefixes/{prefix}/segments/{seq:020}.seg
```

`{seq}` is a zero-padded segment sequence: a unique, create-only object name
(the create is the write-ordering authority for producing), **not** an offset.
A reader must **not** assume sequence order equals offset order — **segment
compaction (#66) writes a merged segment covering old offsets under a fresh
(higher) sequence**, so a high sequence can hold low offsets. Offset order comes
from the footer, never from the name. A segment multiplexes the batches of many
sub-streams, and is self-describing: an external S3-direct reader (e.g. kotatsu,
kotatsu#82) locates and decodes any sub-stream from the object alone, with no
external index — resolving `(sub-stream, offset)` through the footer's offset
ranges (on the rare overlap left by a compaction/failover, see
[Resolving overlaps](#resolving-overlaps)).

**What identifies a sub-stream changed at v4 (#442).** It used to be
`(topic, partition)` by **name**, and at v4 it is `(topic_id, partition)` for
any entry carrying a topic id. See
[Sub-stream identity](#sub-stream-identity-v4) — this is the one part of the
contract that is not additive, and a reader that ignores it serves a deleted
topic's records as its successor's.

This document is the **wire contract**. All integers are **big-endian**.

## Layout

```
+-----------------------------------------------------------+
| sub-stream region 0   (contiguous RecordBatch bytes)      |
| sub-stream region 1                                       |
| ...                                                       |
+-----------------------------------------------------------+
| footer            (footer_len bytes)                      |
+-----------------------------------------------------------+
| trailer           (18 bytes, fixed)                       |
+-----------------------------------------------------------+
```

A reader recovers the index with **one** ranged GET of a fixed-size suffix
(64 KiB), which for almost every segment already covers both the trailer and the
whole footer: read `footer_len` from the last 18 bytes, then slice the
`footer_len` bytes immediately before them out of the same buffer. Any leading
record bytes in the over-read are ignored. Only a footer *larger* than the
over-read needs a second, exact GET of the last `18 + footer_len` bytes. The
record body is never downloaded to locate a sub-stream.

### Sub-stream region

Each region is the concatenation of that sub-stream's Kafka `RecordBatch` wire
bytes, in order — byte-for-byte what a legacy single-topic coalesced object
(#50) contains. A region is decoded by walking `base_offset (i64) +
batch_length (i32) + batch_length bytes` repeatedly (the standard batch prefix).
Absolute offsets come from the footer, **not** from the batch headers.

A region's extent holds whole batches: `byte_start` points at a batch prefix and
`byte_len` ends on a batch boundary. So a walk that reads a `batch_length` which
is negative, or which runs past the extent, has not found a short frame — it has
found that the footer entry and the bytes disagree, and the reader should treat
the region as damaged rather than as a region that happens to be empty. The one
exception is a *short read*: a ranged GET that returns fewer bytes than
`byte_len` is a truncated read of a whole region, and the partial tail is
ignored (#386).

### Footer

Four versions exist. **v3 is what production writes by default** — since
`0.7.0-beta.25` (#188), every write emits it: the leaseless flush (#86), merge
compaction, and the per-key compaction rewrite alike. **Nothing emits v2 or v1
any more**; both remain readable so segments written before that release stay
readable in place. **v4 (#442) is emitted only by a deployment configured with
`segment_format=4`.** A reader MUST accept all four, and MUST reject any other
version.

The practical consequence for an external reader: **v3 support is not optional,
and v4 support is not optional either.** Every segment written by a current
broker is v3, so a reader that implements only v1 and v2 fails on the whole
bucket, not on a subset — and the moment a deployment raises `segment_format`,
the same is true of v4. Because the version follows the *writer*, not the
content, that flip makes every new segment in every prefix v4 at once.

```
writer_epoch      i64          # see "writer_epoch" below
nonce             u64          # v2+ ONLY — per-flush write identity (#89)
entries[entry_count]:
    topic_len     u16
    topic         [topic_len]  # UTF-8
    topic_id      [16]         # v4+ ONLY — nil uuid = "no id" (#442)
    partition     i32
    base_offset   i64          # absolute offset of the region's first record
    record_count  i64          # last_offset = base_offset + record_count - 1
    byte_start    u64          # region offset within the segment
    byte_len      u64          # region length in bytes
    max_timestamp i64          # greatest record timestamp in the sub-stream
    # ---- the following block is v2+ ONLY ----
    pcoord_count  u16          # producer coordinates for this sub-stream
    producers[pcoord_count]:
        producer_id     i64
        producer_epoch  i16
        base_sequence   i32    # -1 on a control batch (transaction marker)
        last_sequence   i32    # base_sequence + last_offset_delta (wrapping)
        offset_delta    u32    # batch's first offset - entry base_offset
        # ---- v3 ONLY ----
        flags           u8     # bit 0 transactional, bit 1 control;
                               # bits 2-7 written 0, ignored on read
```

`entry_count` comes from the trailer, not from a count inside the footer.

**A v2 reader MUST parse (or skip by `pcoord_count`) the `producers` block even
if it ignores its contents**: a reader that stops at `max_timestamp` — i.e. one
written against the v1 layout — leaves its cursor mid-entry and mis-decodes every
following entry. This is the single most likely way to get v2 wrong. The same
applies at v3 with one more trap: **a v3 coordinate is 23 bytes, not 22** — a
reader that skips the `producers` block by size, or walks it with the v2 stride,
mis-decodes every following coordinate and entry.

At v2 a coordinate is emitted per **idempotent** batch. At v3 the emission rule
widens: a coordinate is emitted for every batch that is idempotent,
transactional, or control, with `flags` derived from the batch's attribute
bits. A transaction marker's coordinate carries its real
`producer_id`/`producer_epoch`, `base_sequence = last_sequence = -1` (markers
are not idempotent-sequenced), and `flags = 0b11`.

The `producers` block exists so idempotent-producer dedup is derivable from the
log itself (#88): it records, per idempotent batch, which producer/epoch/sequence
range landed at which offset. An external reader that only serves records can
skip it; one that wants to recognise duplicates does not have to keep separate
state.

To read a sub-stream from `offset`: find its entry, ranged-GET
`[byte_start, byte_start + byte_len)`, decode the region, and assign offsets
running from `base_offset`.

#### Sub-stream identity (v4)

**A sub-stream is identified by `(topic_id, partition)` when its entry carries a
non-nil `topic_id`, and by `(topic, partition)` otherwise.** The two are
disjoint: an id-keyed entry does **not** answer to its own name, and a name-keyed
entry does not answer to an id.

`topic_id` is absent from the layout below v4, and a v4 entry writes the **nil**
uuid for a name-keyed sub-stream — so "the segment is v4" and "this sub-stream is
keyed by id" are independent facts. They have to be: both kinds of topic live in
one prefix, and share one segment, for as long as any topic created before the
flip is alive.

**Why it exists.** A segment is immutable and shared, and is reclaimed whole only
once every sub-stream in it is past retention, so a deleted topic's slices cannot
be removed. Keyed by name, a topic recreated under that name found its
predecessor's slices and folded its offsets from them: watermarks `(10, 13)`
where Apache Kafka answers `(0, 3)` (#442). Keyed by id, a recreation is a
different uuid, so it is a different sub-stream — its predecessor's records are
unreachable rather than hidden, and its log starts at 0.

**How a reader resolves a topic name to an id.** From the topic's own metadata,
`clusters/{cluster}/topic-routing/{name}.json`:

```json
{"prefix": "org.env.conn", "substream_id": "0192…"}
```

`substream_id` is **absent** for a name-keyed topic, which is every topic created
before the flip. The object is immutable for the topic's lifetime, so a reader
may cache it permanently — and MUST re-read it (or drop it) when the topic is
deleted, because the name may be reused by a topic with a different id.

**A reader that ignores `topic_id` is not merely missing a feature.** Resolving
by name against a bucket that has id-keyed topics finds the *predecessor's*
slices for any recreated name, at offsets the live topic also uses. That is
exactly the old/new data mixing this keying exists to make impossible.

#### `writer_epoch`

The field is present in every version and is the overlap tie-break in all of
them, but the writer regimes stamp it differently:

- **leaseless mode (#86, what production runs — v3 segments):** the prefix's
  **era epoch**, read from `prefixes/{prefix}/era.json` (`{"era_epoch": i64}`).
  It is a constant per prefix, not per writer: seeded once as
  `max(last lease epoch, highest footer epoch) + 1`, so it is `>= 1` and every
  leaseless segment out-epochs every pre-cutover lease-era segment on the same
  prefix (#92).
- **compaction:** the merged segment carries the maximum `writer_epoch` of the
  segments it merged (floored at 0). It is written at the deployment's configured
  version regardless of the versions of the segments it merged — so a v4 writer
  compacting v3 inputs emits v4, with the nil uuid on every name-keyed entry it
  carried forward.
- **lease mode (#59) — historical, v1 segments only:** the epoch of the lease the
  writer held, `0` if none. No current writer emits v1; this is here to read
  segments written before `0.7.0-beta.25`.

### Trailer (fixed 18 bytes, at the very end)

```
footer_len    u64
entry_count   u32
version       u16              # 1, 2, 3 or 4 (3 by default; 4 with segment_format=4)
magic         u32              # 0x5453_4547  ("TSEG")
```

## Versioning & backward compatibility

- `version = 1` is the first self-describing multi-topic segment; `version = 2`
  adds the footer `nonce` and the per-entry `producers` block described above;
  `version = 3` (#174) appends one `flags` byte per producer coordinate and
  widens coordinate emission to transactional and control batches; `version = 4`
  (#442) inserts a 16-byte `topic_id` after each entry's `topic` and changes what
  identifies a sub-stream (see
  [Sub-stream identity](#sub-stream-identity-v4)). A reader MUST accept
  `{1, 2, 3, 4}` and MUST reject any other version rather than guessing.

  **v4 is the only version bump so far that is not purely additive.** v2 and v3
  added fields a reader could skip and still answer the same questions the same
  way. v4 adds a field that decides *which records belong to the topic you asked
  for*, so skipping it is not degradation — it is a wrong answer.
- **v3 is what is written, since `0.7.0-beta.25` (#188).** The writer stamps it
  unconditionally — the version follows the writer, never the segment's content —
  so from that release on, *every* new segment is v3, including compaction
  output. v2 and v1 are read-only history.

  This document previously said v3 was accepted on read but not yet emitted,
  which was the plan: publish the layout one release ahead so every reader could
  accept `{1, 2, 3}` before any writer flipped. The flip has happened.
- **v4 is at that same stage, with the gate moved from a release to a flag
  (#442).** Every reader in this repository accepts v4; nothing emits it until a
  deployment sets `segment_format=4`. So the ordering is one deploy rather than
  two: roll the binary everywhere, confirm every reader — internal *and*
  S3-direct external — is on a build that accepts v4 and resolves by
  `substream_id`, then flip.

  Readers before writers, because the version-rejection MUST above means an old
  reader fails **cleanly** on a newer segment, but it fails on *every* segment
  the moment a writer flips, not on a subset — and because a segment is shared,
  one topic's writer flipping takes out reads of the entire prefix.

  **The flip is one-way in practice.** A topic created under the v4 regime has a
  `substream_id` pinned for its lifetime, and a writer put back to v3 cannot
  express that identity: it refuses the write rather than storing records under a
  key nothing reads. Those topics stop being writable; they do not quietly
  degrade. A binary that predates #442 does not model `substream_id` at all and
  *would* do the quiet thing, which is why the roll must precede the flip.
- A **legacy** single-topic coalesced object (#50) has **no trailer**: its last
  bytes are record data, so the trailing `magic` will not equal `TSEG`. A reader
  MUST treat "magic absent" as the v0 case and decode the whole object as a bare
  `RecordBatch` concatenation. This is how coalesced and legacy objects coexist
  in one bucket during migration.

## Transactional and control batches

A sub-stream region can contain transactional data batches and **control
batches** (transaction markers) — routed into segments by #188, the same change
that made v3 the emitted version. Both are ordinary Kafka `RecordBatch` wire
bytes inside the region; the batch's attribute bits say what it is (bit 4
transactional, bit 5 control), and the v3 footer's `flags` mirror those bits
per coordinate.

- A control batch is **metadata, never data**. Its single record's key decodes
  as Kafka's `ControlBatch` (`version i16 + type i16`, type `0` = abort, `1` =
  commit); its value is the `EndTransactionMarker`. An external reader MUST
  skip a control batch's records and MUST NOT count it as a consumer-visible
  message — but it **does occupy one offset**, which the footer's
  `record_count` includes.
- **Read-committed is not derivable from segments alone.** The transaction
  ledger — open transactions, the last stable offset, the aborted-transaction
  list — lives in `clusters/{cluster}/meta.json`, which is **not** part of this
  contract. An S3-direct reader working only from segments is read-uncommitted
  by construction: it sees committed, uncommitted, and aborted records alike,
  plus the markers. If it needs commit semantics it must get them from the
  broker, not from this format.

## Resolving overlaps

Compaction and writer failover can leave two segments claiming the same offset
range for one sub-stream. Sort the candidate entries by ascending `base_offset`,
breaking ties by **higher `writer_epoch` first, then higher `seq` first**, then
sweep in that order. The higher-sequence tie-break is what makes a merged
segment win over the originals it merged: they carry the same epoch, and the
merged segment's sequence is always higher.

What the sweep does with a losing entry depends on what is being computed:

- **Resolving one offset**: drop any entry whose `base_offset` falls below the
  range already covered — the higher-priority entry already answers it.
- **Computing coverage** (what the sub-stream contains — the read path, the
  log-end offset, anything that decides what exists before acting on it): drop
  an entry only when it is **wholly** inside the range already covered. An
  entry that starts inside it but reaches **past** it holds a tail nothing
  else holds; serve that tail, `[covered, entry end)`, and treat only the head
  as superseded.

The distinction is load-bearing. Applying the one-offset drop to coverage
discards the reaching entry's tail — the exact records nothing else holds —
which inflated the audit's loss figure until #460 and, applied by the broker's
own overlap resolver, silently hid and (via compaction, which deletes the run
it merged) destroyed record tails until #461. The only overlap a healthy
history produces — a merged segment's originals, or a rewrite's original
during the write→delete window — is wholly inside its winner, so on healthy
data the two rules agree.

## Notes for readers

- **A topic's prefix — and, since #442, its sub-stream identity — is pinned at
  creation, not derivable from its config (#236).** The mapping lives in
  `clusters/{cluster}/topic-routing/{topic}.json` —
  `{"prefix": "...", "substream_id": "0192…"}`, with `substream_id` **absent**
  for a name-keyed topic — written create-only with the topic, immutable for its
  lifetime, and deleted with it. A reader that needs either fact for a topic name
  must read that object (it can be cached indefinitely, but must be dropped when
  the topic is deleted: the name can come back with a different id).

  Do **not** recompute the prefix from `cleanup.policy`: a compacted topic is
  routed under its own name and everything else under its connector prefix (by
  default the first three dotted components, and the shape is per-cluster — see
  `prefix-shape.json` below), but that derivation is only correct until an
  `AlterConfigs` changes the policy — after which the records stay where they
  were, and only the pin still says where that is. Topics created before this
  object existed have no pin until a broker resolves their routing once, at which
  point it writes the derivation they were already using.
- **The prefix shape** — how many leading components of a topic name form the
  connector prefix, and what separates them — is sealed once per cluster at
  `clusters/{cluster}/prefix-shape.json` — `{"depth": 3, "separator": "."}`
  (#464). It is written create-only by whichever broker first builds a store
  against the bucket and is never rewritten: a broker configured with a different
  shape fails to start rather than joining. A reader that derives prefixes from
  topic names should read it rather than assume `3` / `.`; a reader that resolves
  through the routing pin above does not need it at all. An absent object means
  no broker has built a store here yet.
- **Retention** is whole-segment, per prefix: a segment is deleted only once
  every sub-stream in it is past retention (`max_timestamp`), so a live topic
  never loses a shared segment.
- **Truncation (`DeleteRecords`) is logical, not physical.** A sub-stream's
  truncation floor lives in its per-partition
  `clusters/{cluster}/topics/{topic}/partitions/{partition}/watermark.json`
  (field `truncate`), outside this format: segments are never rewritten, so an
  S3-direct reader working from segments alone may see records below the
  broker's log start. A segment every one of whose sub-stream slices ends
  at/below its floor becomes reclaimable ahead of age-based retention — the
  deletion/`404` contract below applies unchanged.
- **A deleted topic's slices survive, and for a name-keyed topic the floor is
  what hides them (#246).** `DeleteTopics` rewrites each partition's
  `watermark.json` as a tombstone at the deleted log end rather than removing it,
  so a same-named successor starts past whatever it would otherwise inherit. An
  **id-keyed** topic needs none of that — its successor has a different id, so it
  cannot reach the slices at all — and its creation therefore *clears* the floor
  rather than preserving it. A reader that honours `truncate` sees the right
  thing either way; one that keys by name against an id-keyed bucket sees neither.
- **Segments are immutable but not permanent.** Compaction deletes the originals
  once the merged segment exists, and retention deletes whole segments (by age,
  or fully-truncated per the previous note), so a GET of a segment a reader
  learned about earlier can `404` at any time. Re-list the prefix and resolve
  the offset again rather than treating the `404` as data loss — the records
  are in the merged segment, or genuinely past retention.
- **Single writer per prefix is no longer the production regime.** Lease mode
  (`prefixes/{prefix}/lease.json`, #59) still exists in the code, but production
  runs **leaseless** (#86): any replica may append to any prefix, arbitrated by
  the create-only segment-sequence CAS, and the per-prefix marker object is
  `era.json` (above), not `lease.json`. Either way the reader contract is the
  same — decode the footer, and use the overlap rule above.
- **There is no hybrid layout any more (#179).** A topic opted into segments
  mid-life used to keep its earlier `records/{offset}.batch` objects for `[0, C)`
  and write segments for `[C, ∞)`, and a reader was expected to stitch the two
  across the seam `C`. The broker no longer writes, reads or probes that layout:
  segments are the whole log. A reader that still implements the seam is not
  wrong, but it is dead code — and a `records/` object it finds is abandoned data
  the broker will never serve, not a region below a seam.
