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
from the footer, never from the name. A segment multiplexes the batches of every
`(topic, partition)` sub-stream, and is self-describing: an external S3-direct
reader (e.g. kotatsu, kotatsu#82) locates and decodes any sub-stream from the
object alone, with no external index — resolving `(topic, partition, offset)`
through the footer's offset ranges (on the rare overlap left by a
compaction/failover, see [Resolving overlaps](#resolving-overlaps)).

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

### Footer

Three versions exist. **v2 is what production writes** — the leaseless writer
(#86) emits it, and so does compaction on a leaseless prefix. v1 is emitted only
by the lease-mode writer (#59) and remains readable. **v3 (#174) is accepted on
read but not yet emitted by anything**: it exists so every reader — this broker
and external S3-direct readers alike — learns the layout one release before the
first v3 segment can appear (see [Versioning](#versioning--backward-compatibility)).
A reader MUST accept all three, and MUST reject any other version.

```
writer_epoch      i64          # see "writer_epoch" below
nonce             u64          # v2+ ONLY — per-flush write identity (#89)
entries[entry_count]:
    topic_len     u16
    topic         [topic_len]  # UTF-8
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

To read `(topic, partition)` from `offset`: find its entry, ranged-GET
`[byte_start, byte_start + byte_len)`, decode the region, and assign offsets
running from `base_offset`.

#### `writer_epoch`

The field is present in both versions and is the overlap tie-break in both, but
the two writer regimes stamp it differently:

- **v1, lease mode (#59):** the epoch of the lease the writer held, `0` if none.
- **v2, leaseless mode (#86, what production runs):** the prefix's **era epoch**,
  read from `prefixes/{prefix}/era.json` (`{"era_epoch": i64}`). It is a constant
  per prefix, not per writer: seeded once as
  `max(last lease epoch, highest footer epoch) + 1`, so it is `>= 1` and every
  leaseless segment out-epochs every pre-cutover lease-era segment on the same
  prefix (#92).
- **compaction:** the merged segment carries the maximum `writer_epoch` of the
  segments it merged (floored at 0), and is written v2 on a leaseless prefix and
  v1 otherwise.

### Trailer (fixed 18 bytes, at the very end)

```
footer_len    u64
entry_count   u32
version       u16              # 1, 2 or 3 (2 is what production writes)
magic         u32              # 0x5453_4547  ("TSEG")
```

## Versioning & backward compatibility

- `version = 1` is the first self-describing multi-topic segment; `version = 2`
  adds the footer `nonce` and the per-entry `producers` block described above;
  `version = 3` (#174) appends one `flags` byte per producer coordinate and
  widens coordinate emission to transactional and control batches. A reader
  MUST accept `{1, 2, 3}` and MUST reject any other version rather than
  guessing.
- **v3 is not yet written.** Production writes v2 everywhere today; v3 starts
  being emitted only in a later release (release B of #174, which routes
  transactional and control batches into segments), and only once every
  deployed reader accepts `{1, 2, 3}`. This ordering is deliberate: the
  version-rejection MUST above means a v2-only reader fails **cleanly** on a v3
  segment — it does not mis-read — but it fails on *every* segment the moment a
  writer flips, because the writer stamps v3 unconditionally (the version
  follows the writer regime, never the segment's content). Readers must
  therefore upgrade **before** writers, which is why this document describes v3
  ahead of any object carrying it.
- A **legacy** single-topic coalesced object (#50) has **no trailer**: its last
  bytes are record data, so the trailing `magic` will not equal `TSEG`. A reader
  MUST treat "magic absent" as the v0 case and decode the whole object as a bare
  `RecordBatch` concatenation. This is how coalesced and legacy objects coexist
  in one bucket during migration.

## Transactional and control batches

Once v3 segments are written (#174, release B — not yet, see above), a
sub-stream region can contain transactional data batches and **control
batches** (transaction markers). Both are ordinary Kafka `RecordBatch` wire
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
sweep in that order and drop any entry whose `base_offset` falls below the range
already covered. The higher-sequence tie-break is what makes a merged segment win
over the originals it merged: they carry the same epoch, and the merged segment's
sequence is always higher.

## Notes for readers

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
- **Hybrid topics:** a topic opted into segments mid-life keeps its earlier
  `records/{offset}.batch` objects for `[0, C)` and writes segments for
  `[C, ∞)`; a reader serves the legacy region from `records/` and the rest from
  segments, continuous across the seam `C`.
