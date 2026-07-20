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
compaction/failover, the higher `writer_epoch` wins).

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

A reader recovers the index with two ranged GETs of the tail: the last 18 bytes
(the trailer) give `footer_len`, then the preceding `footer_len` bytes are the
footer. The record body is never downloaded to locate a sub-stream.

### Sub-stream region

Each region is the concatenation of that sub-stream's Kafka `RecordBatch` wire
bytes, in order — byte-for-byte what a legacy single-topic coalesced object
(#50) contains. A region is decoded by walking `base_offset (i64) +
batch_length (i32) + batch_length bytes` repeatedly (the standard batch prefix).
Absolute offsets come from the footer, **not** from the batch headers.

### Footer

```
writer_epoch      i64          # lease epoch of the writer (#59); 0 if unleased
entries[entry_count]:
    topic_len     u16
    topic         [topic_len]  # UTF-8
    partition     i32
    base_offset   i64          # absolute offset of the region's first record
    record_count  i64          # last_offset = base_offset + record_count - 1
    byte_start    u64          # region offset within the segment
    byte_len      u64          # region length in bytes
    max_timestamp i64          # greatest record timestamp in the sub-stream
```

To read `(topic, partition)` from `offset`: find its entry, ranged-GET
`[byte_start, byte_start + byte_len)`, decode the region, and assign offsets
running from `base_offset`.

### Trailer (fixed 18 bytes, at the very end)

```
footer_len    u64
entry_count   u32
version       u16              # format version, currently 1
magic         u32             # 0x5453_4547  ("TSEG")
```

## Versioning & backward compatibility

- `version = 1` is the first self-describing multi-topic segment.
- A **legacy** single-topic coalesced object (#50) has **no trailer**: its last
  bytes are record data, so the trailing `magic` will not equal `TSEG`. A reader
  MUST treat "magic absent" as the v0 case and decode the whole object as a bare
  `RecordBatch` concatenation. This is how coalesced and legacy objects coexist
  in one bucket during migration.
- Readers MUST reject a segment whose `version` they do not understand rather
  than guessing.

## Notes for readers

- **Retention** is whole-segment, per prefix: a segment is deleted only once
  every sub-stream in it is past retention (`max_timestamp`), so a live topic
  never loses a shared segment.
- **Single writer** per prefix is enforced by an S3 conditional-write lease
  (`prefixes/{prefix}/lease.json`); `writer_epoch` in the footer identifies the
  writer that produced the segment.
- **Hybrid topics:** a topic opted into segments mid-life keeps its earlier
  `records/{offset}.batch` objects for `[0, C)` and writes segments for
  `[C, ∞)`; a reader serves the legacy region from `records/` and the rest from
  segments, continuous across the seam `C`.
