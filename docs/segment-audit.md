# Offline segment audit

`tansu audit` walks a bucket's segments and reports the offsets they **cannot
serve**: ranges present in no object.

It exists because the corruption counters cannot answer this question.
`tansu_prefix_segment_regions_corrupt` and `..._truncated` count reads that *met*
damage; they cannot count records that no longer exist to be read. When a merge
wrote a segment that silently omitted a region and then deleted the originals
(#386, fixed in `1.0.0-alpha.2`), or when a freed sequence name was reborn under
a peer's cached footer (#432, fixed in #434), **every surviving byte stays
intact**: CRC32C validates, footers are self-consistent, regions sum exactly to
the body. Kafka consumers tolerate offset gaps, so nothing downstream reports
anything either. The damage leaves no trace in what remains — which is exactly
why it goes unnoticed.

The measurement that sized the loss on the deployment in #447 — 32 424 376
records, 12.77 % of the offset span — was taken this way, offline, from a copy of
the object store. This is that method, shipped.

## Running it

Against a live bucket, read-only (LIST plus one ranged GET per segment; record
bodies are never downloaded):

```shell
tansu audit --storage-engine s3://my-bucket/ --cluster-id my-cluster
```

Against an **offline copy**, which is the form to prefer — it needs no broker, no
credentials, and cannot be perturbed by a replica still writing:

```shell
aws s3 sync s3://my-bucket/ ./copy/
tansu audit --storage-engine file://$PWD/copy --cluster-id my-cluster
```

`file://` is deliberately not a storage engine the broker accepts. A deployment
already past the damage cannot be measured by starting one; the audit resolves
the URL itself.

Useful flags:

| Flag | Effect |
|---|---|
| `--ranges` | Print every lost range, with the segments bracketing it |
| `--format json` | The whole report, every range included — the form to keep |
| `--topic NAME` | Restrict the *report* to these topics (the walk is unchanged) |
| `--fail-on-loss` | Exit non-zero when records are lost, for a scripted sweep |
| `--concurrency N` | Segments read concurrently (default 32) |

`--fail-on-loss` is opt-in because the exit is carried as `CORRUPT_MESSAGE`,
whose generic description ("this message has failed its CRC checksum") is the
opposite of what an audit finds. It is a mechanism for the shell, not a claim
about a surviving object.

## Reading the report

```
segments      3 054 in 35 prefixes (v3 3054)
unreadable    0
legacy        26 149 abandoned records/ objects — the broker has served none since #179

records lost  32 424 376 of 253 966 964 offsets — 12.77 %
              cleanup.policy=delete only, and a floor: a hole is visible only
              between two surviving segments, never at the head or tail of a log.

TOPIC                            P            SPAN            LOST        %  RANGES
org.env.conn.tab_a               1      16 003 452      13 490 384   84.30%      37
```

Three things about that headline are load-bearing.

**It counts `cleanup.policy=delete` only.** Per-key compaction removes superseded
keys, and a removed key *is* an offset gap. On a compacted topic the method
cannot separate legitimate compaction from damage, so it evidences nothing
there. Compacted topics are still printed — under their own heading, never added
in. Folding them into the total is how a first pass over the #447 data
overstated the loss; the correction is why the two populations are separated
here structurally rather than by discipline.

**It is a floor, not a total.** A hole is visible only *between* two surviving
segments. Loss at the head or tail of a log, or a wholly erased prefix, leaves
nothing to bracket it and is not counted.

**It says nothing about the record bodies.** Every check comes from the footer
plus the object's size in the listing, which is what keeps the cost at one
ranged GET per segment rather than the whole bucket. That covers the structural
faults which leave a trace — an entry claiming bytes past the object (#393,
#395), regions that do not tile the body (#403), an object under `segments/`
with no `TSEG` trailer — and those are printed as **faults**. It does not cover a
region whose bytes are not the batches its entry describes. That is a separate,
whole-bucket pass.

### Ranges

`--ranges` prints, per hole:

```
org.env.conn.tab_a
  p0   3 145 728 .. 3 147 000  1 273 records
       written between 2026-07-14T09:12:41.204Z and 2026-07-14T09:19:03.881Z
       between org.env.conn/4471 (16.0 MiB) and org.env.conn/4474 (21.4 KiB)
```

The two timestamps are the bracketing segments' `max_timestamp`s, so they bound
**when the lost records were written** — which is what a re-snapshot or a
source-journal replay has to target.

The two sizes are the merge-path signal. A segment at the ~16 MiB roll target is
a merged segment; a flush segment is orders of magnitude smaller. On the #447
deployment the segment immediately before a hole was at the roll target 81 % of
the time, against 34 % for segments followed contiguously — which is what
attributed the holes to compaction rather than to retention or the produce path.

### Overlaps and legacy objects

`overlaps_dropped` (JSON only) counts slices the overlap rule discarded. A
merged segment winning over the originals it merged is the normal case, so this
is informational. The audit resolves slices exactly as a reader does — ascending
`base_offset`, ties broken by higher `writer_epoch` then higher `seq`
([docs/virtual-topics-format.md](virtual-topics-format.md)) — so the gaps it
reports are the gaps a consumer meets, not an artefact of a different
resolution.

`legacy_batches` counts abandoned `records/{offset}.batch` objects (#50). Since
#179 the broker neither writes nor reads them, so they are not offsets the
sub-stream serves and are never folded into coverage. They are reported because
a log that has them predates segments, and a gap at its head may be theirs.

## Minimum version

Two silent-loss paths were fixed after `1.0.0-alpha.1`, and the root-cause fix
for the second is the later of the two:

| Fix | Released in |
|---|---|
| #388 — compaction no longer silently drops an undecodable region | `alpha.2` |
| #395 — a footer entry cannot over-claim | `alpha.3` |
| #403 — a short region resolves against the object's trailer | `alpha.4` |
| #402 — one undecodable segment no longer ends its prefix's drain | `alpha.4` |
| #433 — a full-length frameless region asks the trailer | after `alpha.4` |
| #434 — a create folds the durable seq floor, so a freed name is not reborn | after `alpha.4` |

`1.0.0-alpha.1` is the release to get off first: it *destroys* during compaction
(a region that decoded to nothing contributes nothing to the merged segment, and
the sources are then deleted) where `alpha.2`+ fails the tick loudly instead.
`alpha.4` stops the silent destruction but still carries the sequence-reuse
defect of #432; the fix for that is #434.

Long-lived pinned deployments are the exposure: fleet counters describe what the
fleet's release is doing, not what a cluster pinned to an older tag has already
accumulated. Run the audit before assuming a deployment is clean.
