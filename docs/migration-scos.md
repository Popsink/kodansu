# Migrating a prefix-coalesced deployment to SCOS (leaseless)

This is the operator runbook for cutting a prefix-coalesced deployment over from
the single-writer **lease** regime (#59) to the leaseless **SCOS** arbiter
(#82/#86), and for rolling back. It covers only the produce path; consumer
groups and metadata are unaffected.

> **A mixed lease + leaseless fleet is corrupt.** A leaseless pod never touches
> `lease.json`, so it cannot fence an old lease-holder. If both regimes run at
> once, the old holder's in-flight ("straggler") flush wins its sequence CAS and
> its **lease-era epoch beats the new pod's** on the segment overlap tie-break
> (`valid_substream_segments`) — silently erasing data the new pod already
> acked. The cutover is therefore **quiesce-and-flip, never a RollingUpdate.**

## Why it is safe once flipped

Every leaseless segment is stamped with a per-prefix **era epoch**, seeded on the
first leaseless flush of the prefix as:

```
era_epoch = max(lease.json epoch, max segment footer epoch) + 1     (never 0)
```

Because the era strictly exceeds every pre-cutover lease-era epoch, a
leaseless segment always wins the overlap tie-break over any lease-era segment —
so even a straggler that lands after the flip is structurally harmless. The era
is written create-only to `prefixes/{prefix}/era.json` and is immutable: one
constant era for the whole leaseless regime, and every replica converges on it
(the first writer to seed wins; peers read and adopt that value).

Seeding is **automatic** on the first leaseless flush — there is no pre-seed step
to run. The correctness precondition is only that no old lease-era writer is
still running when the first leaseless flush happens, which the quiesce below
guarantees.

## Forward cutover (lease → leaseless)

1. **Quiesce.** Scale the old (lease-mode) ReplicaSet to **zero** — a `Recreate`
   deployment strategy, or an explicit scale-to-0 then deploy. Do **not** use a
   RollingUpdate. CDC producers buffer/retry through the gap.
2. **Drain.** Wait past the object-store client request timeout (**≥ 60–90 s**)
   after the last old pod exits, so no old process can still have a PUT in
   flight. Lease *expiry* is **not** the gate — a straggler PUT can land after
   the lease lapses.
3. **Start leaseless.** Bring up the new pods with the leaseless flag on
   (`prefix_leaseless`). The first flush of each prefix seeds and stamps the era
   automatically (step above). No produce routing is involved — any replica may
   append to any prefix (this tree never wired a multi-broker routing layer, so
   there is no routing config to remove).

Format note: v2 segments (footer nonce + producer coordinates) coexist with v1
per-object. All readers — the broker and any external S3-direct reader
(kotatsu) — must accept v2 **before** any v2 write. Keep a soak window before
enabling leaseless writes, because rolling back *after* a v2 write requires a
segment-rewrite tool.

## Rollback (leaseless → lease)

Reverse the flip with the **same quiesce discipline**, then raise each active
prefix's lease epoch above its era so a restarted lease-holder out-epochs every
leaseless-era segment:

1. **Quiesce** the leaseless ReplicaSet to zero.
2. **Drain** ≥ 60–90 s.
3. **Rewrite lease epochs.** For each active prefix, rewrite `lease.json` with an
   epoch **strictly above** `era_epoch`. The `rollback_prefix_to_lease` storage
   entry point does this: it reads the seeded era and CASes `lease.json` to
   `era_epoch + 1` (an expired term, so the first restarted lease pod re-acquires
   immediately, bumping to `era_epoch + 2`). A restarted holder therefore stamps
   segments that out-epoch every leaseless-era segment and wins the overlap
   tie-break — the mirror of the forward guarantee.
4. **Start** the old lease-mode pods (there is no routing config to restore).

If any v2 segment was written during the leaseless window, the segments must be
rewritten to v1 before old readers that predate v2 can read them; gate the
rollback on whether your readers accept v2.

## Quick reference

| Object | Role |
|---|---|
| `prefixes/{prefix}/lease.json` | Single-writer lease + epoch (#59). |
| `prefixes/{prefix}/era.json` | Seeded leaseless era epoch (#92), immutable. |
| segment footer `writer_epoch` | The stamped era (leaseless) or lease epoch (lease); higher wins an overlap. |

- Cutover and rollback are **quiesce-and-flip**; never mix the two regimes.
- The era is seeded automatically; there is nothing to pre-provision.
- Rollback must raise lease epochs **above** the era before old pods restart.
