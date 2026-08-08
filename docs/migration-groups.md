# Migrating consumer groups to the decomposed layout

This is the operator runbook for cutting a deployment over from the single
`{group}.json` consumer-group object to the decomposed layout of #359, and for
rolling back. It covers only consumer group *coordination*. Committed offsets,
topics, produce and metadata are untouched.

> **A mixed old + new fleet splits every group.** An old pod drives
> `groups/consumers/{group}.json`; a new pod drives
> `groups/consumers/{group}/generation.json`. Neither reads the other, so the
> two halves of the fleet hold disjoint views of the same group, hand the same
> partitions to two consumers at once, and both commit to the same
> per-partition offset objects — last writer wins, so an offset can go
> backwards. The cutover is therefore **quiesce-and-flip, never a
> RollingUpdate.**

## Why there is nothing to migrate

Group state is *soft state about live members*: the timeouts, the member set,
the generation, the leader, the assignment. After a quiesce there are no live
members, so there is no state left to carry across.

The only durable thing a group owns is its **committed offsets**, and those
already live in their own per-partition objects under
`groups/consumers/{group}/offsets/`. This split never touches them. That is why
there is no converter, no dual-write window and no per-group claim: each group
re-forms on its first join after the flip, and picks up exactly the offsets it
had.

The ~2 minutes of consumer unavailability the quiesce costs was accepted for
that reason — the alternative was two releases and a dual-layout reader to
avoid an outage that consumers already tolerate on every rebalance.

## Before you start

- **Every replica must be on a version that writes the decomposed layout.** The
  binary asserts this at startup (below), but the assertion cannot see an old
  pod, so the fleet-wide check is yours.
- Expiry is layout-agnostic as of #367: `expire_groups` folds the newest
  `last_modified` over everything under `{group}/`, so a group that has stopped
  having a `{group}.json` rewritten is not condemned. **Do not flip onto a build
  that predates that**, or every group's committed offsets are reaped at the
  retention window.

## The startup assertion

On boot the broker reads `clusters/{cluster}/schema/groups.json`:

- **absent** — claimed with a create-only PUT of `{"version":2}`, and logged.
  Two replicas starting together race on the key; the loser reads what the
  winner claimed and agrees.
- **`version == 2`** — start normally.
- **anything else** — **refuse to start**, with the found and expected versions
  in the error.

It is an assertion, never a converter. It cannot remove the mixed-fleet hazard
above — an old binary never reads it — and an eager conversion pass would be all
cost and no benefit, because post-quiesce it would be rebuilding member objects
for members that no longer exist. What it buys is that a cluster cannot end up
with two layouts written into it by accident, and that the failure says so at
3am instead of looking like a rebalance that never finishes.

## Forward cutover

1. **Quiesce.** Scale the old ReplicaSet to **zero** — a `Recreate` strategy, or
   an explicit scale-to-0 then deploy. Do **not** use a RollingUpdate. Producers
   buffer and retry through the gap; consumers pause.
2. **Drain.** Wait past the object-store client request timeout (**≥ 60–90 s**)
   after the last old pod exits, so no old process can still have a PUT of
   `{group}.json` in flight.
3. **Start the new pods.** Each group re-forms on its first join:
   `generation.json` is absent, so it is created, and members register their own
   documents as they join. There is no pass to run and nothing to pre-provision.

Expect, in the first minutes:

- every consumer group rebalancing once, which is what a coordinator restart
  always looks like to a client;
- `tansu_group_coordinator_requests{method="join_outdated"}` spiking during
  formation and falling to ~zero once groups are stable — the members race to
  add themselves and the CAS resolves it, which is the designed behaviour, not a
  regression;
- `tansu_group_coordinator_requests{method="*_outdated"}` on the *old*
  `{group}.json` object disappearing entirely. **Re-baseline any alert built on
  it**, or it will read as "the broker stopped serving groups".

## Rollback

Symmetric and free, precisely because offsets are never touched:

1. **Quiesce** the new ReplicaSet to zero.
2. **Drain** ≥ 60–90 s.
3. **Start the old binary.** Groups re-form from the legacy `{group}.json`
   layout, with the same committed offsets.

The decomposed objects are left behind, inert, under each group's prefix — the
mirror image of what the forward cutover leaves behind. They cost storage and
nothing else; `delete_groups` removes them along with everything else the group
owns, and expiry sweeps them with the group.

**Rollback is only free while the old binary is still deployable.** The new
binary can no longer read or write `{group}.json` at all, so the leftover is what
the *old* one reads — keep the old image available for as long as you would want
to roll back, which is the same discipline as `docs/migration-scos.md`.

Rolling *forward again* after a rollback works unchanged: the schema object
still reads `2`, so the assertion passes and the groups re-form.

## Behaviour changes to note in the release

- **`OffsetCommit` is now fenced**, as Kafka fences it. A commit that names a
  member the group does not know is answered `UNKNOWN_MEMBER_ID`, and one that
  claims a generation the group has left is answered `ILLEGAL_GENERATION`. A
  commit with `generation_id == -1` or an empty member id — the simple consumer
  managing its own offsets — is not fenced and reads nothing about the group.
  This broker previously accepted every commit, so **a zombie consumer that used
  to overwrite a live consumer's offsets now fails instead**. Clients handle both
  error codes by rejoining.
- **Group state is no longer written on the commit path at all**, which removes
  the `{group}.json` mtime churn behind #272.
- **`{group}.json` is not read either.** Between the quiesce and a group's first
  join, `DescribeGroups` reports it as **empty** rather than reporting the
  membership the old object records — which is the honest answer, because the
  quiesce made that membership vacuous. `ListGroups` still *names* such a group
  (it owns an object under the consumer root) with state `Unknown`. Both
  converge on the truth the moment the group re-forms.
- `ListGroups` gains a real `states_filter` — it used to report `Unknown` for
  every group.

## Quick reference

| Object | Role |
|---|---|
| `clusters/{cluster}/schema/groups.json` | The layout assertion. `{"version":2}` is the decomposed layout. |
| `groups/consumers/{group}.json` | The **legacy** single group object. Neither read nor written; deleted with the group and reaped by expiry. |
| `groups/consumers/{group}/generation.json` | The group's composition. The only object several members contend on, CAS'd, and only when membership changes. |
| `groups/consumers/{group}/members/{member}.json` | One member's liveness and subscription. One logical writer, at most once per session/2. |
| `groups/consumers/{group}/assignment/{generation}.json` | A generation's assignment. Create-only and immutable; its existence is what makes a group `Stable`. |
| `groups/consumers/{group}/offsets/...` | Committed offsets. **Untouched by this migration.** |

- Cutover and rollback are **quiesce-and-flip**; never mix the two layouts.
- Nothing is migrated: groups re-form, offsets stay where they are.
- One release, not two: this binary reads only the layout it writes.
- The schema object is an assertion, not a converter, and never a substitute for
  checking that the whole fleet is on one binary.
