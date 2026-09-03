# RFC: an ADLS Gen2 store, not an S3 proxy

**Status:** draft, revision 2 — for iteration
**Scope:** add a third real object-store backend to `tansu-storage`, alongside
`s3://` and `gs://`, speaking Azure's own REST API against **Azure Data Lake
Storage Gen2** (Blob Storage with hierarchical namespace enabled).
**Non-goal:** running Kodansu against an S3-compatible gateway in front of ADLS.

> Revision 2 resolved three of revision 1's open questions from the Azure REST
> reference and from `object_store`'s source, and surfaced two more it claimed
> were consequences of HNS: an empty-directory GC obligation (§5.2) and a
> retention path billed on Azure but free on S3 (§6.1).
>
> **Revision 3 retracts both of those.** The directory residue is removable —
> deepest-first through the plain Blob API, or one recursive call against the DFS
> endpoint — and deletes are free on Azure exactly as on S3. Both were verified
> against a real hierarchical-namespace account and against Microsoft's own
> billing references (#417, #422). Revision 3 also closes revision 2's four
> remaining HNS unknowns by observation, and the answers are in §4 and §5. Two
> smaller claims are retracted along the way, in §5.1 and §6.3 — every retraction
> in the direction of less work.

## 1. Why not the S3 gateway

The S3-compatible front ends for Azure (MinIO gateway — archived; third-party
shims) all fail on the same axis: this fork's object layout is **create-only and
CAS-driven**, not read-mostly. `tansu-storage/src/dynostore.rs` issues 11
`PutMode::Create` and 4 `PutMode::Update` calls, and the group coordinator, the
segment sequence allocator and the maintenance lease are all written against the
*error class* a losing writer gets back (`docs/testing.md`,
`tansu-storage/tests/conditional_put.rs`).

A gateway has to translate `If-None-Match: *` and `If-Match: <etag>` onto Azure
semantics faithfully, on every path, forever. When it doesn't, the symptom is not
an error — it is two writers both believing they won a CAS, which surfaces days
later as a duplicated offset or a wedged consumer group. We know from #13 and
#157 what that costs to debug through *one* layer of indirection. A proxy is also
a second thing to deploy, scale and pay for inside the tenant's network, which
defeats the point of a stateless broker.

The good news: Azure supports every primitive we need **natively**, and on
several axes is a closer fit than S3 was.

## 2. What Azure gives us for free

Verified against `object_store` 0.14.1 (pinned at `Cargo.toml:100`), feature
`azure`:

| Primitive | `dynostore` use | Azure status |
|---|---|---|
| Create-only put | `PutMode::Create`, ×11 | Native `If-None-Match: *` — `azure/client.rs:760` |
| ETag CAS | `PutMode::Update`, ×4 | Native `If-Match` — `azure/client.rs:761` |
| Bulk delete | `delete_stream`, ×5 | Native Blob Batch, chunked at 256 — `azure/mod.rs:147` |
| Listing | `list`, `list_with_delimiter` | Native `List Blobs`; HNS directory entries filtered — `azure/client.rs:1339` |
| Tail-offset listing | `list_with_offset` (`scan_from`, `dynostore.rs:4050`) | Native `startFrom` — see §4.1 |
| `head` | ×1 (boot backfill marker) | Native `Get Blob Properties` |
| Multipart | never on the write path (only the metering passthrough, `dynostore.rs:13620`) | n/a |

Two consequences worth stating plainly:

- The S3 arm needs `with_conditional_put(S3ConditionalPut::ETagMatch)`
  (`lib.rs:2770`) to opt into semantics Azure has had since 2009. No such flag
  here.
- The GCS arm needs a whole per-object rate limiter (`tansu-storage/src/gcs/limit.rs`,
  ~1 write/s/object, #13). Azure's per-blob ceiling is ~500 req/s, three orders
  of magnitude higher. **Do not copy the `gs` arm's `PutRateLimiter`.**

## 3. The one real blocker: suffix range GETs

Azure does not implement `Range: bytes=-N`. `object_store` fails such a request
**client-side, before sending it**:

```rust
// object_store-0.14.1/src/azure/client.rs:1177
if let Some(GetRange::Suffix(_)) = options.range.as_ref() {
    return Err(crate::Error::NotSupported {
        source: "Azure does not support suffix range requests".into(),
    });
}
```

The coalesced-segment reader is built entirely on suffix GETs — the footer index
lives at the *tail* of the object (#58/#64), and the read path never learns the
object length first. Four call sites, all hot:

| Function | Role |
|---|---|
| `read_segment_footer` | speculative 64 KiB tail (`SEGMENT_FOOTER_OVER_READ`) |
| `read_segment_footer` | exact `[footer‖trailer]` for an oversized footer |
| `fold_segment_footer` | fold a lost create-CAS without a LIST (#411) |
| `probe_prefix_tail` | tail probe; **a 404 here is the absence proof** |

*(Located by name rather than by line: the line numbers revision 2 carried are
~1200 lines stale.)*

On Azure without the decorator: `probe_prefix_tail` degrades to permanent
`Inconclusive`, so every read falls back to a LIST — the expensive tier, and
precisely what #411/#412 exist to avoid.

**And a fetch fails, but only from a replica that did not write the segment.**
Revision 2 said "every fetch fails", which is the more alarming and less useful
statement. A writer's flush populates its own `PrefixIndex` with the footers it
just wrote and never reads one back, so it serves its own partition perfectly
well. Measured while implementing #419: the same four-record partition reads
`Ok(4)` in the writing process and `NotSupported` from any other one. That is
every replica in a fleet and none in a single-process test — which is why #420's
Azurite job cannot be the thing that catches a regression here, and why the
#419 tests all read from a second store over the same bucket.

### 3.1 Options

**(A) Translate suffix → `head` + `Bounded`, in a decorator.** An `ObjectStore`
wrapper on the Azure arm intercepts `get_opts` with `GetRange::Suffix(n)`, issues
`head()` for the size, then re-issues `GetRange::Bounded(size-n .. size)`. Zero
change to `dynostore`; mirrors the existing `gcs::limit::PutRateLimiter`
decorator exactly. Cost: one extra request per footer read. `Get Blob Properties`
and `Get Blob` are the same Azure billing tier, and a LIST is an order of
magnitude above it — so the probe stays comfortably cheaper than the LIST it
exists to replace. A 404 from `head` is the same absence signal
`probe_prefix_tail` already keys on.

**(B) Cache the size.** `PrefixIndex` already learns `ObjectMeta` on the refresh
path; the second request is only unavoidable for a *blind* probe of a sequence
never listed. Layer on top of (A), not instead of it.

**(C) Move the index to a fixed-size header.** Segments are assembled in memory
and written with a single `put_opts`, so a leading pointer block is physically
possible and would make the read one bounded GET on every backend. Breaking
format change plus a migration, and it buys nothing on S3/GCS where the suffix
GET already works.

**Recommendation: (A) now, (B) as a follow-up, (C) explicitly deferred** and
revisited only if Azure read amplification shows up in the bill.

## 4. Questions revision 1 left open, now answered

**Spike, 3 September 2026 (#417).** Four of the answers below were behaviours of
a hierarchical-namespace account rather than statements in a reference, and
Azurite has no hierarchical namespace, so they were run against a purpose-made
account: `StorageV2`, `Standard_LRS`, hot tier, **HNS enabled**, `northeurope`
(`westeurope` refuses new customers). Two layers, because the questions are not
all about the same one — raw `List Blobs` / `Delete Blob` over the REST API with
`x-ms-version: 2023-11-03` for what the *service* does, and `object_store` 0.14.1
for what the broker actually sees. Each finding below says which layer observed
it. The harness was throwaway and is not in the tree; the deliverable is this
section.

One result contradicts revision 2 outright, and it is §5.2.

### 4.1 `startFrom` is not HNS-gated, and our `x-ms-version` is new enough

Revision 1's highest-risk unknown. Answered by the `List Blobs` reference:

> The `startFrom` and `endBefore` parameters let you list a range of blobs within
> a container. `startFrom` sets the lower bound of the range […] Results are
> inclusive of `startFrom` and exclusive of `endBefore`. […] The `startFrom`
> parameter is supported for both XML and Apache Arrow listings.

`startFrom` is `Optional. Version 2023-05-03 and newer` with no hierarchical-namespace
restriction. `object_store` pins `x-ms-version: 2023-11-03`
(`azure/credential.rs:49`), which is newer than both `startFrom` (2023-05-03) and
the `ResourceType` list element that the HNS directory filter depends on
(2020-10-02). Both are available.

`startFrom` is *inclusive* where S3/GCS `start-after` is exclusive;
`object_store` already reconciles this by dropping a leading exact match
(`azure/client.rs:1276-1302`). So `scan_from` keeps its O(new) refresh.

**Observed, and this was revision 1's highest-risk unknown, so it is worth the
detail.** At the REST layer, `prefix=q1/&startFrom=q1/0009.seg` over ten seeded
blobs returned **one** entry. The offset at the last name is the sharp form of
the question: a client-side filter and a server-side skip are indistinguishable
from an offset in the middle, which is why asserting through `list_with_offset`
proves less than it looks. One entry means the *service* skipped. Two further
observations:

- the entry returned was `q1/0009.seg` itself, confirming `startFrom` is
  inclusive — so the exclusive semantics `list_with_offset` presents are
  `object_store` dropping the first entry, not Azure's;
- `startFrom=q1/0004.zzz`, a name that does not exist, resumed at `q1/0005.seg`.
  The offset need not name a real blob, which matters because
  `segment_location(prefix, seq)` is synthesised from a cached sequence and may
  name a segment a peer has since retired.

Through `object_store`, offsetting at the fifth of ten returned exactly five and
excluded the offset. **So `scan_from` keeps its O(new) refresh on ADLS Gen2, and
this ticket does not become a redesign.**

**The CI hedge stands, because it is about CI and not about Azure:**
`object_store` detects the emulator and bypasses `startFrom` entirely, falling
back to client-side filtering (`azure/mod.rs:127-140`, Azurite issue #2619). **An
Azurite-green run still proves nothing about `list_with_offset`** — what changed
is that the behaviour is now known, not that the emulator can check it.

### 4.2 HNS sort order does not reach us — and that is a property to preserve

The `List Blobs` reference:

> Blobs are listed in alphabetical order in the response body, with upper-case
> letters listed first. Note that for accounts with a hierarchical namespace
> enabled, `/` is treated as the lowest sort order. This difference in behavior is
> only applicable to listing recursively.

This *is* a divergence from S3/GCS lexicographic order, and it is reachable in
principle: Kafka topic names admit `.` (0x2E) and `-` (0x2D), both of which sort
*below* `/` (0x2F) in ASCII but *above* it under HNS. A recursive listing
spanning sibling directories would therefore come back in a different order on
ADLS than on S3.

It does not reach us, because the only offset listing we do is within a single
directory. `segment_prefix` is `clusters/{cluster}/prefixes/{prefix}/segments/`
(`dynostore.rs:4090`) and `segment_location` appends `{seq:0>20}.seg`
(`dynostore.rs:4100`). Every key in a `scan_from` shares the full prefix and the
remainder contains no `/` — so there is nothing for the `/` rule to reorder.

**Observed.** Four blobs seeded as `q5/a-b`, `q5/a.b`, `q5/a/b`, `q5/a0b` came
back from a recursive `List Blobs` in the order

    q5/a/b, q5/a-b, q5/a.b, q5/a0b

against the ASCII order `a-b, a.b, a/b, a0b` (`-`=0x2D, `.`=0x2E, `/`=0x2F,
`0`=0x30). `/` does sort lowest, ahead of both characters a Kafka topic name
admits, so the divergence is real and not a documentation artefact.

**This is therefore a constraint, not an observation.** Any future layout that
puts a `/` *below* the listing prefix, and relies on ordering across it, is
correct on S3 and wrong on ADLS. It belongs in a comment next to
`segment_location`.

### 4.3 CI account: same question as GCS, one answer

Deferred, and deliberately merged with the GCS gap. `docs/testing.md` records
that S3 and GCS are both untested — every test runs on `memory://`, and
conditional put is exactly where `InMemory` and a real store diverge. The GCS
hole exists because nobody owned an account.

So this is not an Azure decision: it is one decision about **who owns
credentials for a nightly non-emulator run**, answered once for `gs://` and
`az://` together. Until it is answered, the Azurite job (§7) is the ceiling of
what we can claim.

**One datapoint from #417, because it changes the calculus rather than the
decision.** Standing up a throwaway HNS account was three commands — register
`Microsoft.Storage`, `az group create`, `az storage account create
--enable-hierarchical-namespace true` — plus one `Storage Blob Data Contributor`
assignment, and the whole spike (a few hundred one-byte blobs, deleted) costs
pennies. The obstacle to a nightly Azure run is therefore *ownership of a
credential*, not provisioning effort or spend. That is the same obstacle GCS has,
and it is worth saying plainly: nothing technical is in the way.

Note also that §4.1's specific residual risk is now closed by observation — the
emulator still cannot check `startFrom`, but we know what a real account does.

## 5. ADLS Gen2 / hierarchical namespace

`object_store` speaks the **Blob** endpoint (`blob.core.windows.net`), which
works against HNS accounts. Per *Known issues with Azure Data Lake Storage*, the
unsupported Blob REST APIs are all page-blob / append-blob / incremental-copy
operations — **none that we call**. Blob Batch is not listed as unsupported.

**Blob Batch observed working on HNS, across the chunk boundary.** 300 blobs
deleted through `object_store`'s `delete_stream` — which chunks at 256, so this
was two batch requests — reported 300 deletions and left the prefix empty. The
five `delete_stream` call sites in `dynostore` are sound as written.

The timing is a second datapoint and it is the one that matters for §6.1: the
same 300 blobs took **12.2 s** to create one `Put Blob` at a time and **393 ms**
to delete in two batches. Batching is ~31x on wall clock. It is not a saving on
transactions — each subrequest is billed, so those two batches are 302 billed
transactions, not 2.

### 5.1 Directory entries in listings — already handled

> When you use the `List Blobs` operation without specifying a delimiter, the
> results include both directories and blobs.

`object_store` filters `ResourceType == "directory"` in `to_list_result`
(`azure/client.rs:1339`), which is shared by both `list` and
`list_with_delimiter`. So directory placeholders do not reach `segment_seq_of`.

**Observed, and the answer has two halves.** The filter works: listing one level
up from a segments directory returned **five** entries over raw REST — two
`ResourceType=directory` and three `file` — and **three** through `object_store`.
It is load-bearing, not belt-and-braces, for any layout that nests below a
listing prefix.

But it never fires on the listing we actually issue. An undelimited raw list of
`…/prefixes/org.env.conn/segments/` returned three entries, all
`ResourceType=file`, and no directory. That follows from the layout rather than
from luck: `segment_location` appends `{seq:0>20}.seg` with no `/` in it, so a
`segments/` directory has no directory children to enumerate — the same property
§4.2 turns into a constraint.

**So revision 2's "permanent per-listing overhead proportional to topic count"
is wrong, and it is retracted.** We do not pay to enumerate and discard a
directory per prefix on the refresh path, because there is none below the listing
prefix. Nothing for the 1500-topic rig to show here.

### 5.2 Empty directories are residue, not a leak — revision 2 was wrong here

> If you use the `Delete Blob` API to delete a directory, the directory is
> deleted only if it's empty. This condition means that you can't use the Blob
> API to delete directories recursively.

Retention deletes segment blobs. It does not delete the
`clusters/…/prefixes/…/segments/` directories that HNS materialised to hold them,
and that much is confirmed: after deleting all three segments under a seeded
prefix, an undelimited list still returned `q3/prefixes`,
`q3/prefixes/org.env.conn` and `q3/prefixes/org.env.conn/segments`, all
`ResourceType=directory`.

**But revision 2 called this "the biggest new finding" and concluded the skeleton
survives *forever*, and that is wrong.** It read "cannot delete recursively" as
"cannot delete", on the evidence of a `409` returned by `Delete Blob` against the
*top* of the skeleton. `409` is what a non-empty directory returns. Deleting the
same skeleton **deepest-first** through the plain Blob API returned `202` on
every one, and left the prefix empty:

    DELETE q3/prefixes/org.env.conn/segments  -> 202
    DELETE q3/prefixes/org.env.conn           -> 202
    DELETE q3/prefixes                        -> 202

And the DFS endpoint's recursive delete works on the same account with the same
bearer token — `DELETE …dfs.core.windows.net/{container}/q3?recursive=true` →
`200`, one call.

So the residue is real, and it is removable two ways, one of which needs nothing
`object_store` does not already do. The options, re-weighed:

1. **Accept the residue.** Directories are not billed for capacity, and §5.1 now
   says they are not enumerated on the refresh path either — so the cost is an
   untidy account and an undelimited list that nobody on the hot path issues.
2. **Delete directories bottom-up in `maintain`**, once a prefix's last segment
   goes. Now known to work through the Blob API, at one `Delete Blob` per level.
   The race with a writer about to re-create the prefix is the real objection and
   it is unchanged: a re-create is a `PutMode::Create` that would have to
   re-materialise the parent, and losing that race costs a produce.
3. **DFS-endpoint recursive delete** for topic deletion only. Now known to work,
   and topic deletion is the one path with no concurrent writer to race, which is
   what made (2) unattractive. It is a raw call outside `object_store`, using the
   credential we already hold.

**Recommendation: still (1) for a first release, but the reason has changed.** It
is no longer "we cannot fix this" — it is "this is not worth fixing yet", with
(3) available for topic deletion whenever an account accumulates enough dead
prefixes to matter. That is a much cheaper position than revision 2's, and it
means §5.2 is not a blocker on anything.

### 5.3 Private endpoints need both sub-resources

> If you're using private endpoints […] you need to create one for both the
> **blob** and **dfs** sub-resources.

We only use `blob`, so a blob-only private endpoint is sufficient *for Kodansu* —
but a tenant whose network team provisioned only `dfs` (the usual default for a
data-lake account) will see us fail to connect. Worth one line in the deployment
docs; it will otherwise be someone's afternoon.

## 6. Costs and the retention path

### 6.1 Deletes are free on Azure too — revision 2 was wrong, and #422 retracted it

Revision 2 read the `Blob Batch` billing note

> The `Blob Batch` REST request is counted as one transaction, and each
> individual subrequest is also counted as one transaction.

as meaning a 256-blob batch delete costs 257 *billed* transactions, and built a
whole cost concern on it. **It is a statement about counting, not charging.**
The `Delete Blob` reference is explicit — *"Storage accounts are not charged for
`Delete Blob` requests"* — the pricing page's fourth category is verbatim "All
other Operations (per 10,000), except Delete, which is free", and the retail
prices API returns a real zero-priced `Hot LRS Delete Operations` meter.

So the maintenance delete flood is free-but-throttled on **both** backends, and
the "deletes are free" assumption carries over intact. #5, #6 and
`UPSTREAM_ISSUE_object_store_deleteobjects.md` stay about throttling, and no
tuning knob acquires a cost dimension.

What *is* different, measured and written up in `docs/storage-tuning.md`: a list
is billed as a **read** on ADLS Gen2 where S3 prices `LIST` with `PUT`, which is
~9× cheaper and drops the LIST plane from 18 % of a measured fleet's bill to
under 2 %; and read/write transactions are metered **per 4 MiB**, which makes
`prefix_compact_target_bytes` (default `16m`) four write transactions per merged
segment. Re-pricing that fleet end to end gives ~1.11× the S3 bill, almost all of
it the ~1.30× hierarchical-namespace write premium.

**Observed (#417).** 300 blobs deleted through `delete_stream`, which chunks at
256, is two batch requests and **302** billed transactions — not 2. The same 300
took 12.2 s to create one at a time against 393 ms to delete in two batches, so
what batching buys is wall clock (~31x), and it buys nothing on the bill. That is
the shape §6.1 asserts, now measured rather than derived from the reference.

This should be modelled before the first customer, not after. `docs/storage-tuning.md`
already has the S3 request-bill section to extend.

### 6.2 An opportunity HNS actually unlocks: `Set Blob Expiry`

> The `Set Blob Expiry` operation sets an expiration date on an existing blob.
> **This operation is allowed only on hierarchical namespace-enabled accounts.**

With `x-ms-expiry-option: RelativeToCreation`, a segment could carry its own
retention at write time and Azure would reclaim it — removing time-based
retention deletes from our critical path entirely. Given that the delete flood is
the origin of #5, #6 and an upstream `object_store` bug, that is an operational
win before it is a cost one.

Caveats, all real:

- It is a **separate PUT** (`?comp=expiry`), so it costs one write-tier-ish op
  per segment at flush in exchange for one delete op later. Roughly transaction-neutral;
  the win is removing the burst, not the count.
- Expiry is stamped at write. `AlterConfigs` changing `retention.ms` would not
  retroactively apply, so the delete path has to stay as the authority anyway.
- Useless for `cleanup.policy=compact` and for size-based retention.
- `object_store` does not expose it, and owns the credential — so this needs
  either an upstream addition or credential plumbing of our own.

**Recommendation: not in the first release.** Record it here so it is a decision
we deferred rather than a capability we never noticed.

### 6.3 Account configuration is part of the contract

The layout is create-only and immutable, and retention is implemented by delete.
The container must therefore be created with:

- **blob versioning off** — otherwise `PutResult.version` starts populating and
  every superseded write is retained and billed;
- **soft delete off, or minimal** — supported on HNS, but it would stop retention
  deletes from freeing space. (Note an expired file under §6.2 is *not*
  recoverable via soft delete, by design.)
- **no immutability / legal-hold policy** — it would break the delete path;
- **hot tier** — cool/cold add per-GB read charges this access pattern pays
  constantly.

**Observed (#417): a freshly created account already satisfies all four.** On a
`StorageV2` / HNS account created with `az storage account create
--enable-hierarchical-namespace true --access-tier Hot` and nothing else,
`blob-service-properties` reports `isVersioningEnabled: null`,
`deleteRetentionPolicy.enabled: false` and `containerDeleteRetentionPolicy:
null`. So this contract is **"do not turn these on"**, not "remember to turn them
off" — which is a materially easier thing to write in a deployment doc, and it
means the failure mode is a tenant who hardened an existing data-lake account
rather than one who followed the default path.

These belong in the docs and, ideally, in a `ping()`-time warning.

## 7. Testing — the part that is actually a win

Azurite is a first-class Microsoft emulator and runs in `compose.yaml` next to
minio. That makes ADLS the **first backend where
`just test-conditional-put az://tansu/` can run in CI on every PR** — worth more
than the Azure support itself, given §4.3.

What Azurite does *not* cover, and must not be claimed:

- `startFrom` / `list_with_offset` — bypassed on the emulator (§4.1);
- HNS behaviour of any kind — Azurite has no hierarchical namespace, so §5.1 and
  §5.2 are untested by it;
- throttling behaviour under load.

## 8. Design decisions

### 8.1 URL scheme

`object_store`'s parser accepts `az://`, `abfs://`, `abfss://`, `adl://`,
`azure://` and `https://<account>.{blob,dfs}.core.windows.net/`
(`azure/builder.rs:720-745`). Our builder dispatches on `Url::scheme()`
(`lib.rs:2732`) and takes the container from `host_str()`, exactly as it takes
the bucket today.

Proposal: **`abfss://<container>@<account>.dfs.core.windows.net/` is canonical**
— it is what an ADLS Gen2 user already writes, and it carries the account — with
`az://<container>/` accepted as the short form (account from
`AZURE_STORAGE_ACCOUNT_NAME`). *This flips revision 1, which made `az://`
canonical; ADLS Gen2 users write `abfss://`.* All existing query parameters
(`batch_min_size`, `coalesce_*`, `producer_checkpoint_*`, …) are
scheme-independent and apply unchanged.

### 8.2 Authentication

`MicrosoftAzureBuilder::from_env` covers account key, client secret, MSI and
**workload identity federation** (`AZURE_FEDERATED_TOKEN_FILE`). The last is what
an AKS deployment should use, and it is the direct analogue of the IRSA path in
the serverless roadmap (#364). No credential code of our own.

### 8.3 Retry policy

Azure throttles at the *account* and *storage-partition* level (`503 ServerBusy`,
`500 OperationTimedOut`), not per object. That failure shape is S3's, not GCS's —
so the Azure arm takes the S3 arm's long, gentle budget (32 retries / 300 s,
`lib.rs:2784`), **not** the GCS arm's fail-fast one (5 retries / 15 s), which
exists only because a GCS per-object throttle is unwinnable by waiting.

### 8.4 Feature flag

Same `dynostore` feature as S3 and GCS — one more `object_store` feature, no new
axis in the build matrix. Revisit only if binary size bites.

## 9. Work breakdown

| PR | Content |
|---|---|
| 1 | **Done (#417).** Live spike against a real ADLS Gen2 account: `list_with_offset` with `startFrom` (§4.1), directory entries in listings (§5.1), empty-directory residue after delete (§5.2), Blob Batch on HNS (§5), and HNS sort order (§4.2, which #421 depends on). Findings in §4 and §5. No product code. |
| 2 | **Done (#418).** `object_store` `azure` feature; `abfss`/`az`/`abfs` arm in `Builder::build`, S3-shaped retry, no rate limiter. Fetch still broken. |
| 3 | **Done (#419).** Suffix-range decorator (§3, option A) + unit tests over `InMemory` with suffix rejection injected. Fetch works. |
| 4 | Azurite in `compose.yaml`, `just az-up` / `just broker-az`, `just test-conditional-put az://tansu/` wired into `pr.yml` with §7's exclusions written down. |
| 5 | `example.env`, README storage table, account-configuration contract (§6.3), private-endpoint note (§5.3), the ordering constraint comment at `segment_location` (§4.2). |
| 6 | `docs/storage-tuning.md`: Azure transaction model, including the billed-delete change (§6.1). |
| 7 | Optional: size caching (§3, option B) if measurements justify it. |

## 10. What this RFC deliberately does not decide

- Whether ADLS becomes a *supported* backend (README, SLA) or an experimental
  one. Depends on §4.3.
- Any change to the segment format (§3, option C).
- `Set Blob Expiry`-based retention (§6.2).
- Absolute pricing figures — region- and redundancy-dependent, and to be quoted
  from the pricing page at the time of writing, not from here. Only the *shape*
  of the difference (§6.1) is asserted.

## Sources

- [List Blobs (REST API)](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs) — `startFrom`, HNS sort order
- [Known issues with Azure Data Lake Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues) — unsupported Blob APIs, directory delete, private endpoints
- [Blob Batch (REST API)](https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch) — 256 subrequests, per-subrequest billing
- [Set Blob Expiry (REST API)](https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-expiry) — HNS-only, expiry options
