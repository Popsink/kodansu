# Azure Data Lake Storage Gen2

Operator guide for `abfss://` and `az://`. The design reasoning is in
[docs/rfc-adls.md](rfc-adls.md); this page is what you need to run the thing.

**Support level: experimental, by decision rather than by omission.** Every
assertion the broker makes about conditional put is verified against Azurite on
every PR, which is more than the S3 or GCS arms get. There is no nightly run
against a real ADLS Gen2 account and there is not going to be one: that was
decided for `gs://` and `az://` together (RFC §4.3), and without it "supported"
would be a claim CI cannot stand behind.

So what has been verified against a real hierarchical-namespace account was
verified **once, by hand** (#417) — true of `object_store` 0.14.1 on 3 September
2026, and nothing will notice if a future version changes it. Before promoting
this backend, or when `object_store` changes its Azure client, reproduce that
spike from [docs/rfc-adls.md](rfc-adls.md) §4 and §5: they record the method and
the observed result for each question, deliberately, because the harness itself
was throwaway and is not in the tree.

[docs/testing.md](testing.md) is precise about the shape of the gap.

## The URL

| Form | When |
|---|---|
| `abfss://<container>@<account>.dfs.core.windows.net/` | **Canonical.** What an ADLS Gen2 user already writes, and it carries the account |
| `az://<container>/` | Short form; the account comes from `AZURE_STORAGE_ACCOUNT_NAME` |
| `abfs://<container>@<account>.dfs.core.windows.net/` | The same Hadoop convention over http |

`wasbs://`, `adl://`, `azure://` and a raw
`https://<account>.blob.core.windows.net/` are **not** accepted, even though
`object_store`'s own parser understands them. Every extra alias is another
spelling a deployment can drift onto, and `wasbs://` in particular is the legacy
Blob scheme — it should fail loudly rather than be quietly treated as Gen2.

A `dfs` host in the URL is resolved to `<account>.blob.core.windows.net`:
`object_store` speaks the **Blob** endpoint against an ADLS Gen2 account, not the
DFS one. That matters for private endpoints, below.

Every storage-URL query parameter is scheme-independent and applies unchanged —
`coalesce_*`, `batch_min_size`, `batch_max_delay`, and the rest of
[docs/storage-tuning.md](storage-tuning.md).

## Credentials

From the environment, via `MicrosoftAzureBuilder::from_env`, which covers:

| Mechanism | Variables |
|---|---|
| Account key | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` |
| Client secret | `AZURE_STORAGE_CLIENT_ID`, `AZURE_STORAGE_CLIENT_SECRET`, `AZURE_STORAGE_TENANT_ID` |
| Managed identity | `AZURE_STORAGE_CLIENT_ID` (optional; IMDS otherwise) |
| **Workload identity federation** | `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_STORAGE_CLIENT_ID`, `AZURE_STORAGE_TENANT_ID` |

**Workload identity is the AKS path**, and the analogue of IRSA on EKS. Prefer it
over an account key: it is the one mechanism with nothing to rotate.

Whichever you use, the identity needs the **`Storage Blob Data Contributor`**
role on the account or container. `Owner` is an ARM role and grants no data-plane
access at all — this is the first thing that goes wrong, and it fails as a `403
AuthorizationFailure` with no hint about which role is missing.

## Account configuration is part of the contract

The layout is create-only and immutable, and retention is implemented by delete.
So four account settings are not preferences:

| Setting | Required | Why |
|---|---|---|
| Blob versioning | **off** | Every superseded write is retained and billed. The broker keeps working — the CAS is on the etag and `object_store` ignores the version on a conditional put — so this fails as a bill, not as an error |
| Blob soft delete | **off**, or minimal | Supported on hierarchical namespace, but a retention delete then frees no space |
| Immutability / legal hold | **absent** | It breaks the delete path outright |
| Access tier | **hot** | Cool and cold add per-GB read charges that this access pattern pays constantly |

**A freshly created account already satisfies all four**, verified on a
`StorageV2` account created with nothing but
`--enable-hierarchical-namespace true --access-tier Hot`: versioning unset, blob
soft delete `false`, no container soft delete. So read this table as *"do not
turn these on"* rather than *"remember to turn these off"* — the deployment that
gets it wrong is one that hardened a pre-existing data-lake account, not one that
followed the default path.

None of the four is detectable from the data plane, which is why there is no
`ping()`-time warning for them. What `Get Account Information` does return is the
account kind, the SKU and whether hierarchical namespace is on — none of which
this broker requires a particular value of.

### Hierarchical namespace

HNS-on is the target and what a data-lake tenant will have. Two consequences,
both benign, both verified against a real HNS account (#417):

- **Deleting every blob under a prefix leaves the directories behind.** They are
  not billed for capacity, and they do not appear in the listings the broker
  issues on its hot paths, so the cost is an untidy account. They *are*
  removable — deepest-first `Delete Blob`, or one recursive `DELETE` against the
  DFS endpoint — and the broker does neither today.
- **`/` sorts lowest**, unlike ASCII. This does not reach the broker: the only
  offset listing it does runs to `…/segments/` and the remainder of every key is
  `{seq:0>20}.seg`, which contains no `/`. It is recorded as a constraint next to
  `segment_location`.

Flat Blob Storage is not the target and is not tested. It will probably work,
since only the Blob endpoint is ever used.

## Private endpoints

Azure's own guidance for an ADLS Gen2 account is to create private endpoints for
**both** the `blob` and `dfs` sub-resources, because *"some operations (such as
managing ACLs, creating directories, and deleting directories) require a DFS
private endpoint"*. The broker does none of those three — it never manages an
ACL, and directories are materialised and left behind by HNS rather than created
or removed by us — so blob-only is sufficient for Kodansu.

The failure worth knowing about is the other direction: a network team that
provisioned **`dfs` only** — the usual default for a data-lake account — leaves
the broker unable to connect, with nothing in the error naming the sub-resource.
If `abfss://` times out or refuses the connection and the account resolves, check
this before anything else.

## Costs

At the same shape as a measured production S3 fleet, ADLS Gen2 comes to **~1.11×
the S3 request bill**, and the difference is almost entirely the ~1.30×
hierarchical-namespace write premium — a ~30 % surcharge — rather than anything
the broker does differently. Two things worth knowing before sizing one:

- **A list is billed as a *read* on ADLS Gen2**, where S3 prices `LIST` with
  `PUT`. That makes a list ~9× cheaper here, and it collapses the LIST plane from
  18 % of that fleet's bill to under 2 %.
- **Read and write transactions are metered per 4 MiB.** An operation under
  4 MiB is one transaction; a 16 MiB one is four. The knob this touches is
  `prefix_compact_target_bytes` (default `16m`), so every merged segment write
  bills four times. Still overwhelmingly worth it — 256 writes become 4 — but it
  is arithmetic the S3 section does not have.

**Deletes are free**, exactly as on S3 — *"Storage accounts are not charged for
`Delete Blob` requests"*, and the pricing page's fourth category is "All other
Operations, except Delete, which is free". If you have read an earlier version of
this repository's ADLS RFC saying otherwise, that claim was wrong and is
retracted (#422).

`docs/storage-tuning.md` has the transaction model, the per-class arithmetic and
why cool and cold tiers are backwards for this access pattern.

## Running it locally

```shell
just broker-az                    # Azurite in compose, az://tansu/
just test-conditional-put-azurite # the conformance target against Azurite
```

`just az-up` alone brings up Azurite and creates the container. Note that a
green Azurite run does **not** cover `list_with_offset`, hierarchical namespace
or throttling — [docs/testing.md](testing.md) is explicit about which three
things it leaves out and why.
