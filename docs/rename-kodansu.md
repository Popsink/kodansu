# Renaming `tansu` → `kodansu`

The product is already called Kodansu in prose. This document is about the other
half: the binary, the crates, the image, the metric names and the object keys,
which are all still spelled `tansu`. It is a checklist, ordered so that the
breaking things are decided before anything is typed.

The rename is ~1140 occurrences of the string across ~180 Rust files plus the
build and ops surface. Almost all of that is mechanical. What follows is the part
that is *not* mechanical — the places where `sed` produces a clean diff and a
broken deployment.

## 0. Decide these first

Four of the identifiers below are contracts with something outside this repo. Each
is independently rename-able, and each has its own blast radius. Decide all four
before starting, because the cheap ones can ship in one PR and the expensive ones
probably should not ship at all.

| Identifier | Current | Contract with | Cost of renaming |
|---|---|---|---|
| Object key prefix | `clusters/{cluster_id}/…`, `CLUSTER_ID` default `tansu_cluster` | **the production bucket** | Full data migration or a dual-read window. See [§4](#4-the-object-store-is-the-expensive-one). |
| Metric names | `tansu_*` | Prometheus, Grafana, any alert rule | Dashboards and alerts break silently — the query returns no data, not an error. |
| OTLP `service.name` | `tansu-broker` (from `CARGO_PKG_NAME`) | Jaeger, the otel-collector sidecar config | Trace and metric filters stop matching. |
| Container image | `ghcr.io/popsink/tansu` | every k8s manifest, the `TANSU_IMAGE` env | Cheap if the old tag keeps being pushed for a release or two. |
| Helm values key | `tansu:` in `Popsink/data-plane` | **every self-hosted customer's `values.yaml`** | Helm ignores unknown keys, so a customer's `tansu.enabled: true` becomes a no-op and their broker disappears with no error. Needs a release that reads both keys. |

Recommendation: rename the crates, the binary and the image; **keep the metric
prefix and the cluster-id default alone** for now, and rename them later as two
separate, individually-announced changes. They are the two that can break
production without producing a compile error, a failing test or a red CI job.

## 1. Cargo / crate identity

- [ ] `Cargo.toml` workspace `members` — 10 directory names.
- [ ] `Cargo.toml` `[workspace.dependencies]` — 10 `tansu-* = { path = … }` entries.
- [ ] Each `*/Cargo.toml`: `name`, and every `tansu-*.workspace = true` dependency line.
- [ ] Rename the 10 crate directories themselves.
- [ ] Rust module paths: `use tansu_storage::…` → `use kodansu_storage::…` across ~180 files.
- [ ] `tansu/src/bin/tansu.rs` — the file name *is* the binary name. Rename the file,
      or add an explicit `[[bin]] name = …`.
- [ ] `workspace.package.authors` is still `Peter Morgan <peter.morgan@tansu.io>`.
      Decide whether to keep it as attribution (the Apache-2.0 headers already carry
      that) or set it to Popsink. Not a link, but it is published crate metadata.
- [ ] `.github/workflows/release.yml` runs `cargo publish --workspace` on `v*` tags.
      The `tansu-*` names on crates.io belong to upstream, so this job cannot
      currently succeed. It is `disabled_manually` today. **Decide: delete it, or
      keep it as the vehicle for `kodansu-*`** — and if the latter, all ten names
      have to be taken before the first tag, because `kodansu` cannot publish
      without `kodansu-broker` already on the registry.

      `kodansu` itself is taken (0.0.0, a documented placeholder pointing at this
      repository and at the container image). The other nine were **deliberately
      left free**: crates.io discourages holding names one does not intend to use,
      and a server distributed as an image has no reason to publish nine library
      crates nobody will depend on. Taking them is a decision that follows from
      keeping `release.yml`, not a precaution to take in advance.

      Ownership of `kodansu` is currently one personal account. Add a second owner
      (`cargo owner --add <login>`, or a GitHub team once one exists) — the product
      name should not have a bus factor of one.

The mechanical part is one `fd`/`sd` pass plus `cargo fmt`; `cargo check --workspace
--exclude fuzz --all-targets` catches everything it missed.

## 2. Build, image, CI

- [ ] `Dockerfile` — four references to the binary: `xx-cargo build --bin tansu`,
      `xx-verify … /release/tansu`, `cp … /release/tansu /image`, and
      `ENTRYPOINT ["/tansu"]`.
- [ ] `.github/workflows/publish.yml` — `IMAGE: ghcr.io/popsink/tansu`. The comment
      above it explains the hardcoded lowercase org; keep the comment accurate.
- [ ] `justfile` — ~50 occurrences: `target/{profile}/tansu` paths, the
      `docker-build` tags, the `tansu-broker` / `tansu-up` / `minio-tansu-bucket`
      recipe names, `--storage-engine=s3://tansu/`.
- [ ] `compose.yaml` — the `tansu:` service name and `${TANSU_IMAGE}`.
- [ ] `example.env` — `TANSU_IMAGE`, `CLUSTER_ID`, `STORAGE_ENGINE`, and the
      `RUST_LOG=warn,tansu_broker=debug,tansu_storage=debug` targets (these are
      **crate names**, so they break the moment the crates are renamed and they
      break *quietly* — logging just goes silent).
- [ ] `logs/tansu-*/` — five tracked directories, one per crate that logs. Tests write to
      `../logs/{CARGO_PKG_NAME}/…`, so a renamed crate needs its directory renamed
      too or every test in it fails on a missing path.
- [ ] `.github/workflows/ci.yml` is disabled and is upstream's; it still carries
      `TANSU_IMAGE: ghcr.io/tansu-io/tansu` and a `github.actor == 'shortishly'`
      gate. Delete it rather than renaming it.
- [ ] Branch protection requires the check named `test`. Renaming a job renames the
      required check; update the protection rule in the same change.
- [ ] `codebook.toml` / `typos.toml` — `typos.toml` excludes `tansu-sans-io/message`.

## 3. Observability

- [ ] Metric names: `tansu_api_requests_total`, `tansu_request_duration_*`,
      `tansu_group_coordinator_requests_total`, `tansu_group_forward_*`,
      `tansu_object_store_request_*`, `tansu_objectstore_cache_*`,
      `tansu_opticon_*`, `tansu_request_size_bytes`, `tansu_response_size_bytes`.
      **See §0 — the recommendation is to defer this.** If it goes ahead, the
      dual-emit window (both names for one release) is what makes it survivable.
- [ ] `etc/grafana/dashboards/home.json` — 20 PromQL expressions.
- [ ] OTLP `service.name` is `env!("CARGO_PKG_NAME")` in `tansu-broker/src/otel.rs`
      and `tansu-broker/src/lib.rs`. Renaming the crate renames the service —
      intended, but it must land with the collector/Jaeger-side filters.
- [ ] `tansu-cli/src/cli/user.rs` sets the Kafka `client_id` from `CARGO_PKG_NAME`.
      Cosmetic, but it shows up in broker-side logs.

## 4. The object store is the expensive one

`CLUSTER_ID` defaults to `tansu_cluster` and *is* the object key prefix:

```
clusters/{cluster}/prefixes/{org.env.conn}/segments/{seq:020}.seg
```

Production sets `CLUSTER_ID` explicitly, so changing the **default** is safe on its
own — but it is exactly the kind of change that gets read as cosmetic and then
silently strands a dev or staging bucket, because a broker with a new prefix sees
an empty cluster rather than an error. Every topic, every offset, every consumer
group is gone from its point of view.

If the prefix is ever to change, it needs a real migration: copy under the new
prefix, run both, cut over, delete. That is a project, not a checklist item.
The default in `tansu-cli/src/cli/broker.rs` and the `memory://tansu/` /
`s3://tansu/` bucket names in the same file, `example.env` and ~50 test call sites
are all separate decisions from it.

## 5. Kubernetes / production

Not in this repo — the manifests live in the deployment repo — but they break in
lockstep and need to be in the same rollout:

- [ ] namespaces `tansu-external`, `tansu-maintain`
- [ ] container name `tansu` (`kubectl logs -c tansu` and anything scripted on it)
- [ ] image tag, the otel-collector sidecar's `service.name` relabelling
- [ ] ~~`GROUP_FORWARD_PEER_DNS` — the headless Service name~~ — **no longer applies
      to a 1.0 fleet.** Forward-to-owner coordination was deleted in #360, and the
      only tag containing that removal is `v1.0.0-alpha.1`. The deployment repo is
      already clean of it; what is *not* clean is `Popsink/data-plane`, whose chart
      still pins `0.7.0-beta.39` and therefore still ships `GROUP_FORWARDING`,
      `GROUP_FORWARD_PEER_DNS`, `INTERNAL_LISTENER_URL`, `POD_IP`, a headless
      Service and a public `tansu.groupForwarding` values key — as a **live**
      feature, not dead weight. Removing any of it is gated on that chart moving to
      ≥ `1.0.0-alpha.1`.

      More generally: production runs three broker generations at once
      (`eks-mb-production` on `1.0.0-alpha.1`, both GCP fleets on `0.7.0-beta.16`,
      the self-hosted chart on `0.7.0-beta.39`). Any cleanup of the form "the broker
      no longer does X, so its config can go" has to be answered per fleet.
- [ ] Prometheus alert rules and any saved Grafana dashboard, per §3.

## 6. Repository and docs

- [x] **Done.** `Popsink/tansu` → `Popsink/kodansu`; the old path answers `301` and
      GitHub redirects web and git URLs indefinitely, so clones and open pull
      requests keep working. Update `origin` on local checkouts anyway, and never
      create a new repository named `tansu` in the organisation — that is the one
      thing that breaks the redirect.

      It turned out to be nearly free, for a reason worth writing down: searching
      the organisation for `Popsink/tansu` returns ~20 files, and almost all of them
      are `ghcr.io/popsink/tansu` — **image paths, not repository URLs.** A GHCR
      package name does not follow the repository it was built from, so the rename
      touched none of them. The two registry paths (`ghcr.io/popsink/tansu` and the
      `onprem/tansu` the data-plane chart pulls) plus
      `infra-terraform/.github/scripts/test_sync_images.py` belong to §7 step 3.
- [ ] `README.md` — nine of the ten crate READMEs are **symlinks to it**
      (`tansu-auth` has none), so there is one file to edit, not ten — and the
      symlinks are relative, so renaming the crate directories keeps them valid.
      Its links are all correct today; what changes at
      rename time is the body: the `tansu` command examples, the subcommand table,
      the `[kodansu-issues]` target, and the "On the name" paragraph at the bottom,
      which exists only to explain this gap and should be deleted when it closes.
- [ ] `CLAUDE.md` — the "the binary, the crates and the container image are all still
      named `tansu`" paragraph, and the crate table.
- [ ] `docs/*.md` — `docs/testing.md` and `docs/storage-tuning.md` name crates and
      the `s3://tansu/` URL.
- [ ] `tansu-sans-io/src/lib.rs` links to `blog.tansu.io`. That is an upstream
      article about upstream's design; keep it, it is a citation.

## 7. Suggested sequencing

1. **Done — delete what would otherwise be renamed twice.** [§8](#8-dead-weight--already-removed),
   plus upstream's `ci.yml`: `disabled_manually`, gated on `github.actor ==
   'shortishly'`, and carrying `ghcr.io/tansu-io/tansu`. The merge gate comes from
   `pr.yml`, which owns the `test` context branch protection requires, so deleting
   it changed no required check. `kodansu` was reserved on crates.io in the same
   pass — see §1 on why the other nine names were deliberately *not* taken.
2. **Done — GitHub repo rename** and the README/CLAUDE.md prose. Moved ahead of the
   code because it is independent of all of it: no image path, no manifest and no
   crate name refers to the repository. See [§6](#6-repository-and-docs).
3. **PR — crates, binary, modules, logs dirs, justfile, Dockerfile.** Pure
   compile-time; CI proves it. The image keeps its old name.
4. **PR — image and manifests.** Push both tags for one release, then drop the old.
5. **PR — the `tansu:` Helm values key in `Popsink/data-plane`,** read both for one
   release before dropping the old one. This is the only step with a failure mode
   that reaches customers who did not ask for any of this.
6. **Later, separately, each with its own announcement** — metric prefix (dual-emit
   for one release), `service.name`, and `CLUSTER_ID`. Or never; there is no cost to
   a broker named kodansu that emits `tansu_*` metrics beyond the mild confusion of
   reading it.

## 8. Dead weight — already removed

Renaming is the moment to stop paying for code nobody uses, so this happened first:
`tansu-proxy` (2 791 lines, no integration tests, never deployed), `tansu-perf`
(860 lines, never invoked — every `*-perf` recipe shells out to
`kafka-producer-perf-test`) and `tansu-otel` (77 lines, depended on by the proxy
and by nothing else — the broker has its own `src/otel.rs`) are gone, along with
the `proxy` and `perf` subcommands and the dashboard's dead `Proxy` row.

That is ~3 700 lines, two subcommands, three crate directories, one `logs/`
directory and three sets of Cargo metadata the rename no longer has to touch. The
counts above in this document are already net of it: the workspace is **10 crates**,
not thirteen.
