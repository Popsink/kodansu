# Copyright notices

Kodansu is a fork of [tansu](https://github.com/tansu-io/tansu). Both are
Apache-2.0. Apache-2.0 §4(c) obliges us to retain upstream's notices in the files
we derived from tansu; it does not ask us to *add* them to files we wrote, and for
a long time we did exactly that — every `.rs` file in the tree opened with Peter
Morgan's notice, including the ones that never existed upstream. This document
records what the notices say now, why, and how the classification was derived.

Enforced by `.github/scripts/check-copyright.py`, run as the `copyright` job on
every pull request. `just copyright` checks, `just copyright-fix` repairs.

## The three classes

Below the notice, every file carries the same 12-line Apache-2.0 grant, byte for
byte. Only the notice lines above it differ.

| class | count | notice |
|---|---|---|
| inherited from tansu | 162 | Peter Morgan's line, unchanged |
| inherited, rewritten in place here | 17 | Peter Morgan's line, then Popsink's |
| written at Popsink | 93 | Popsink's line alone |

The first two rows are the 179 files `copyright.toml` calls `inherited`; the middle
row is the subset it also calls `joint`. 162 + 17 + 93 = 272, every tracked `.rs`
file.

Popsink's line is `// Copyright ⓒ 2026 Popsink SAS`. It claims 2026 and no earlier
year: the fork point is 2026-05-16 and all 469 commits since are dated 2026, so
there is no 2025 Popsink work for a `2025-2026` range to cover.

Upstream's line appears in two year ranges, `2024-2025` and `2024-2026`. Both are
upstream's and neither is ours to renumber, so the check accepts either one
wherever an inherited notice is expected.

`copyright.toml` lists the inherited and rewritten-in-place files explicitly.
Everything else must carry Popsink's notice — **the default is Popsink, not
upstream**, which is the point: the drift this fixes happened because new files
were created by copy-pasting old ones, and a check that defaulted to accepting
upstream's notice would wave that through again. The `inherited` list is closed to
new code: it shrinks when an inherited file is deleted and an entry is edited when
one is moved, but it never grows on its own. Upstream code can only enter the tree
through a deliberate port, and adding a line to `inherited` is the reviewable act
that records one.

The check also validates `copyright.toml` against itself before it looks at the
tree — a path listed twice, a path that is no longer tracked, or a `joint` entry
missing from `inherited` all fail — because a stale entry there silently
reclassifies a file rather than erroring.

## Reproducing the classification

```bash
git remote add tansu-io https://github.com/tansu-io/tansu.git
git fetch tansu-io main
BASE=$(git merge-base origin/main tansu-io/main)   # ae28fa7, 2026-05-16
git diff --no-renames --name-status "$BASE" HEAD -- '*.rs'
```

That gives 102 added, 98 modified, 44 deleted, and 72 tracked files it does not
mention because they have not changed since the fork.

Three things a naive reading of that output gets wrong.

**Use `--no-renames`.** With rename detection on, git reports two renames that are
not renames: `tansu-schema/src/lake/tansu.rs` → `fuzz/fuzz_targets/fuzz_member_metadata.rs`
at 54%, and `tansu-storage/src/slate/mod.rs` → `tansu-storage/src/azure.rs` at 49%.
Both targets are short files — 22 and 15 lines — of which 13 are the licence header
every file shares, so the header alone is most of the claimed similarity. The header
is what makes rename detection fire here, which makes it useless for this purpose.

**Nine of the added files are upstream code, not ours.** They were added by
`27df1fb` (2026-06-11), *feat(broker): integrate upstream consumer-group rework
(tansu-io#680)*, whose git author is Peter Morgan. They are new files by tree
comparison and upstream-authored in fact:

```
fuzz/fuzz_targets/fuzz_member_metadata.rs
tansu-broker/tests/cg_latency.rs
tansu-broker/tests/new_cg.rs
tansu-client/examples/group_consumer.rs
tansu-sans-io/src/consumer/assignor/{cooperative_sticky,round_robin,uniform}.rs
tansu-service/src/consumer.rs
tansu-storage/src/latency.rs
```

All nine exist upstream today, under `nisshi-*` paths — upstream has since renamed
its crates `tansu-*` → `nisshi-*`, so checking for them at their old paths reports
them absent. Four are byte-identical to upstream modulo that rename; the other five
have drifted on both sides since June.

So classification keys off **the authorship of the commit that added a file**, not
off `--diff-filter=A`:

```bash
git log --diff-filter=A -1 --format=%an -- <file>
```

which splits the 102 additions into 92 Alexandre Colella, 1 Pierre-Yves Péton, and
the 9 Peter Morgan above.

**No Popsink-added file is upstream code that was split out.** Worth checking,
because a file created by moving a module out of an upstream file is inherited code
under a new name, and the command above would call it ours. It does not happen here:
comparing each added file's distinctive lines (over 40 characters, header stripped)
against every line in the tree at the fork point, the highest overlap is 26%, and
those matches are all `ObjectStore` trait-impl boilerplate.

## Rewritten in place

Some inherited files have been rewritten to the point where most of what they
contain is now ours. The rule is mechanical, so that it is reproducible and so that
it does not have to be re-argued per file: **lines added since the fork are more
than half of the file today, and at least 100 of them.**

```bash
git diff --no-renames --diff-filter=M --numstat "$BASE" HEAD -- '*.rs' |
  while read add del f; do
    now=$(wc -l < "$f")
    echo "$(( add * 100 / now ))% add=$add now=$now $f"
  done | sort -rn
```

Seventeen files qualify, led by `tansu-storage/src/dynostore.rs` (89% of today's
content added here, +15864 lines) and
`tansu-broker/src/coordinator/group/administrator.rs` (88%, +6187).

Note that the measure is the share of the file that is *ours today*, not the share
of upstream's that was deleted. The two disagree, and only the first is relevant to
who holds the copyright in what the file now contains.
`tansu-storage/src/service.rs` is the clearest case: 95% of upstream's lines are
gone, but it went 1489 → 72 lines and 65 of those 72 are still upstream's, with 7
added here. It was gutted, not rewritten, and it does not qualify.

Both notices stay on these files. Apache-2.0 §4(c) retains the original either way.

## Related decisions

- **`workspace.package.authors`** is `["Popsink SAS"]`. It was
  `["Peter Morgan <peter.morgan@tansu.io>"]`, inherited by every member crate,
  while `homepage` and `repository` had already been re-pointed at Kodansu.
  Upstream's attribution lives in the file headers, in `README.md` and in this
  document, which is where it belongs; `authors` is published crate metadata and
  says who ships the crate. `fuzz` sets `publish = false` and needs no `authors`.
- **`.devcontainer/Dockerfile`** carried 13 lines of GNU AGPL-3.0 grant text — the
  only AGPL remnant in an otherwise Apache-2.0 tree, inherited unchanged from
  upstream, where it is still present. Peter Morgan's copyright line is kept
  verbatim and only the grant was replaced with the Apache-2.0 one every other file
  in the repo carries. This corrects an upstream inconsistency; it does not
  restate anyone's authorship. Worth reporting upstream.
- **`tansu-auth/LICENSE`** was the one member crate missing its copy. Added, and
  identical to the root `LICENSE` like the other nine.
- **`LICENSE:190`** reads `Copyright [yyyy] [name of copyright owner]`, which is
  *not* a defect and has deliberately been left alone. The root `LICENSE` is the
  canonical Apache-2.0 text (md5 `3b83ef96387f14655fc854ddc3c6bd57`) and line 190
  is inside its own "APPENDIX: How to apply the Apache License to your work". The
  placeholders are meant to stay placeholders; you fill them in your source
  headers, which is what this document is about. Filling them in would mean
  shipping a modified licence text.
- **The two header emails no longer disagree.** Upstream's headers say
  `peter.james.morgan@gmail.com` while `authors` said `peter.morgan@tansu.io`.
  Dropping upstream from `authors` removes the discrepancy rather than picking one
  of the two addresses on Peter Morgan's behalf; the headers keep the address he
  put in them.
- **There is no `NOTICE` file**, by choice. Apache-2.0 §4(d) only requires one if
  the work you received carried one, and upstream ships none. The per-file notices
  plus the README paragraph carry the attribution instead.

## Adding a file

New `.rs` files need Popsink's notice and the Apache-2.0 grant beneath it. Copying
the header off a neighbouring file is how the drift started, so let the tooling do
it: write the file, run `just copyright-fix`, and it prepends the right notice.
The one thing it will not do is synthesise a licence grant — if the grant is
missing or altered, the check reports it and leaves the file alone, because
writing a licence grant into a file is the author's call and not a formatting fix.

## What is not checked

Only tracked `*.rs` files. `Dockerfile` and `.devcontainer/Dockerfile` carry the
same notice in `#` comments and are not covered — there are two of them, they
change roughly never, and teaching the check a second comment syntax to guard two
files is not worth the code. If a third `#`-commented file with a header shows up,
extend the check rather than add to the exceptions.
