#!/usr/bin/env python3
# Copyright ⓒ 2026 Popsink SAS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Count, per tracked `*.rs` file, the lines `git blame` still attributes to
upstream tansu.

A line is upstream's when its blame commit is the fork point or an ancestor of
it, or one of the post-fork upstream ports listed in `copyright.toml` under
`[blame] ports`. The copyright notice and the Apache-2.0 grant at the top of a
file are not counted: they are identical everywhere and are replaced, not
rewritten, when a file leaves `inherited`.

Nor is a line of syntax alone: blank, punctuation only (`}`, `)]`), or an
attribute opener such as `#[derive(`. A rewrite that keeps a type's shape keeps
those lines, and blame matches them back to upstream however the code around
them changes; there is nothing in them to rewrite. The same holds for a bare
module declaration (`pub mod join;`): it names a file, and only renaming the
file changes it. And for a bare crate flag (`#![no_main]`), which a fuzz target
cannot do without and cannot spell any other way. And for a bare `Ok(())`, the
last line of every fallible test and unit-returning function.

Two measures:

  plain  `git blame -w -M`: lines whose last change is upstream's.
  moved  `git blame -w -M -C`: also follows lines moved in from another file
         changed in the same commit, which is how #550's split of
         `dynostore.rs` carried upstream lines into eighteen new files that
         plain blame calls entirely ours.

`--check` runs `plain` only and fails when a file *not* listed in `inherited`
has an upstream line, so that a file cannot leave the list while it still holds
upstream code. Without `--check`, both measures are reported for every
inherited file, with per-crate totals. See docs/copyright.md.

Needs full history: in a shallow clone the fork point is missing.
"""

import argparse
import os
import re
import subprocess
import sys
import tomllib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HEADER = re.compile(r"^([0-9a-f]{40}) \d+ \d+")
GRANT_LINES = 12
ATTRIBUTE_OPENER = re.compile(r"^#!?\[[\w:]+\($")
MODULE_DECLARATION = re.compile(r"^(pub(\([\w: ]+\))? )?mod \w+;$")
CRATE_FLAG = re.compile(r"^#!\[\w+\]$")
UNIT_OK = "Ok(())"


def syntax_only(text: str) -> bool:
    stripped = text.strip()
    return (
        not any(c.isalnum() for c in stripped)
        or bool(ATTRIBUTE_OPENER.match(stripped))
        or bool(MODULE_DECLARATION.match(stripped))
        or bool(CRATE_FLAG.match(stripped))
        or stripped == UNIT_OK
    )


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    ).stdout


def upstream_lines(
    root: Path, path: str, fork: str, ports: set[str], moved: bool
) -> int:
    flags = ["-w", "-M"] + (["-C"] if moved else [])
    out = git(root, "blame", *flags, "--line-porcelain", f"{fork}..HEAD", "--", path)

    count = 0
    lineno = 0
    notice = 0
    commit = None
    boundary = False
    for line in out.split("\n"):
        if line.startswith("\t"):
            lineno += 1
            text = line[1:]
            if lineno == notice + 1 and text.startswith("// Copyright"):
                notice += 1
            elif (
                lineno > notice + GRANT_LINES
                and (boundary or commit in ports)
                and not syntax_only(text)
            ):
                count += 1
            commit, boundary = None, False
        elif commit is None and (m := HEADER.match(line)):
            commit = m.group(1)
        elif line == "boundary":
            boundary = True
    return count


def crate(path: str) -> str:
    return path.split("/", 1)[0]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        action="store_true",
        help="fail if a file outside `inherited` has an upstream line",
    )
    args = ap.parse_args()

    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").strip())
    cfg = tomllib.loads((root / "copyright.toml").read_text(encoding="utf-8"))
    inherited = set(cfg["files"]["inherited"])
    fork = cfg["blame"]["fork_point"]
    ports = set(cfg["blame"]["ports"])

    for sha in [fork, *sorted(ports)]:
        probe = subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", f"{sha}^{{commit}}"],
            capture_output=True,
        )
        if probe.returncode != 0:
            print(
                f"error: commit {sha} from copyright.toml is not in this clone; "
                "blame needs full history (actions/checkout `fetch-depth: 0`)",
                file=sys.stderr,
            )
            return 2

    tracked = sorted(p for p in git(root, "ls-files", "-z", "*.rs").split("\0") if p)
    measures = [False] if args.check else [False, True]

    def measure(path: str) -> tuple[str, list[int]]:
        return path, [upstream_lines(root, path, fork, ports, m) for m in measures]

    with ThreadPoolExecutor(os.cpu_count() or 4) as pool:
        counts = dict(pool.map(measure, tracked))

    escaped = sorted(p for p in tracked if p not in inherited and counts[p][0])

    if args.check:
        for path in escaped:
            print(
                f"error: {path}: {counts[path][0]} line(s) blamed on upstream, "
                "but it is not listed in copyright.toml `inherited`",
                file=sys.stderr,
            )
        if escaped:
            print(
                f"\n{len(escaped)} file(s) hold upstream lines outside `inherited`. "
                "Either the file was derived from tansu and belongs in the list, or "
                "the remaining lines need rewriting first. See docs/copyright.md.",
                file=sys.stderr,
            )
            return 1
        held = sum(counts[p][0] for p in inherited)
        print(
            f"{len(tracked)} .rs files: upstream lines only in the "
            f"{len(inherited)} inherited files ({held} lines, plain blame)"
        )
        return 0

    lengths = {
        p: len((root / p).read_text(encoding="utf-8").split("\n")) for p in inherited
    }
    print(f"{'plain':>6} {'moved':>6} {'lines':>6}  inherited file")
    for path in sorted(inherited, key=lambda p: (-counts[p][1], p)):
        plain, moved = counts[path]
        print(f"{plain:>6} {moved:>6} {lengths[path]:>6}  {path}")

    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for path in inherited:
        t = totals[crate(path)]
        t[0] += counts[path][0]
        t[1] += counts[path][1]
        t[2] += 1
    print(f"\n{'plain':>6} {'moved':>6} {'files':>6}  crate")
    by_moved = sorted(totals.items(), key=lambda kv: -kv[1][1])
    for name, (plain, moved, files) in by_moved:
        print(f"{plain:>6} {moved:>6} {files:>6}  {name}")
    plain = sum(t[0] for t in totals.values())
    moved = sum(t[1] for t in totals.values())
    print(f"{plain:>6} {moved:>6} {len(inherited):>6}  total")

    ready = sorted(p for p in inherited if counts[p] == [0, 0])
    print(
        f"\n{len(ready)} inherited file(s) at zero on both measures, "
        "ready to leave the list"
    )
    for path in ready:
        print(f"  {path}")
    for path in escaped:
        print(f"error: {path}: upstream lines outside `inherited`", file=sys.stderr)
    return 1 if escaped else 0


if __name__ == "__main__":
    sys.exit(main())
