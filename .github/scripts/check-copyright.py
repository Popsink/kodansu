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

"""Check that every tracked `*.rs` file opens with the notice `copyright.toml`
says it should.

Three classes, keyed off `copyright.toml`:

  inherited  derived from tansu -> one upstream notice line
  joint      inherited but rewritten here -> upstream line, then Popsink's
  popsink    written here -> Popsink's line alone (the default)

Below the notice every file carries the same 12-line Apache-2.0 grant, and the
check is byte-exact on it: a file that paraphrases the grant, or drops the blank
comment line, fails.

`--fix` rewrites the notice lines in place; the grant is never synthesised, so a
file whose grant does not match is reported, not repaired.
"""

import argparse
import subprocess
import sys
import tomllib
from pathlib import Path

# The 12 lines that follow the notice in every file, byte-exact.
GRANT = [
    "//",
    '// Licensed under the Apache License, Version 2.0 (the "License");',
    "// you may not use this file except in compliance with the License.",
    "// You may obtain a copy of the License at",
    "//",
    "// http://www.apache.org/licenses/LICENSE-2.0",
    "//",
    "// Unless required by applicable law or agreed to in writing, software",
    '// distributed under the License is distributed on an "AS IS" BASIS,',
    "// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "// See the License for the specific language governing permissions and",
    "// limitations under the License.",
]


def tracked_rs(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "*.rs"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return sorted(p for p in out.split("\0") if p)


def check_file(
    path: str,
    root: Path,
    notice: dict,
    inherited_paths: set[str],
    joint_paths: set[str],
) -> tuple[str, list[str]] | None:
    """Return `(message, repaired_lines)` if `path` is wrong, else `None`.

    `repaired_lines` is `None` when the file cannot be repaired automatically.
    """
    upstream = notice["upstream"]
    popsink = notice["popsink"]
    inherited = path in inherited_paths
    joint = path in joint_paths

    text = (root / path).read_text(encoding="utf-8")
    lines = text.split("\n")

    if joint:
        want_len, describe = 2, "upstream notice, then Popsink's"
        ok = len(lines) > 1 and lines[0] in upstream and lines[1] == popsink
    elif inherited:
        want_len, describe = 1, "the upstream notice, unchanged"
        ok = bool(lines) and lines[0] in upstream
    else:
        want_len, describe = 1, f"{popsink!r}"
        ok = bool(lines) and lines[0] == popsink

    # Locate the grant: it follows however many notice lines the file actually
    # opens with, so that a misclassified file reports the notice as the fault
    # rather than the grant.
    actual_len = 0
    for line in lines:
        if line.startswith("// Copyright"):
            actual_len += 1
        else:
            break
    grant = lines[actual_len : actual_len + len(GRANT)]

    if ok and grant == GRANT:
        return None

    problems = []
    if not ok:
        got = "<empty file>" if not text else lines[0]
        problems.append(f"opens with {got!r}, expected {describe}")
        if actual_len != want_len:
            problems.append(f"{actual_len} copyright line(s), expected {want_len}")
    if grant != GRANT:
        for i, (a, b) in enumerate(zip(grant + [None] * len(GRANT), GRANT)):
            if a != b:
                problems.append(
                    f"line {actual_len + i + 1}: {a!r} != {b!r} (Apache-2.0 grant)"
                )
                break

    # Only offer a repair when the grant is intact; we replace notice lines, we
    # do not write a licence grant into a file on the author's behalf.
    repaired = None
    if grant == GRANT:
        if joint:
            keep = lines[0] if lines and lines[0] in upstream else upstream[-1]
            notice = [keep, popsink]
        elif inherited:
            notice = [lines[0] if lines and lines[0] in upstream else upstream[-1]]
        else:
            notice = [popsink]
        repaired = notice + lines[actual_len:]

    return "; ".join(problems), repaired


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--fix", action="store_true", help="rewrite offending notices in place"
    )
    args = ap.parse_args()

    root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    cfg = tomllib.loads((root / "copyright.toml").read_text(encoding="utf-8"))
    raw = {key: list(cfg["files"][key]) for key in ("inherited", "joint")}
    inherited, joint = set(raw["inherited"]), set(raw["joint"])

    failures = []

    # `copyright.toml` is data CI depends on, so its own consistency is checked
    # before the tree is: a stale path here would silently reclassify a file.
    tracked = tracked_rs(root)
    if stray := joint - inherited:
        failures += [
            f"copyright.toml: {p!r} is in `joint` but not `inherited`"
            for p in sorted(stray)
        ]
    if gone := inherited - set(tracked):
        failures += [
            f"copyright.toml: `inherited` lists {p!r}, which is not tracked"
            for p in sorted(gone)
        ]
    for key, paths in raw.items():
        seen = set()
        failures += [
            f"copyright.toml: `{key}` lists {p!r} twice"
            for p in paths
            if p in seen or seen.add(p)
        ]

    fixed = []
    for path in tracked:
        result = check_file(path, root, cfg["notice"], inherited, joint)
        if result is None:
            continue
        message, repaired = result
        if args.fix and repaired is not None:
            (root / path).write_text("\n".join(repaired), encoding="utf-8")
            fixed.append(path)
        else:
            failures.append(f"{path}: {message}")

    for path in fixed:
        print(f"fixed {path}")
    for line in failures:
        print(f"error: {line}", file=sys.stderr)

    if failures:
        print(
            f"\n{len(failures)} problem(s). For a wrong notice, `just copyright-fix` "
            "rewrites it; if the notice is right and the classification is not, "
            "move the path in copyright.toml. See docs/copyright.md.",
            file=sys.stderr,
        )
        return 1
    print(f"{len(tracked)} .rs files: copyright notices match copyright.toml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
