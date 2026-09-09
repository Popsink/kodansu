# Non-doc comment density over the Rust sources (#540). Driven by `just comments`,
# which decides which files to feed it; this program only counts and judges.
#
# `-v ceiling=N` fails the run when the repo-wide ratio exceeds N percent.
# `-v top=N` caps the per-file listing.
#
# WHAT IS COUNTED, per file, after the Apache licence header is dropped:
#
#   doc      a line beginning `///` or `//!` — API documentation, encouraged,
#            and load-bearing here (`broken_intra_doc_links = "deny"`). Never
#            counted against anything, in either half of the ratio, because a
#            total-comment threshold flags the wrong file: half the lines of
#            `tansu-service/src/lib.rs` are comment, and the split is 308 doc
#            against 4 non-doc.
#   non-doc  a line beginning `//` that is not a doc comment. The population the
#            rule is about, and 6,387 of its 6,519 lines are indented, i.e.
#            inside a body.
#   cited    a non-doc BLOCK — a run of adjacent non-doc lines — with `#123`
#            somewhere in it. Exempt: an issue number is the strongest evidence
#            a comment records a decision rather than narrating the next line.
#            The citation carries the whole block because a block is one thought.
#   code     everything else that is not blank, INCLUDING a line of code with a
#            trailing `// …` on it. That is a hole, and it is deliberate: of the
#            312 lines here that carry `//` past column 0, 214 are `//` inside a
#            string literal — `Url::parse("memory://tansu/")` and its kin. So
#            finding a trailing comment means lexing Rust string literals, and
#            a regex that does not is wrong on two lines out of three.
#
# The ratio is non-doc / (code + non-doc): narration as a share of the lines
# that are either code or narration. Blanks and doc comments are outside it.
#
# Two things the tree makes safe to ignore. Block comments (`/* */`) do not
# occur in it at all — the single `/*` that `git grep` finds is a glob inside a
# doc comment in `tansu-storage/src/audit.rs`. And every file but one carries
# the licence header, the exception opening with `//!`.

FNR == 1 {
  flush_block()
  if (file != "") record(file)
  file = FILENAME
  code = nondoc = cited = doc = 0
  # The 13-line Apache header, and the `//` separator most files put after it,
  # sits at the top of all but one file and totals 3,437 lines — counted, it
  # would more than double the non-doc figure. Keyed on the copyright line
  # rather than on "the file opens with comments", so that a file arriving
  # without the header keeps whatever comments it opens with.
  in_header = ($0 ~ /^\/\/ Copyright/)
}

{
  if (in_header) {
    if ($0 ~ /^\/\/$/ || $0 ~ /^\/\/ /) next
    in_header = 0
  }

  line = $0
  sub(/^[ \t]+/, "", line)

  if (line == "") { flush_block(); next }

  if (line ~ /^\/\/\// || line ~ /^\/\/!/) { flush_block(); doc++; next }

  if (line ~ /^\/\//) {
    block++
    if (line ~ /#[0-9]+/) block_cited = 1
    next
  }

  flush_block()
  code++
}

function flush_block() {
  if (block == 0) return
  if (block_cited) cited += block; else nondoc += block
  block = 0
  block_cited = 0
}

function record(f) {
  n = ++files
  f_name[n] = f
  f_nondoc[n] = nondoc
  f_ratio[n] = ratio(nondoc, code)
  f_cited[n] = cited
  f_doc[n] = doc
  f_code[n] = code
  t_nondoc += nondoc
  t_cited += cited
  t_doc += doc
  t_code += code
}

function ratio(n, c) { return (n + c) ? 100 * n / (n + c) : 0 }

END {
  flush_block()
  if (file != "") record(file)

  printf "%7s %7s %7s %7s %7s  %s\n", "ratio", "nondoc", "cited", "doc", "code", "file"

  # Worst offenders by absolute non-doc lines, not by ratio: the ratio puts a
  # 5-line file with two comments in it at the top and says nothing useful.
  # Selection sort over the top `top` rather than a sort() this awk may not have.
  shown = 0
  while (shown < top + 0) {
    best = 0
    for (i = 1; i <= files; i++)
      if (!seen[i] && f_nondoc[i] > 0 && (best == 0 || f_nondoc[i] > f_nondoc[best])) best = i
    if (best == 0) break
    seen[best] = 1
    shown++
    printf "%6.2f%% %7d %7d %7d %7d  %s\n", \
      f_ratio[best], f_nondoc[best], f_cited[best], f_doc[best], f_code[best], f_name[best]
  }

  total = ratio(t_nondoc, t_code)
  printf "%6.4f%% %7d %7d %7d %7d  %d files\n", \
    total, t_nondoc, t_cited, t_doc, t_code, files

  if (ceiling == "") exit 0

  # A tenth of a point here is ~110 lines of narration, so the comparison is at
  # the precision the ceiling is written to and nothing is rounded away.
  if (total > ceiling + 1e-9) {
    printf "\nFAIL: %.4f%% non-doc comments, ceiling %s%%\n", total, ceiling
    print  "The ceiling is a ratchet. Delete the narration, or make the comment"
    print  "say why and cite the issue it came from; raise the ceiling only in a"
    print  "change that says why the real number moved."
    exit 1
  }
  printf "\nOK: %.4f%% non-doc comments, ceiling %s%%\n", total, ceiling
}
