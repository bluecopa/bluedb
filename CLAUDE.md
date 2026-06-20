# bluedb

## Always use superpowers skills

Before responding to any task, check whether a superpowers skill applies (even a
1% chance) and invoke it via the Skill tool. Announce it ("Using [skill] to ...")
and follow it. This is non-negotiable.

The skills that most often apply here:
- **brainstorming** before EnterPlanMode / designing anything non-trivial
- **systematic-debugging** for any bug, test failure, or unexpected behavior
- **test-driven-development** when writing or fixing code
- **verification-before-completion** before claiming a task is done
- **git-commit-push-pr** (or **git-commit**) for commit/PR work

If you skip a skill that plausibly applied, say so explicitly rather than
silently moving on.

## Humanize all prose (the humanizer rules, applied everywhere)

Apply the rules of the `humanizer` skill to **every piece of prose you write or
edit in this repo**, not only long-form content. That includes:

- mkdocs docs (`docs/**/*.md`)
- code comments (Rust `//` and `///`, Python `#`, doc strings)
- README, ROADMAP, CHANGELOG, PR descriptions, commit messages
- API error messages and user-facing strings

The aim is writing that reads as human. Concrete rules:

**Punctuation.** Keep it simple. Periods and commas first; a colon or parens
when they genuinely help. No em dashes or en dashes as sentence breaks (the
single most reliable AI tell; models reach for them constantly). Where you would
reach for an em dash, end the sentence, or use a comma, colon, or parentheses.
Do not stitch clauses together with semicolons for the same reason.

**Rhythm.** Vary sentence length on purpose. A short sentence next to a long
one. Do not fall into a uniform mid-length cadence. In comments, one-liners are
fine; in docs, let some sentences run and cut others short.

**Word choice.** Reach for the precise, sometimes unexpected word, not the one a
model reflexively picks. Prefer plain concrete terms over jargon.

**Banned phrases (do not write these).** "it's important to note", "in today's
fast-paced world", "delve", "tapestry", "navigate the landscape", "robust",
"leverage" (as a verb), "seamless", "unlock", "empower", "cutting-edge",
"game-changer", "revolutionize", "harness", "streamline", and over-hedged
claims generally.

**Structure.** Avoid the relentless three-part list where every item is the same
shape. Avoid formulaic openings. Do not over-polish; a draft sanded perfectly
smooth reads as machine output.

**Comments specifically.** Explain why, not what the code obviously does. Drop
the comment if the code is self-explanatory. A comment that just narrates the
next line is noise. Opinion and specificity are welcome.

This does not mean run the `humanizer` skill's full brief-generate-audit
procedure on every doc comment. It means these rules are the default style for
all prose in this repo. When a substantial doc page or marketing-ish passage is
being written, then invoke the `humanizer` skill proper for the full treatment.

## bluedb conventions

- The product name is **bluecopa** (lowercase). Never "BlueCopa". Applies to all
  prose, docs, site, and commits.
- **Commit-only, never push** unless explicitly told to push or raise a PR.
- **No `cargo fmt`** in this repo. No `rustfmt.toml` exists and rustfmt version
  skew reformats ~90 files. Match surrounding style by hand. `cargo clippy` is
  fine.
- **Run test suites directly with `2>&1`**. Do not pipe `cargo test` / `pytest`
  through `tail` / `grep` (it buffers everything and looks hung).
- **No unnecessary flags** (e.g. `--durations`, `-v`) unless there is a clear
  reason.
- Branch off `dev` for PRs; `dev` is the default branch.
