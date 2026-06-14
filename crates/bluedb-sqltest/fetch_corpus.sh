#!/usr/bin/env bash
# Fetch the SQLite sqllogictest corpus subset used by the conformance baseline.
#
# This data is third-party (public-domain SQLite test data) so it is gitignored
# rather than vendored. Run this once to populate slt/corpus/.
#
# Note: uses BSD/macOS `sed -i ''`. On GNU/Linux change to `sed -i`.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="$HERE/slt/corpus/sqlite"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

git clone --depth 1 https://github.com/gregrahn/sqllogictest.git "$TMP"

mkdir -p "$DEST/evidence"
cp "$TMP"/test/evidence/*.test "$DEST/evidence/"
cp "$TMP"/test/select[1-5].test "$DEST/"

# sqllogictest-rs's parser rejects an inline "# comment" after an onlyif/skipif
# condition; strip it so every file parses.
find "$DEST" -name '*.test' -print0 | xargs -0 sed -i '' -E '/^(onlyif|skipif) /{ s/[[:space:]]*#.*$//; }'

echo "sqlite corpus: $(find "$DEST" -name '*.test' | wc -l) files under $DEST"

# --- DuckDB corpus subset ---
# DuckDB's tests use *literal* expected results (not MD5 hashes like the SQLite
# suite), so PASS actually measures correctness. DuckDB-extension files that
# sqllogictest-rs can't parse (require/loop/foreach/mode) are skipped by the
# runner. We take a focused subset of core standard-SQL areas.
DDEST="$HERE/slt/corpus/duckdb"
DTMP="$(mktemp -d)"
trap 'rm -rf "$TMP" "$DTMP"' EXIT

git clone --depth 1 --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$DTMP"
git -C "$DTMP" sparse-checkout set test/sql
for area in aggregate filter order projection subquery join cte cast types; do
    if [ -d "$DTMP/test/sql/$area" ]; then
        mkdir -p "$DDEST/$area"
        cp -r "$DTMP/test/sql/$area/." "$DDEST/$area/"
    fi
done

echo "duckdb corpus: $(find "$DDEST" -name '*.test' | wc -l) files under $DDEST"
