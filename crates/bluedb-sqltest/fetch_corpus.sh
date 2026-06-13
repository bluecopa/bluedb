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

echo "corpus ready: $(find "$DEST" -name '*.test' | wc -l) files under $DEST"
