# Blessed UAT Reports

This directory contains the checked-in release UAT snapshot.

- `latest-uat-report.md` is the current blessed full-profile report.
- `bluedb-<git-sha>-full-uat-report.md` files are immutable snapshots.
- `uat-badge.json` drives the README's Shields.io blessed UAT badge.

Regular CI runs write transient reports under `uat/reports/` and upload them as
workflow artifacts. Those transient reports are intentionally ignored by Git.
