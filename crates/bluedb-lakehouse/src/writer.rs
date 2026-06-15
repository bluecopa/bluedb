//! Self-authored Iceberg commit writer (spec §5.2): writes Parquet data + equality-delete
//! files, then authors the data/delete manifests, manifest list, snapshot, and `metadata.json`
//! and publishes via bluedb's own catalog (never `Catalog::update_table`). Built out in
//! Tasks 2.5–2.7.
