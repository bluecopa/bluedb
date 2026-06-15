//! Memory-bounded, bin-packed, streaming sort-merge compaction (spec §5.1): rewrites a
//! bounded bin of small files to copy-on-write, peak memory ≈ one target-sized output file.
//! Built out in Task 3.5.
