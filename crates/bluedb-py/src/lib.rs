//! `bluedb-testkit` — a PyO3 wheel that embeds the real bluedb-server axum app
//! in the Python test process. See `docs/superpowers/specs/2026-06-16-bluedb-testkit-python-design.md`.
//!
//! The PyO3 surface lives behind the `python` feature (enabled by maturin) so the
//! pure-Rust core compiles and tests without libpython.

pub mod authz_cfg;
pub mod embedded;

#[cfg(feature = "python")]
mod py {
    use pyo3::prelude::*;

    #[pymodule]
    fn _bluedb_testkit(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("DEFAULT_TOKEN", crate::authz_cfg::DEFAULT_TOKEN)?;
        Ok(())
    }
}
