//! `bluedb-testkit` — a PyO3 wheel that embeds the real bluedb-server axum app
//! in the Python test process. See
//! `docs/superpowers/specs/2026-06-16-bluedb-testkit-python-design.md`.
//!
//! The PyO3 surface lives behind the `python` feature (enabled by maturin) so the
//! pure-Rust core (`authz_cfg`, `embedded`) compiles and tests without libpython.

pub mod authz_cfg;
pub mod embedded;

#[cfg(feature = "python")]
mod py {
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::authz_cfg::{AuthzSpec, DEFAULT_TOKEN};
use crate::embedded::{EmbeddedConfig, EmbeddedServer};

/// A running in-process bluedb, bound to an ephemeral loopback port.
#[pyclass]
struct TestServer {
    inner: Option<EmbeddedServer>,
    #[pyo3(get)]
    base_url: String,
    #[pyo3(get)]
    token: Option<String>,
}

#[pymethods]
impl TestServer {
    #[new]
    #[pyo3(signature = (authz=None, token=None, admin_sql=true, flush_interval_ms=None, db_path=None, evidence_signing=false))]
    fn new(
        py: Python<'_>,
        authz: Option<Py<PyAny>>,
        token: Option<String>,
        admin_sql: bool,
        flush_interval_ms: Option<u64>,
        db_path: Option<String>,
        evidence_signing: bool,
    ) -> PyResult<Self> {
        let spec = parse_authz(py, authz, token)?;
        let cfg = EmbeddedConfig {
            db_path: db_path.unwrap_or_else(|| "bluedb".to_string()),
            admin_sql,
            authz: spec,
            evidence_signing,
            flush_interval_ms,
        };
        let server = py
            .detach(|| EmbeddedServer::start(cfg))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(TestServer {
            base_url: server.base_url().to_string(),
            token: server.token().map(str::to_string),
            inner: Some(server),
        })
    }

    /// Stop the server and join its thread. Idempotent.
    fn stop(&mut self, py: Python<'_>) {
        if let Some(mut s) = self.inner.take() {
            // The joined thread is pure Rust (no Python access), so detach from
            // the interpreter while it drains so other Python threads aren't blocked.
            py.detach(move || s.shutdown());
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> bool {
        self.stop(py);
        false
    }
}

/// Map the Python `authz`/`token` arguments to an `AuthzSpec`.
/// `None` => default superuser; `True`/`False` => default superuser / open;
/// `str` => raw env string; `dict[str, list[str]]` => scoped token map.
fn parse_authz(py: Python<'_>, authz: Option<Py<PyAny>>, token: Option<String>) -> PyResult<AuthzSpec> {
    let default_token = || token.clone().unwrap_or_else(|| DEFAULT_TOKEN.to_string());
    let Some(obj) = authz else {
        return Ok(AuthzSpec::DefaultSuperuser { token: default_token() });
    };
    let b = obj.bind(py);
    if let Ok(flag) = b.extract::<bool>() {
        return Ok(if flag {
            AuthzSpec::DefaultSuperuser { token: default_token() }
        } else {
            AuthzSpec::Open
        });
    }
    if let Ok(s) = b.extract::<String>() {
        return Ok(AuthzSpec::RawEnv(s));
    }
    if let Ok(d) = b.cast::<PyDict>() {
        let mut entries = Vec::with_capacity(d.len());
        for (k, v) in d.iter() {
            let tok: String = k.extract()?;
            let items: Vec<String> = v.extract()?;
            entries.push((tok, items));
        }
        return Ok(AuthzSpec::Map(entries));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "authz must be None, bool, str, or dict[str, list[str]]",
    ))
}

#[pymodule]
fn _bluedb_testkit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TestServer>()?;
    m.add("DEFAULT_TOKEN", DEFAULT_TOKEN)?;
    Ok(())
}

} // mod py
