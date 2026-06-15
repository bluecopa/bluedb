//! `/evidence/*` — HTTP surface for the append-only evidence-chain substrate.
//!
//! Handlers mirror the [`super::ledger_api`] conventions: every handler calls
//! `state.require_active()?` for writes, `state.authorize(…)?` for scope, and
//! `state.tenant(&headers)?` for multi-tenancy on every request.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_evidence::EvidenceError;

use crate::{authz::Scope, AppError, AppState};

// --- error mapping ----------------------------------------------------------

fn map_evidence_err(e: EvidenceError) -> AppError {
    match e {
        EvidenceError::IdemConflict => {
            AppError::conflict("E_IDEM_CONFLICT: idempotency key reused with a different payload")
        }
        EvidenceError::ChainModeConflict(c) => {
            AppError::conflict(format!("E_CHAIN_MODE_CONFLICT: chain '{c}' already exists with a different mode"))
        }
        EvidenceError::EntryNotFound { chain, seq } => {
            AppError::not_found(format!("entry {seq} not found in chain '{chain}'"))
        }
        EvidenceError::NotWriter => {
            AppError::service_unavailable("node is a read-only replica (not the active writer)")
        }
        EvidenceError::Storage(e) => AppError::internal(format!("evidence storage: {e}")),
    }
}

// --- PUT /evidence/{chain} --------------------------------------------------

/// `PUT /evidence/{chain}` — ensure the chain exists with the given mode.
/// Body: `{ "verified"?: bool }` (defaults to `true`).
pub async fn create_chain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let verified = body
        .as_ref()
        .and_then(|Json(v)| v.get("verified"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    state
        .evidence(&tenant)
        .await?
        .create_chain(&chain, verified)
        .await
        .map_err(map_evidence_err)?;
    Ok(Json(json!({ "chain": chain, "verified": verified })))
}

// --- POST /evidence/{chain}/entries -----------------------------------------

/// `POST /evidence/{chain}/entries` — append events to a chain.
/// Body: `{ "events": [{ "type", "payload_b64", "at"? }], "idem_key"? }`.
pub async fn append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;

    let events = body
        .get("events")
        .and_then(|v| v.as_array())
        .ok_or_else(|| AppError::bad_request("missing or non-array 'events' field"))?;

    let mut entries = Vec::with_capacity(events.len());
    for (i, ev) in events.iter().enumerate() {
        let etype = ev
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::bad_request(format!("events[{i}]: missing 'type'")))?
            .to_string();
        let payload_b64 = ev
            .get("payload_b64")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::bad_request(format!("events[{i}]: missing 'payload_b64'")))?;
        let payload = B64
            .decode(payload_b64)
            .map_err(|e| AppError::bad_request(format!("events[{i}]: invalid base64: {e}")))?;
        let at = ev
            .get("at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        entries.push(bluedb_evidence::EntryInput {
            etype,
            payload,
            at,
            edges: Vec::new(), // Plan 3 wires edges
        });
    }

    let idem_key = body
        .get("idem_key")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let appended = state
        .evidence(&tenant)
        .await?
        .append(&chain, entries, idem_key.as_deref())
        .await
        .map_err(map_evidence_err)?;

    Ok(Json(json!({
        "base_seq": appended.base_seq,
        "seqs": appended.seqs,
    })))
}

// --- GET /evidence/{chain}/head ---------------------------------------------

/// `GET /evidence/{chain}/head` — current head sequence for the chain.
pub async fn head(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let seq = state
        .evidence(&tenant)
        .await?
        .head(&chain)
        .await
        .map_err(map_evidence_err)?;
    Ok(Json(json!({ "seq": seq })))
}

// --- GET /evidence/{chain}/entries ------------------------------------------

#[derive(Deserialize)]
pub struct EntriesQuery {
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub after: Option<i64>,
    pub limit: Option<usize>,
}

/// `GET /evidence/{chain}/entries` — read entries.
/// - `?from=&to=` → `read_range` (inclusive, defaults: from=1, to=head)
/// - `?after=&limit=` → `read_from`
/// - no params → from=1, to=head (full chain)
pub async fn read_entries(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Query(q): Query<EntriesQuery>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let ev = state.evidence(&tenant).await?;

    let entries: Vec<(i64, bluedb_evidence::EntryRecord)> =
        if q.after.is_some() || (q.from.is_none() && q.to.is_none() && q.after.is_some()) {
            // read_from path
            let after = q.after.unwrap_or(0);
            ev.read_from(&chain, after, q.limit)
                .await
                .map_err(map_evidence_err)?
        } else {
            // read_range path (default: from=1, to=head)
            let lo = q.from.unwrap_or(1);
            let hi = match q.to {
                Some(t) => t,
                None => ev.head(&chain).await.map_err(map_evidence_err)?,
            };
            ev.read_range(&chain, lo, hi)
                .await
                .map_err(map_evidence_err)?
        };

    let out: Vec<Value> = entries
        .into_iter()
        .map(|(seq, rec)| {
            let mut obj = json!({
                "seq": seq,
                "type": rec.etype,
                "at": rec.at,
                "redacted": rec.redacted,
            });
            if !rec.redacted {
                obj["payload_b64"] = Value::String(B64.encode(&rec.payload));
            }
            obj
        })
        .collect();

    Ok(Json(Value::Array(out)))
}
