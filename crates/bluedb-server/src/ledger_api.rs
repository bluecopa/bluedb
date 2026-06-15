//! `/ledger/*` — the double-entry ledger HTTP surface over [`bluedb_ledger`].
//!
//! `POST /ledger/accounts` and `POST /ledger/transfers` take a JSON array (or a
//! single object) of specs and return one TigerBeetle-style result code per
//! input item, in order. `GET /ledger/accounts/{id}` and
//! `GET /ledger/transfers/{id}` return canonical state.
//!
//! 128-bit and 64-bit fields (`id`, `amount`, `user_data_*`, balances,
//! `timestamp`, …) cross the wire as **decimal strings** so JSON clients that
//! parse numbers as f64/i64 (browsers, Clojure/`cheshire`) don't truncate large
//! values. Inputs are lenient — a number is accepted too — but outputs are
//! always strings for those fields.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Map, Value};

use bluedb_ledger::{
    Account, AccountFlags, CreateAccountResult, CreateTransferResult, Transfer, TransferFlags,
};

use crate::{AppError, AppState};

// --- scalar parsing ---------------------------------------------------------

/// Parse a JSON value (decimal string, non-negative number, or null→0) to u128.
fn value_to_u128(v: &Value, field: &str) -> Result<u128, AppError> {
    match v {
        Value::String(s) => s
            .parse::<u128>()
            .map_err(|_| AppError::bad_request(format!("{field}: invalid integer '{s}'"))),
        Value::Number(n) => n
            .as_u64()
            .map(u128::from)
            .ok_or_else(|| AppError::bad_request(format!("{field}: not a non-negative integer"))),
        Value::Null => Ok(0),
        _ => Err(AppError::bad_request(format!("{field}: expected a string or number"))),
    }
}

/// A required field by key (missing → 400).
fn req_u128(obj: &Map<String, Value>, key: &str) -> Result<u128, AppError> {
    let v = obj
        .get(key)
        .ok_or_else(|| AppError::bad_request(format!("missing field '{key}'")))?;
    value_to_u128(v, key)
}

/// An optional field by key (missing/null → 0), parsed as u128.
fn opt_u128(obj: &Map<String, Value>, key: &str) -> Result<u128, AppError> {
    match obj.get(key) {
        Some(v) => value_to_u128(v, key),
        None => Ok(0),
    }
}

/// An optional field narrowed to `u64` (out of range → 400).
fn opt_u64(obj: &Map<String, Value>, key: &str) -> Result<u64, AppError> {
    u64::try_from(opt_u128(obj, key)?)
        .map_err(|_| AppError::bad_request(format!("{key}: exceeds u64")))
}

/// An optional field narrowed to `u32` (out of range → 400).
fn opt_u32(obj: &Map<String, Value>, key: &str) -> Result<u32, AppError> {
    u32::try_from(opt_u128(obj, key)?)
        .map_err(|_| AppError::bad_request(format!("{key}: exceeds u32")))
}

/// An optional field narrowed to `u16` (out of range → 400).
fn opt_u16(obj: &Map<String, Value>, key: &str) -> Result<u16, AppError> {
    u16::try_from(opt_u128(obj, key)?)
        .map_err(|_| AppError::bad_request(format!("{key}: exceeds u16")))
}

/// Normalize a request body (single object or array of objects) into objects.
fn body_objects(body: Value, what: &str) -> Result<Vec<Map<String, Value>>, AppError> {
    match body {
        Value::Object(map) => Ok(vec![map]),
        Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Value::Object(map) => Ok(map),
                other => Err(AppError::bad_request(format!("{what} must be JSON objects, got {other}"))),
            })
            .collect(),
        other => Err(AppError::bad_request(format!(
            "{what} body must be an object or array of objects, got {other}"
        ))),
    }
}

// --- spec parsing -----------------------------------------------------------

fn parse_account(obj: &Map<String, Value>) -> Result<Account, AppError> {
    let mut a = Account::input(req_u128(obj, "id")?, opt_u32(obj, "ledger")?);
    a.code = opt_u16(obj, "code")?;
    a.flags = AccountFlags(opt_u16(obj, "flags")?);
    a.user_data_128 = opt_u128(obj, "user_data_128")?;
    a.user_data_64 = opt_u64(obj, "user_data_64")?;
    a.user_data_32 = opt_u32(obj, "user_data_32")?;
    Ok(a)
}

fn parse_transfer(obj: &Map<String, Value>) -> Result<Transfer, AppError> {
    let mut t = Transfer::new(
        req_u128(obj, "id")?,
        req_u128(obj, "debit_account_id")?,
        req_u128(obj, "credit_account_id")?,
        req_u128(obj, "amount")?,
        opt_u32(obj, "ledger")?,
    );
    t.code = opt_u16(obj, "code")?;
    t.flags = TransferFlags(opt_u16(obj, "flags")?);
    t.pending_id = opt_u128(obj, "pending_id")?;
    t.timeout = opt_u32(obj, "timeout")?;
    t.user_data_128 = opt_u128(obj, "user_data_128")?;
    t.user_data_64 = opt_u64(obj, "user_data_64")?;
    t.user_data_32 = opt_u32(obj, "user_data_32")?;
    Ok(t)
}

// --- response rendering -----------------------------------------------------

fn account_json(a: &Account) -> Value {
    json!({
        "id": a.id.to_string(),
        "ledger": a.ledger,
        "code": a.code,
        "flags": a.flags.0,
        "debits_pending": a.debits_pending.to_string(),
        "debits_posted": a.debits_posted.to_string(),
        "credits_pending": a.credits_pending.to_string(),
        "credits_posted": a.credits_posted.to_string(),
        "user_data_128": a.user_data_128.to_string(),
        "user_data_64": a.user_data_64.to_string(),
        "user_data_32": a.user_data_32,
        "timestamp": a.timestamp.to_string(),
    })
}

fn transfer_json(t: &Transfer) -> Value {
    json!({
        "id": t.id.to_string(),
        "debit_account_id": t.debit_account_id.to_string(),
        "credit_account_id": t.credit_account_id.to_string(),
        "amount": t.amount.to_string(),
        "pending_id": t.pending_id.to_string(),
        "user_data_128": t.user_data_128.to_string(),
        "user_data_64": t.user_data_64.to_string(),
        "user_data_32": t.user_data_32,
        "timeout": t.timeout,
        "ledger": t.ledger,
        "code": t.code,
        "flags": t.flags.0,
        "timestamp": t.timestamp.to_string(),
    })
}

/// The snake_case wire name of a result code (via its `Serialize` impl).
fn account_result_str(r: CreateAccountResult) -> String {
    serde_json::to_value(r)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn transfer_result_str(r: CreateTransferResult) -> String {
    serde_json::to_value(r)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

// --- handlers ---------------------------------------------------------------

/// `POST /ledger/accounts` — create a batch of accounts; returns one result
/// code per item: `{ "results": [ { "index", "id", "result" }, ... ] }`.
pub async fn create_accounts(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let objects = body_objects(body, "account")?;
    let specs: Vec<Account> = objects.iter().map(parse_account).collect::<Result<_, _>>()?;
    let ledger = state.ledger().await?;
    let results = ledger
        .create_accounts(&specs)
        .await
        .map_err(|e| AppError::internal(format!("create_accounts: {e}")))?;
    let out: Vec<Value> = specs
        .iter()
        .zip(results)
        .enumerate()
        .map(|(i, (a, r))| json!({ "index": i, "id": a.id.to_string(), "result": account_result_str(r) }))
        .collect();
    Ok(Json(json!({ "results": out })))
}

/// `POST /ledger/transfers` — create a batch of transfers; returns one result
/// code per item, in order.
pub async fn create_transfers(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let objects = body_objects(body, "transfer")?;
    let specs: Vec<Transfer> = objects.iter().map(parse_transfer).collect::<Result<_, _>>()?;
    let ledger = state.ledger().await?;
    let results = ledger
        .create_transfers(&specs)
        .await
        .map_err(|e| AppError::internal(format!("create_transfers: {e}")))?;
    let out: Vec<Value> = specs
        .iter()
        .zip(results)
        .enumerate()
        .map(|(i, (t, r))| json!({ "index": i, "id": t.id.to_string(), "result": transfer_result_str(r) }))
        .collect();
    Ok(Json(json!({ "results": out })))
}

/// `GET /ledger/accounts/{id}` — canonical account state, or `404`.
pub async fn get_account(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id: u128 = id.parse().map_err(|_| AppError::bad_request("invalid account id"))?;
    let ledger = state.ledger().await?;
    match ledger.lookup_account(id).await.map_err(|e| AppError::internal(e.to_string()))? {
        Some(a) => Ok(Json(account_json(&a))),
        None => Err(AppError::not_found(format!("account {id} not found"))),
    }
}

/// `GET /ledger/transfers/{id}` — canonical transfer state, or `404`.
pub async fn get_transfer(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id: u128 = id.parse().map_err(|_| AppError::bad_request("invalid transfer id"))?;
    let ledger = state.ledger().await?;
    match ledger.lookup_transfer(id).await.map_err(|e| AppError::internal(e.to_string()))? {
        Some(t) => Ok(Json(transfer_json(&t))),
        None => Err(AppError::not_found(format!("transfer {id} not found"))),
    }
}
