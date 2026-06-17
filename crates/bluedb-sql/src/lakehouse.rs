//! `PRAGMA lakehouse_mirror` — runtime control of the Iceberg mirror.
//!
//! The mirror is always compiled in (no cargo feature, no separate process) and
//! is toggled purely at runtime. Two forms:
//!
//! ```sql
//! PRAGMA lakehouse_mirror = off;                 -- flip the default (opt-out)
//! PRAGMA lakehouse_mirror_table('docs', off);    -- override one table
//! ```
//!
//! GlueSQL doesn't understand these, so the server intercepts them before
//! `Glue::execute` (mirroring [`crate::parse_default_null_order`]) and forwards
//! the parsed [`LhPragma`] to the lakehouse engine.

/// A parsed `PRAGMA lakehouse_mirror` directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LhPragma {
    /// Flip the global default: `true` = mirror every table (opt-out), `false` =
    /// only explicitly-enabled tables (opt-in).
    GlobalDefault(bool),
    /// Override one table's mirror state, regardless of the default.
    Table(String, bool),
    /// Set the incremental-compaction bin-pack target file size, in bytes.
    TargetFileBytes(u64),
    /// Set the read-your-writes freshness tolerance for the analytical path, in
    /// *seal cycles* (`PRAGMA bluedb_read_wait_seal_n = N`). Governs how a
    /// `X-Bluedb-Min-Watermark` request that outruns the sealed Iceberg
    /// watermark is handled: serve fresh from the writer / wait / redirect / 503.
    /// `0` reverts to the engine default (1).
    ReadWaitSealN(u64),
}

/// Parse a `PRAGMA`/`SET lakehouse_mirror[...]` statement, or `None` if `sql`
/// isn't one. `on/true/1` enable; `off/false/0` disable.
pub fn parse_lakehouse_pragma(sql: &str) -> Option<LhPragma> {
    let lower = sql.trim().to_ascii_lowercase();
    let body = lower.strip_suffix(';').unwrap_or(&lower).trim();
    if !(body.starts_with("pragma ") || body.starts_with("set ")) {
        return None;
    }
    // Read-wait freshness tolerance form: bluedb_read_wait_seal_n = <integer>.
    // Checked before the mirror containment guard (which early-returns None).
    if let Some(rest) = body.split("bluedb_read_wait_seal_n").nth(1) {
        let value = rest.trim_start_matches([' ', '=']).trim();
        let value = value.split_whitespace().next().unwrap_or(value);
        return value.parse::<u64>().ok().map(LhPragma::ReadWaitSealN);
    }
    // Target-file-size form: lakehouse_target_file_bytes = <integer>. Checked
    // before the mirror containment guard (which early-returns None).
    if let Some(rest) = body.split("lakehouse_target_file_bytes").nth(1) {
        let value = rest.trim_start_matches([' ', '=']).trim();
        let value = value.split_whitespace().next().unwrap_or(value);
        return value.parse::<u64>().ok().map(LhPragma::TargetFileBytes);
    }

    if !body.contains("lakehouse_mirror") {
        return None;
    }

    // Per-table form: lakehouse_mirror_table('name', on|off)
    if let Some(rest) = body.split("lakehouse_mirror_table").nth(1) {
        let args = rest.trim().trim_start_matches('(').trim_end_matches(')');
        let mut parts = args.splitn(2, ',');
        let name = parts.next()?.trim().trim_matches('\'').trim_matches('"').trim();
        let flag = parts.next()?;
        if name.is_empty() {
            return None;
        }
        return parse_bool(flag).map(|on| LhPragma::Table(name.to_string(), on));
    }

    // Global form: lakehouse_mirror = on|off
    let after = body.split("lakehouse_mirror").nth(1)?;
    let value = after.trim_start_matches([' ', '=']).trim();
    parse_bool(value).map(LhPragma::GlobalDefault)
}

/// Interpret an on/off token (handles trailing junk, quotes).
fn parse_bool(token: &str) -> Option<bool> {
    let t = token.trim().trim_matches('\'').trim_matches('"').trim();
    let t = t.split_whitespace().next().unwrap_or(t);
    match t {
        "on" | "true" | "1" | "enable" | "enabled" => Some(true),
        "off" | "false" | "0" | "disable" | "disabled" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_global_and_per_table() {
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_mirror = off"),
            Some(LhPragma::GlobalDefault(false))
        );
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_mirror = on;"),
            Some(LhPragma::GlobalDefault(true))
        );
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_mirror_table('docs', off)"),
            Some(LhPragma::Table("docs".into(), false))
        );
        assert_eq!(
            parse_lakehouse_pragma("SET lakehouse_mirror_table('orders', on)"),
            Some(LhPragma::Table("orders".into(), true))
        );
    }

    #[test]
    fn parses_target_file_bytes() {
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_target_file_bytes = 134217728"),
            Some(LhPragma::TargetFileBytes(134_217_728))
        );
        assert_eq!(
            parse_lakehouse_pragma("SET lakehouse_target_file_bytes = 1048576;"),
            Some(LhPragma::TargetFileBytes(1_048_576))
        );
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_target_file_bytes = nope"),
            None
        );
    }

    #[test]
    fn parses_read_wait_seal_n() {
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA bluedb_read_wait_seal_n = 3"),
            Some(LhPragma::ReadWaitSealN(3))
        );
        assert_eq!(
            parse_lakehouse_pragma("SET bluedb_read_wait_seal_n = 0;"),
            Some(LhPragma::ReadWaitSealN(0))
        );
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA bluedb_read_wait_seal_n = nope"),
            None
        );
    }

    #[test]
    fn ignores_non_lakehouse_statements() {
        assert_eq!(parse_lakehouse_pragma("SELECT 1"), None);
        assert_eq!(parse_lakehouse_pragma("PRAGMA default_null_order='first'"), None);
        assert_eq!(parse_lakehouse_pragma("INSERT INTO t VALUES (1)"), None);
    }

    #[test]
    fn rejects_unknown_values() {
        assert_eq!(parse_lakehouse_pragma("PRAGMA lakehouse_mirror = maybe"), None);
        assert_eq!(parse_lakehouse_pragma("PRAGMA lakehouse_mirror_table('docs', )"), None);
    }
}
