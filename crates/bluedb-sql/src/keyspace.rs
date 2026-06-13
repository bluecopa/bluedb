//! Key encoding for the SlateDB-backed GlueSQL store.
//!
//! SlateDB is a single ordered byte-keyed keyspace. We pack two logical
//! namespaces into it — table **schemas** and table **data rows** — and we
//! rely on SlateDB's byte-ordered range [`scan`](slatedb::Db::scan) to read a
//! table's rows back in primary-key order. The encoding below is designed so
//! that a lexicographic scan over a per-table prefix yields rows in GlueSQL
//! [`Key`](gluesql_core::data::Key) order.
//!
//! # Layout
//!
//! All keys begin with a single-byte **tag** that partitions the keyspace and
//! keeps the two namespaces from interleaving:
//!
//! ```text
//! schema key:  [TAG_SCHEMA] <table_name_utf8>
//! row key:     [TAG_DATA]   <len(table)::u32-be> <table_name_utf8> <pk_cmp_be>
//! ```
//!
//! * `TAG_SCHEMA` (`0x01`) sorts before `TAG_DATA` (`0x02`), so the two
//!   namespaces never overlap and a data-prefix scan can never run into a
//!   schema key.
//! * For row keys the table name is **length-prefixed** with a big-endian
//!   `u32`. Without the length prefix a table named `t` and a table named `t2`
//!   would share an ambiguous prefix (`data/t...`); the explicit length makes
//!   each table's row range a clean, self-delimiting prefix regardless of the
//!   bytes a sibling table name happens to contain.
//! * The primary-key suffix is GlueSQL's own
//!   [`Key::to_cmp_be_bytes`](gluesql_core::data::Key::to_cmp_be_bytes) — a
//!   big-endian, order-preserving encoding whose byte comparison matches
//!   `Key`'s [`Ord`]. Because the table-name segment in front of it is fixed
//!   for a given table, byte-ordering the full row key is equivalent to
//!   ordering by the encoded primary key. That is what makes `ORDER BY <pk>`
//!   fall straight out of a `scan` with no in-memory sort.
//!
//! # Range scanning a table
//!
//! [`data_prefix`] returns `[TAG_DATA] <len> <table>`; every row of that table
//! has a key with exactly this prefix, and no other key does. To scan the
//! table we walk the half-open range `[prefix, prefix_upper_bound)` where the
//! upper bound is the prefix with its last byte incremented (see
//! [`prefix_upper_bound`]).

use gluesql_core::data::Key;

use crate::error::SqlError;

/// Tag byte for schema keys. Sorts before [`TAG_DATA`].
const TAG_SCHEMA: u8 = 0x01;
/// Tag byte for row-data keys.
const TAG_DATA: u8 = 0x02;

/// Encode the storage key for a table's schema record.
pub fn schema_key(table_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + table_name.len());
    key.push(TAG_SCHEMA);
    key.extend_from_slice(table_name.as_bytes());
    key
}

/// The shared prefix of every row key for `table_name`:
/// `[TAG_DATA] <len(table)::u32-be> <table_name_utf8>`.
pub fn data_prefix(table_name: &str) -> Vec<u8> {
    let name = table_name.as_bytes();
    let mut prefix = Vec::with_capacity(1 + 4 + name.len());
    prefix.push(TAG_DATA);
    prefix.extend_from_slice(&(name.len() as u32).to_be_bytes());
    prefix.extend_from_slice(name);
    prefix
}

/// Encode the full storage key for one row: the table prefix followed by the
/// primary key's order-preserving big-endian bytes.
pub fn row_key(table_name: &str, key: &Key) -> Result<Vec<u8>, SqlError> {
    let pk = key
        .to_cmp_be_bytes()
        .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
    let mut full = data_prefix(table_name);
    full.extend_from_slice(&pk);
    Ok(full)
}

/// Compute the exclusive upper bound for a prefix scan: the smallest byte
/// string strictly greater than every string starting with `prefix`.
///
/// We increment the last byte that is `< 0xFF`, dropping trailing `0xFF`s.
/// If `prefix` is empty or all `0xFF` there is no finite upper bound and we
/// return `None`, meaning "scan to the end of the keyspace".
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_and_data_namespaces_are_disjoint_and_ordered() {
        // Schema keys sort before all data keys.
        let s = schema_key("t");
        let d = data_prefix("t");
        assert!(s < d);
    }

    #[test]
    fn length_prefix_disambiguates_sibling_tables() {
        // Without the length prefix, "t" rows would be a prefix of "t2" rows.
        let t = data_prefix("t");
        let t2 = data_prefix("t2");
        assert!(!t2.starts_with(&t));
        // And a row in "t" must not fall inside "t2"'s scan range.
        let t_row = row_key("t", &Key::I64(99)).unwrap();
        let t2_prefix = data_prefix("t2");
        assert!(!t_row.starts_with(&t2_prefix));
    }

    #[test]
    fn row_keys_sort_by_key_order() {
        // Integer keys: byte order of row keys must match numeric order,
        // including across the sign boundary.
        let mut keys: Vec<Key> = vec![Key::I64(100), Key::I64(-5), Key::I64(0), Key::I64(7)];
        let mut encoded: Vec<Vec<u8>> = keys.iter().map(|k| row_key("t", k).unwrap()).collect();
        encoded.sort();
        keys.sort();
        let resorted: Vec<Vec<u8>> = keys.iter().map(|k| row_key("t", k).unwrap()).collect();
        assert_eq!(encoded, resorted);
    }

    #[test]
    fn prefix_upper_bound_basics() {
        assert_eq!(prefix_upper_bound(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
        assert_eq!(prefix_upper_bound(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_upper_bound(&[]), None);
    }
}
