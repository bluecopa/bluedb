//! Key encoding for the SlateDB-backed GlueSQL store.
//!
//! SlateDB is a single ordered byte-keyed keyspace. Every key produced here is
//! **tenant-namespaced** so that many tenants can share one [`slatedb::Db`]
//! with no chance of a cross-tenant read: a tenant's keys form a contiguous,
//! self-delimiting byte range that no other tenant's keys can fall inside.
//!
//! Inside a tenant we pack four logical namespaces — table **schemas**, table
//! **data rows**, secondary-**index definitions**, and secondary-**index
//! entries** — and we rely on SlateDB's byte-ordered range
//! [`scan`](slatedb::Db::scan) to read rows back in primary-key (or
//! indexed-value) order. The encoding below is designed so that a lexicographic
//! scan over a per-table prefix yields rows in the right
//! [`Key`](gluesql_core::data::Key) order.
//!
//! # Layout
//!
//! Every key begins with the tenant prefix, then a single-byte **tag** that
//! partitions the keyspace within the tenant:
//!
//! ```text
//! <tenant>      := <len(tenant)::u32-be> <tenant_utf8>
//!
//! schema key:    <tenant> [TAG_SCHEMA]    <table_utf8>
//! row key:       <tenant> [TAG_DATA]      <len(table)::u32-be> <table_utf8> <pk_cmp_be>
//! index entry:   <tenant> [TAG_INDEX]     <len(table)::u32-be> <table_utf8>
//!                          <len(index)::u32-be> <index_utf8>
//!                          <value_cmp_be escaped + 0x00 0x00> <pk_cmp_be>
//! external key:  <tenant> [tag >= TAG_EXTERNAL_BASE] <caller-encoded suffix>
//! ```
//!
//! The indexed `<value>` is wrapped in an order-preserving, self-terminating
//! encoding (see "Index entry keys" below); the trailing `<pk_cmp_be>` is the
//! raw primary key bytes, which need no terminator because nothing follows.
//!
//! Note there is no separate index *definition* namespace: a table's index
//! definitions live inside its [`Schema::indexes`](gluesql_core::data::Schema)
//! record (the `TAG_SCHEMA` value), which is exactly where GlueSQL's query
//! planner looks for them. The `TAG_INDEX` namespace holds only the per-row
//! *entries*.
//!
//! * The tenant prefix is length-prefixed with a big-endian `u32` so tenants
//!   `"t"` and `"t2"` never share an ambiguous prefix (the same trick used for
//!   table names below). Because the prefix is fixed-width-then-bytes, two
//!   different tenants can never produce overlapping key ranges.
//! * `TAG_SCHEMA` < `TAG_DATA` < `TAG_INDEX` so the three namespaces never
//!   interleave and a prefix scan in one can never run into a key from another.
//! * Tags `>= TAG_EXTERNAL_BASE` (0x10) are reserved for layers above
//!   bluedb-sql (e.g. bluedb-ledger) and never collide with the SQL namespaces
//!   because they sort after `TAG_INDEX`.
//! * Table (and index) names are **length-prefixed** with a big-endian `u32`.
//!   Without the length prefix a table named `t` and a table named `t2` would
//!   share an ambiguous prefix; the explicit length makes each table's range a
//!   clean, self-delimiting prefix regardless of the bytes a sibling name
//!   happens to contain.
//! * Primary-key and indexed-value suffixes use GlueSQL's own
//!   [`Key::to_cmp_be_bytes`](gluesql_core::data::Key::to_cmp_be_bytes) — a
//!   big-endian, order-preserving encoding whose byte comparison matches
//!   `Key`'s [`Ord`]. Because every segment in front of the suffix is fixed for
//!   a given (tenant, table[, index]), byte-ordering the full key is equivalent
//!   to ordering by that suffix. That is what makes `ORDER BY <pk>` (and
//!   ordered index scans) fall straight out of a `scan` with no in-memory sort.
//!
//! # Index entry keys and the indexed-value segment
//!
//! An index entry key is `index_prefix <value> <pk>`: the order-preserving
//! indexed value followed by the row's primary key, so a byte-ordered range
//! scan yields rows in `(value, pk)` order and an `Eq` scan is the prefix
//! `index_prefix <value>`.
//!
//! For that to be correct the `<value>` segment must be **both order-preserving
//! and prefix-free**, but `Key::to_cmp_be_bytes` is *not* prefix-free for
//! variable-width types: `Key::Str("a")` encodes as `[0x00, b'a']`, a strict
//! prefix of `Key::Str("ab")`'s `[0x00, b'a', b'b']`. Concatenating the raw
//! bytes therefore (a) lets an `Eq` prefix scan on `"a"` bleed into `"ab"`'s
//! entries and (b) breaks `(value, pk)` ordering, because `"a"`'s pk bytes
//! would be compared against `"ab"`'s third value byte. A *length prefix*
//! (the previous approach) is prefix-free but destroys order: the length sorts
//! ahead of the content, so `"z"` (len 1) wrongly sorts before `"aa"` (len 2).
//!
//! So the value segment is wrapped by [`push_order_preserving`]: every `0x00`
//! byte in `to_cmp_be_bytes()` is escaped to `0x00 0xFF` and the segment is
//! terminated with `0x00 0x00`. This is the classic order-preserving,
//! self-terminating byte-string encoding:
//!
//! * **Order-preserving** — the `0x00 0x00` terminator is less than the escape
//!   of a real `0x00` (`0x00 0xFF`) and less than any non-zero content byte, so
//!   a value that is a prefix of another still sorts before it (`"a" < "ab"`)
//!   and unequal first bytes still decide the comparison (`"aa" < "z"`).
//! * **Prefix-free / self-terminating** — `0x00 0x00` never occurs inside an
//!   escaped body, so no encoded value is a prefix of another and the boundary
//!   between the value and the trailing pk is unambiguous. An `Eq` prefix scan
//!   on `"a"` can never match `"ab"`.
//!
//! # Range scanning
//!
//! [`Keyspace::data_prefix`] returns `<tenant> [TAG_DATA] <len> <table>`; every
//! row of that table has a key with exactly this prefix, and no other key does.
//! To scan the table we walk the half-open range `[prefix, prefix_upper_bound)`
//! where the upper bound is the prefix with its last byte incremented (see
//! [`prefix_upper_bound`]).

use gluesql_core::data::Key;

use crate::error::SqlError;

/// Tag byte for schema keys. Sorts before all other tags.
const TAG_SCHEMA: u8 = 0x01;
/// Tag byte for row-data keys.
const TAG_DATA: u8 = 0x02;
/// Tag byte for secondary-index *entries* (one per indexed row).
const TAG_INDEX: u8 = 0x03;

/// Tag floor for namespaces owned by layers *above* bluedb-sql (e.g.
/// `bluedb-ledger`). bluedb-sql's own tags (`TAG_SCHEMA`/`TAG_DATA`/`TAG_INDEX`)
/// stay below this, so an external namespace can never collide with a SQL one
/// inside a shared tenant keyspace.
pub const TAG_EXTERNAL_BASE: u8 = 0x10;

/// The default tenant used by [`SlateDbStorage::new`](crate::SlateDbStorage::new).
///
/// A single underscore keeps the encoded prefix tiny while still being a valid,
/// non-empty namespace (SlateDB rejects empty keys).
pub const DEFAULT_TENANT: &str = "_";

/// Tenant-aware key builder.
///
/// Holds a tenant's already-encoded prefix (`<len::u32-be> <tenant_utf8>`) so
/// the per-key helpers don't recompute it. Construct via [`Keyspace::new`].
#[derive(Clone, Debug)]
pub struct Keyspace {
    /// `<len(tenant)::u32-be> <tenant_utf8>` — prepended to every key.
    tenant_prefix: Vec<u8>,
}

impl Keyspace {
    /// Build a keyspace for `tenant`.
    pub fn new(tenant: &str) -> Self {
        let name = tenant.as_bytes();
        let mut tenant_prefix = Vec::with_capacity(4 + name.len());
        tenant_prefix.extend_from_slice(&(name.len() as u32).to_be_bytes());
        tenant_prefix.extend_from_slice(name);
        Self { tenant_prefix }
    }

    /// Start a fresh key with the tenant prefix and a tag byte.
    fn tagged(&self, tag: u8, extra: usize) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.tenant_prefix.len() + 1 + extra);
        key.extend_from_slice(&self.tenant_prefix);
        key.push(tag);
        key
    }

    /// Append a `<len::u32-be> <bytes>` segment.
    ///
    /// A fixed-width length prefix self-delimits an *interior* segment whose
    /// raw bytes might otherwise collide with a sibling (tenant, table and index
    /// names). It is **not** order-preserving — never use it for a segment that
    /// must sort by content; see [`Self::push_order_preserving`].
    fn push_len_prefixed(key: &mut Vec<u8>, bytes: &[u8]) {
        key.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        key.extend_from_slice(bytes);
    }

    /// Append an order-preserving, self-terminating encoding of `bytes`.
    ///
    /// Each `0x00` byte is escaped to `0x00 0xFF` and the segment is terminated
    /// with `0x00 0x00`. The result is both order-preserving (lexicographic byte
    /// order matches the order of the unescaped `bytes`) and prefix-free (no
    /// encoded segment is a prefix of another), which is what an indexed value
    /// followed by a primary key needs. See the module docs for why.
    fn push_order_preserving(key: &mut Vec<u8>, bytes: &[u8]) {
        for &b in bytes {
            if b == 0x00 {
                key.push(0x00);
                key.push(0xFF);
            } else {
                key.push(b);
            }
        }
        key.push(0x00);
        key.push(0x00);
    }

    /// Encode the storage key for a table's schema record.
    pub fn schema_key(&self, table_name: &str) -> Vec<u8> {
        let name = table_name.as_bytes();
        let mut key = self.tagged(TAG_SCHEMA, name.len());
        key.extend_from_slice(name);
        key
    }

    /// The prefix shared by every schema key in this tenant.
    pub fn schema_prefix(&self) -> Vec<u8> {
        self.tagged(TAG_SCHEMA, 0)
    }

    /// The shared prefix of every row key for `table_name`:
    /// `<tenant> [TAG_DATA] <len(table)::u32-be> <table_utf8>`.
    pub fn data_prefix(&self, table_name: &str) -> Vec<u8> {
        let name = table_name.as_bytes();
        let mut prefix = self.tagged(TAG_DATA, 4 + name.len());
        Self::push_len_prefixed(&mut prefix, name);
        prefix
    }

    /// Encode the full storage key for one row: the table prefix followed by the
    /// primary key's order-preserving big-endian bytes.
    pub fn row_key(&self, table_name: &str, key: &Key) -> Result<Vec<u8>, SqlError> {
        let pk = key
            .to_cmp_be_bytes()
            .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
        let mut full = self.data_prefix(table_name);
        full.extend_from_slice(&pk);
        Ok(full)
    }

    /// The shared prefix of every index *entry* for `(table, index)`:
    /// `<tenant> [TAG_INDEX] <len(table)> <table> <len(index)> <index>`.
    pub fn index_prefix(&self, table_name: &str, index_name: &str) -> Vec<u8> {
        let table = table_name.as_bytes();
        let index = index_name.as_bytes();
        let mut key = self.tagged(TAG_INDEX, 8 + table.len() + index.len());
        Self::push_len_prefixed(&mut key, table);
        Self::push_len_prefixed(&mut key, index);
        key
    }

    /// Encode a full index-entry key: `index_prefix` followed by the
    /// order-preserving indexed value and then the row's primary key (so an
    /// indexed value with several rows keeps a stable, pk-ordered sub-order).
    pub fn index_entry_key(
        &self,
        table_name: &str,
        index_name: &str,
        value: &Key,
        pk: &Key,
    ) -> Result<Vec<u8>, SqlError> {
        let value_bytes = value
            .to_cmp_be_bytes()
            .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
        let pk_bytes = pk
            .to_cmp_be_bytes()
            .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
        let mut key = self.index_prefix(table_name, index_name);
        // Order-preserving + self-terminating value segment, so the full key
        // sorts by (value, pk) AND the pk suffix is unambiguous even for
        // variable-width encoded values (e.g. strings). A length prefix would
        // be unambiguous too, but would sort by length before content.
        Self::push_order_preserving(&mut key, &value_bytes);
        key.extend_from_slice(&pk_bytes);
        Ok(key)
    }

    /// The prefix selecting all index entries for one indexed `value`:
    /// `index_prefix <value-terminated>`. Because the value segment is
    /// prefix-free, every entry for exactly `value` — and no entry for any
    /// other value — starts with this prefix, so it cleanly bounds an `Eq`
    /// scan via [`prefix_upper_bound`].
    pub fn index_value_prefix(
        &self,
        table_name: &str,
        index_name: &str,
        value: &Key,
    ) -> Result<Vec<u8>, SqlError> {
        let value_bytes = value
            .to_cmp_be_bytes()
            .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
        let mut key = self.index_prefix(table_name, index_name);
        Self::push_order_preserving(&mut key, &value_bytes);
        Ok(key)
    }

    /// A key in an **external** namespace: `<tenant> <tag> <suffix>`. The caller
    /// owns the `suffix` encoding (e.g. a `u128` big-endian id). `tag` must be
    /// `>= TAG_EXTERNAL_BASE` so it cannot collide with bluedb-sql's own
    /// namespaces; this is asserted.
    pub fn external_key(&self, tag: u8, suffix: &[u8]) -> Vec<u8> {
        assert!(tag >= TAG_EXTERNAL_BASE, "external tag must be >= TAG_EXTERNAL_BASE");
        let mut key = self.tagged(tag, suffix.len());
        key.extend_from_slice(suffix);
        key
    }

    /// The shared prefix of every key in external namespace `tag`
    /// (`<tenant> <tag>`), for range scans. `tag` must be `>= TAG_EXTERNAL_BASE`.
    pub fn external_prefix(&self, tag: u8) -> Vec<u8> {
        assert!(tag >= TAG_EXTERNAL_BASE, "external tag must be >= TAG_EXTERNAL_BASE");
        self.tagged(tag, 0)
    }
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

    fn ks() -> Keyspace {
        Keyspace::new(DEFAULT_TENANT)
    }

    #[test]
    fn schema_and_data_namespaces_are_disjoint_and_ordered() {
        let ks = ks();
        // Within a tenant, schema keys sort before all data keys, which sort
        // before index-entry keys.
        let s = ks.schema_key("t");
        let d = ks.data_prefix("t");
        let ient = ks.index_prefix("t", "i");
        assert!(s < d);
        assert!(d < ient);
    }

    #[test]
    fn length_prefix_disambiguates_sibling_tables() {
        let ks = ks();
        // Without the length prefix, "t" rows would be a prefix of "t2" rows.
        let t = ks.data_prefix("t");
        let t2 = ks.data_prefix("t2");
        assert!(!t2.starts_with(&t));
        let t_row = ks.row_key("t", &Key::I64(99)).unwrap();
        let t2_prefix = ks.data_prefix("t2");
        assert!(!t_row.starts_with(&t2_prefix));
    }

    #[test]
    fn row_keys_sort_by_key_order() {
        let ks = ks();
        // Integer keys: byte order of row keys must match numeric order,
        // including across the sign boundary.
        let mut keys: Vec<Key> = vec![Key::I64(100), Key::I64(-5), Key::I64(0), Key::I64(7)];
        let mut encoded: Vec<Vec<u8>> = keys.iter().map(|k| ks.row_key("t", k).unwrap()).collect();
        encoded.sort();
        keys.sort();
        let resorted: Vec<Vec<u8>> = keys.iter().map(|k| ks.row_key("t", k).unwrap()).collect();
        assert_eq!(encoded, resorted);
    }

    #[test]
    fn tenants_never_overlap() {
        let a = Keyspace::new("alice");
        let b = Keyspace::new("bob");
        // Same table name, same pk, but the encoded keys differ and neither is
        // a prefix of the other.
        let ka = a.row_key("t", &Key::I64(1)).unwrap();
        let kb = b.row_key("t", &Key::I64(1)).unwrap();
        assert_ne!(ka, kb);
        assert!(!ka.starts_with(&b.data_prefix("t")));
        assert!(!kb.starts_with(&a.data_prefix("t")));
        // A tenant's data scan range cannot contain another tenant's keys.
        let a_prefix = a.data_prefix("t");
        assert!(!kb.starts_with(&a_prefix));
    }

    #[test]
    fn index_entries_sort_by_value_then_pk() {
        let ks = ks();
        // Same indexed value, different pks → ordered by pk.
        let e1 = ks
            .index_entry_key("t", "i", &Key::I64(5), &Key::I64(1))
            .unwrap();
        let e2 = ks
            .index_entry_key("t", "i", &Key::I64(5), &Key::I64(2))
            .unwrap();
        assert!(e1 < e2);
        // Different indexed values → ordered by value first.
        let e3 = ks
            .index_entry_key("t", "i", &Key::I64(6), &Key::I64(1))
            .unwrap();
        assert!(e2 < e3);
        // Every entry for value 5 falls under the value prefix.
        let vp = ks.index_value_prefix("t", "i", &Key::I64(5)).unwrap();
        assert!(e1.starts_with(&vp));
        assert!(e2.starts_with(&vp));
        assert!(!e3.starts_with(&vp));
    }

    #[test]
    fn string_index_entries_sort_by_value_then_pk_across_lengths() {
        let ks = ks();
        // Strings of different lengths exercise the order-preservation that the
        // old length-prefix encoding broke: a 1-byte value must NOT sort ahead
        // of every 2-byte value. Expected value order: "a" < "aa" < "b" < "z"
        // < "zzz".
        let values = ["a", "aa", "b", "z", "zzz"];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                ks.index_entry_key("t", "i", &Key::Str((*v).to_owned()), &Key::I64(1))
                    .unwrap()
            })
            .collect();
        // Already built in the expected value order — assert each is < the next.
        for pair in encoded.windows(2) {
            assert!(pair[0] < pair[1], "index entries not in value order");
        }
        // Sorting a scrambled copy yields the same order (round-trip proof).
        let mut scrambled = encoded.clone();
        scrambled.reverse();
        scrambled.sort();
        assert_eq!(scrambled, encoded);
    }

    #[test]
    fn string_value_prefix_is_prefix_free() {
        let ks = ks();
        // "a"'s cmp-bytes are a prefix of "ab"'s, so a naive concat would let an
        // Eq scan on "a" bleed into "ab". The escaped, terminated encoding must
        // prevent that: an "ab" entry must NOT start with "a"'s value prefix,
        // and vice versa.
        let a_prefix = ks
            .index_value_prefix("t", "i", &Key::Str("a".to_owned()))
            .unwrap();
        let ab_prefix = ks
            .index_value_prefix("t", "i", &Key::Str("ab".to_owned()))
            .unwrap();
        let ab_entry = ks
            .index_entry_key("t", "i", &Key::Str("ab".to_owned()), &Key::I64(1))
            .unwrap();
        let a_entry = ks
            .index_entry_key("t", "i", &Key::Str("a".to_owned()), &Key::I64(1))
            .unwrap();
        assert!(!ab_prefix.starts_with(&a_prefix));
        assert!(!ab_entry.starts_with(&a_prefix));
        assert!(a_entry.starts_with(&a_prefix));
        // And the half-open Eq range for "a" excludes the "ab" entry.
        let a_end = prefix_upper_bound(&a_prefix).unwrap();
        assert!(a_entry >= a_prefix && a_entry < a_end);
        assert!(!(ab_entry >= a_prefix && ab_entry < a_end));
    }

    #[test]
    fn order_preserving_escape_handles_embedded_zeros() {
        let ks = ks();
        // Byte strings containing 0x00 must still sort correctly and stay
        // prefix-free: [0x00] vs [0x00, 0x00] vs [0x01].
        let v0 = ks
            .index_entry_key("t", "i", &Key::Bytea(vec![0x00]), &Key::I64(1))
            .unwrap();
        let v00 = ks
            .index_entry_key("t", "i", &Key::Bytea(vec![0x00, 0x00]), &Key::I64(1))
            .unwrap();
        let v1 = ks
            .index_entry_key("t", "i", &Key::Bytea(vec![0x01]), &Key::I64(1))
            .unwrap();
        assert!(v0 < v00);
        assert!(v00 < v1);
        // [0x00] is a byte-prefix of [0x00,0x00]; the value prefix must not be.
        let p0 = ks
            .index_value_prefix("t", "i", &Key::Bytea(vec![0x00]))
            .unwrap();
        let p00 = ks
            .index_value_prefix("t", "i", &Key::Bytea(vec![0x00, 0x00]))
            .unwrap();
        assert!(!p00.starts_with(&p0));
    }

    #[test]
    fn prefix_upper_bound_basics() {
        assert_eq!(prefix_upper_bound(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
        assert_eq!(prefix_upper_bound(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_upper_bound(&[]), None);
    }

    #[test]
    fn external_namespace_is_disjoint_and_ordered() {
        let ks = ks();
        // External tags (>= 0x10) sort after sql's own namespaces and don't
        // collide with each other.
        let acct = ks.external_key(TAG_EXTERNAL_BASE, &7u128.to_be_bytes());
        let xfer = ks.external_key(TAG_EXTERNAL_BASE + 1, &7u128.to_be_bytes());
        let data = ks.data_prefix("t");
        assert!(data < acct, "sql data namespace sorts before external tags");
        assert!(acct < xfer, "external tag 0x10 sorts before 0x11");
        // Every account key starts with the account prefix; no transfer key does.
        let acct_prefix = ks.external_prefix(TAG_EXTERNAL_BASE);
        assert!(acct.starts_with(&acct_prefix));
        assert!(!xfer.starts_with(&acct_prefix));
    }

    #[test]
    fn external_keys_sort_by_u128_suffix() {
        let ks = ks();
        let a = ks.external_key(TAG_EXTERNAL_BASE, &1u128.to_be_bytes());
        let b = ks.external_key(TAG_EXTERNAL_BASE, &2u128.to_be_bytes());
        let big = ks.external_key(TAG_EXTERNAL_BASE, &u128::MAX.to_be_bytes());
        assert!(a < b && b < big, "big-endian u128 suffixes sort numerically");
    }
}
