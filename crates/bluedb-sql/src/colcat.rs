//! Per-table **column catalog** — the field-id mapping that makes `ALTER TABLE`
//! an O(1) metadata op instead of an O(n) row rewrite.
//!
//! Rows are stored as positional [`DataRow::Vec`], but a column's *logical*
//! position (the order GlueSQL derives from the schema) is decoupled from its
//! *physical slot* (its index in the stored row). ADD COLUMN appends a new slot;
//! DROP COLUMN forgets a slot (its value is left orphaned in old rows, ignored on
//! read); RENAME touches neither. Reads project physical → logical; writes do the
//! reverse.
//!
//! **Absent catalog == identity.** A freshly created (never-altered) table has no
//! catalog persisted: logical position equals physical slot, so reads and writes
//! pass through untouched and existing rows — and the GlueSQL conformance suite —
//! are unaffected. A catalog is written only once the first ADD/DROP makes the
//! mapping diverge from identity.

use std::collections::BTreeMap;

use gluesql_core::data::Value;
use gluesql_core::store::DataRow;
use serde::{Deserialize, Serialize};

/// Maps each current logical column (in schema order) to its stable physical slot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnCatalog {
    /// `slots[i]` = physical slot of the i-th current logical column.
    pub slots: Vec<u32>,
    /// Total physical slots ever allocated — the width of a newly written row.
    pub width: u32,
    /// Fill value for rows written *before* a slot existed (an added column's
    /// resolved default). A slot absent here fills with `Value::Null`.
    pub defaults: BTreeMap<u32, Value>,
}

impl ColumnCatalog {
    /// The identity mapping for an `n`-column table (logical i ↔ physical i).
    pub fn identity(n: usize) -> Self {
        Self {
            slots: (0..n as u32).collect(),
            width: n as u32,
            defaults: BTreeMap::new(),
        }
    }

    /// Append a column (ADD COLUMN). `default` fills rows written earlier.
    pub fn add_column(&mut self, default: Value) {
        let slot = self.width;
        self.slots.push(slot);
        self.width += 1;
        if !matches!(default, Value::Null) {
            self.defaults.insert(slot, default);
        }
    }

    /// Forget the logical column at `logical_idx` (DROP COLUMN). The physical slot
    /// stays orphaned in stored rows (ignored on read) — no row rewrite.
    pub fn drop_column(&mut self, logical_idx: usize) {
        self.slots.remove(logical_idx);
    }

    /// Physical stored row → logical row (current schema column order).
    pub fn to_logical(&self, phys: DataRow) -> DataRow {
        let DataRow::Vec(vals) = phys else {
            return phys; // map rows are schemaless-shaped; untouched
        };
        let logical = self
            .slots
            .iter()
            .map(|&slot| {
                vals.get(slot as usize)
                    .cloned()
                    .unwrap_or_else(|| self.defaults.get(&slot).cloned().unwrap_or(Value::Null))
            })
            .collect();
        DataRow::Vec(logical)
    }

    /// Logical row (current schema order) → physical stored row.
    pub fn to_physical(&self, logical: DataRow) -> DataRow {
        let DataRow::Vec(vals) = logical else {
            return logical;
        };
        let mut phys = vec![Value::Null; self.width as usize];
        for (i, &slot) in self.slots.iter().enumerate() {
            if let Some(v) = vals.get(i) {
                phys[slot as usize] = v.clone();
            }
        }
        DataRow::Vec(phys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_row(vals: &[i64]) -> DataRow {
        DataRow::Vec(vals.iter().map(|v| Value::I64(*v)).collect())
    }

    #[test]
    fn identity_round_trips() {
        let cat = ColumnCatalog::identity(3);
        let row = vec_row(&[1, 2, 3]);
        assert_eq!(cat.to_physical(cat.to_logical(row.clone())), row);
    }

    #[test]
    fn add_column_pads_old_rows_with_default() {
        let mut cat = ColumnCatalog::identity(2);
        cat.add_column(Value::I64(99)); // NOT NULL default
                                        // An old row written before the add has only 2 physical values.
        let old = vec_row(&[1, 2]);
        assert_eq!(cat.to_logical(old), vec_row(&[1, 2, 99]));
    }

    #[test]
    fn add_column_nullable_pads_with_null() {
        let mut cat = ColumnCatalog::identity(2);
        cat.add_column(Value::Null);
        let old = vec_row(&[1, 2]);
        assert_eq!(
            cat.to_logical(old),
            DataRow::Vec(vec![Value::I64(1), Value::I64(2), Value::Null])
        );
    }

    #[test]
    fn new_row_after_add_round_trips() {
        let mut cat = ColumnCatalog::identity(2);
        cat.add_column(Value::Null);
        let logical = vec_row(&[1, 2, 3]);
        let phys = cat.to_physical(logical.clone());
        assert_eq!(phys, vec_row(&[1, 2, 3])); // width 3, slots [0,1,2]
        assert_eq!(cat.to_logical(phys), logical);
    }

    #[test]
    fn drop_column_projects_out_the_slot() {
        // Start with 3 columns, drop the middle one.
        let mut cat = ColumnCatalog::identity(3);
        cat.drop_column(1);
        // Old rows still physically carry 3 values; logical now skips slot 1.
        let old = vec_row(&[10, 20, 30]);
        assert_eq!(cat.to_logical(old), vec_row(&[10, 30]));
        // A new write of the 2 logical columns lands in slots 0 and 2.
        let phys = cat.to_physical(vec_row(&[10, 30]));
        assert_eq!(
            phys,
            DataRow::Vec(vec![Value::I64(10), Value::Null, Value::I64(30)])
        );
        assert_eq!(cat.to_logical(phys), vec_row(&[10, 30]));
    }

    #[test]
    fn add_then_drop_composes() {
        let mut cat = ColumnCatalog::identity(2); // [a,b] slots [0,1]
        cat.add_column(Value::Null); // [a,b,c] slots [0,1,2] width 3
        cat.drop_column(0); // [b,c] slots [1,2]
        let row = vec_row(&[1, 2, 3]); // physical a=1,b=2,c=3
        assert_eq!(cat.to_logical(row), vec_row(&[2, 3]));
    }
}
