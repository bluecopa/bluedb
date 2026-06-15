//! gluesql `Schema`/`ColumnDef` → Iceberg `Schema`: scalar + complex/nested type mapping
//! (spec §7). Complex (`List`/`Map`) handling lands in Task 2.4.

use gluesql_core::ast::DataType;
use iceberg::spec::PrimitiveType;

use crate::{LakehouseError, Result};

/// Map a gluesql scalar [`DataType`] to an Iceberg [`PrimitiveType`] (spec §7.1).
///
/// `Map`/`List` are **not** primitives — they map to Iceberg nested `list`/`map` types in the
/// complex-type path (Task 2.4); calling this on them returns an error.
///
/// Notes on the lossy/guarded cases:
/// - `Int128`/`Uint64`/`Uint128` exceed Iceberg `Long`, so they map to `decimal`; `Int128`/
///   `Uint128` use `decimal(38,0)` and are range-guarded at write time (full 128-bit max is 39
///   digits; real values — money — never reach 10^38).
/// - gluesql `Decimal` is variable-scale (`rust_decimal`, ≤28 digits) but Iceberg needs a
///   fixed `(precision, scale)`; we default to `decimal(38,18)` (documented limitation).
/// - `Inet`/`Interval`/`Point` have no native Iceberg type → stored as `string`.
pub fn iceberg_primitive(dt: &DataType) -> Result<PrimitiveType> {
    Ok(match dt {
        DataType::Boolean => PrimitiveType::Boolean,
        // signed ints: ≤ i32 → Int(32); i64 → Long
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int => PrimitiveType::Long,
        // 128-bit (and u64) exceed Long → decimal
        DataType::Int128 | DataType::Uint128 => PrimitiveType::Decimal {
            precision: 38,
            scale: 0,
        },
        // unsigned: u8/u16 fit Int(32); u32 needs Long; u64 needs decimal(20,0)
        DataType::Uint8 | DataType::Uint16 => PrimitiveType::Int,
        DataType::Uint32 => PrimitiveType::Long,
        DataType::Uint64 => PrimitiveType::Decimal {
            precision: 20,
            scale: 0,
        },
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float => PrimitiveType::Double,
        DataType::Text => PrimitiveType::String,
        DataType::Bytea => PrimitiveType::Binary,
        DataType::Date => PrimitiveType::Date,
        DataType::Time => PrimitiveType::Time,
        DataType::Timestamp => PrimitiveType::Timestamp,
        DataType::Uuid => PrimitiveType::Uuid,
        DataType::Decimal => PrimitiveType::Decimal {
            precision: 38,
            scale: 18,
        },
        DataType::Inet | DataType::Interval | DataType::Point => PrimitiveType::String,
        DataType::Map | DataType::List => {
            return Err(LakehouseError::Schema(format!(
                "{dt:?} is a complex type; use the complex-type mapping, not iceberg_primitive"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_scalar_types() {
        assert_eq!(iceberg_primitive(&DataType::Int).unwrap(), PrimitiveType::Long);
        assert_eq!(iceberg_primitive(&DataType::Int32).unwrap(), PrimitiveType::Int);
        assert_eq!(iceberg_primitive(&DataType::Text).unwrap(), PrimitiveType::String);
        assert_eq!(
            iceberg_primitive(&DataType::Boolean).unwrap(),
            PrimitiveType::Boolean
        );
        assert_eq!(
            iceberg_primitive(&DataType::Float).unwrap(),
            PrimitiveType::Double
        );
        assert_eq!(
            iceberg_primitive(&DataType::Uint32).unwrap(),
            PrimitiveType::Long
        );
        assert!(matches!(
            iceberg_primitive(&DataType::Uint64).unwrap(),
            PrimitiveType::Decimal {
                precision: 20,
                scale: 0
            }
        ));
        assert!(matches!(
            iceberg_primitive(&DataType::Uint128).unwrap(),
            PrimitiveType::Decimal {
                precision: 38,
                scale: 0
            }
        ));
    }

    #[test]
    fn complex_types_are_not_primitives() {
        assert!(iceberg_primitive(&DataType::List).is_err());
        assert!(iceberg_primitive(&DataType::Map).is_err());
    }
}
