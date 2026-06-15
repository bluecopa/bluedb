//! gluesql `Schema`/`ColumnDef` → Iceberg `Schema`: scalar + complex/nested type mapping
//! (spec §7).
//!
//! gluesql's `List`/`Map` are **untyped** (unit variants — the element/value types are not in
//! the column definition), so for complex columns we infer the inner type from sample values at
//! seal time: homogeneous scalar → that primitive, nested list/map → recurse, empty or
//! heterogeneous → `string` (the value is JSON-encoded at write).

use gluesql_core::ast::DataType;
use gluesql_core::data::Value;
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Type};

use crate::{LakehouseError, Result};

/// Map a gluesql scalar [`DataType`] to an Iceberg [`PrimitiveType`] (spec §7.1).
///
/// `Map`/`List` are **not** primitives — they map to Iceberg nested `list`/`map` types via
/// [`iceberg_type`]; calling this on them returns an error.
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
                "{dt:?} is a complex type; use iceberg_type, not iceberg_primitive"
            )));
        }
    })
}

/// Map any gluesql [`DataType`] — scalar **or** complex — to an Iceberg [`Type`] (spec §7).
///
/// Scalars delegate to [`iceberg_primitive`]. For `List`/`Map`, the element/value type is
/// inferred from `sample_values` (the column's sampled cell values for this seal batch); see the
/// module docs for the inference rules. Nested fields receive monotonic Iceberg field-ids drawn
/// from `field_ids` (the next free id in the table schema), parent-before-child.
pub fn iceberg_type(
    col_type: &DataType,
    sample_values: &[&Value],
    field_ids: &mut i32,
) -> Result<Type> {
    Ok(match col_type {
        DataType::List => build_list(&list_elements(sample_values), field_ids),
        DataType::Map => build_map(&map_values(sample_values), field_ids),
        scalar => Type::Primitive(iceberg_primitive(scalar)?),
    })
}

/// Build an Iceberg `list<element>` whose element type is inferred from the flattened
/// `elements` across all sample lists. Allocates the element field-id before recursing so ids
/// run parent-before-child.
fn build_list(elements: &[&Value], ids: &mut i32) -> Type {
    let elem_id = take_id(ids);
    let elem_type = infer_type(elements, ids);
    Type::List(ListType::new(
        NestedField::list_element(elem_id, elem_type, true).into(),
    ))
}

/// Build an Iceberg `map<string, value>` whose value type is inferred from the flattened map
/// values. gluesql map keys are always `string`. Key/value field-ids precede the value's
/// nested ids.
fn build_map(values: &[&Value], ids: &mut i32) -> Type {
    let key_id = take_id(ids);
    let val_id = take_id(ids);
    let val_type = infer_type(values, ids);
    Type::Map(MapType::new(
        NestedField::map_key_element(key_id, Type::Primitive(PrimitiveType::String)).into(),
        NestedField::map_value_element(val_id, val_type, true).into(),
    ))
}

/// Infer one Iceberg [`Type`] covering all of `values` (spec §7): empty/all-null → `string`;
/// homogeneous scalar → that primitive; homogeneous nested list/map → recurse; heterogeneous →
/// `string` (JSON-encoded at write).
fn infer_type(values: &[&Value], ids: &mut i32) -> Type {
    let non_null: Vec<&Value> = values
        .iter()
        .copied()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    let Some(first) = non_null.first() else {
        return Type::Primitive(PrimitiveType::String);
    };
    // Heterogeneous (mixed variants, e.g. [int, string]) → fall back to JSON string.
    let homogeneous = non_null
        .iter()
        .all(|v| std::mem::discriminant(*v) == std::mem::discriminant(*first));
    if !homogeneous {
        return Type::Primitive(PrimitiveType::String);
    }
    match first {
        Value::List(_) => build_list(&list_elements(&non_null), ids),
        Value::Map(_) => build_map(&map_values(&non_null), ids),
        scalar => value_primitive(scalar)
            .map(Type::Primitive)
            .unwrap_or(Type::Primitive(PrimitiveType::String)),
    }
}

/// Iceberg primitive for a concrete scalar [`Value`] (the value-level analogue of
/// [`iceberg_primitive`]). Returns `None` for `Null`/`List`/`Map`, which are not scalars.
fn value_primitive(v: &Value) -> Option<PrimitiveType> {
    use PrimitiveType as P;
    Some(match v {
        Value::Bool(_) => P::Boolean,
        Value::I8(_) | Value::I16(_) | Value::I32(_) => P::Int,
        Value::I64(_) => P::Long,
        Value::I128(_) => P::Decimal {
            precision: 38,
            scale: 0,
        },
        Value::U8(_) | Value::U16(_) => P::Int,
        Value::U32(_) => P::Long,
        Value::U64(_) => P::Decimal {
            precision: 20,
            scale: 0,
        },
        Value::U128(_) => P::Decimal {
            precision: 38,
            scale: 0,
        },
        Value::F32(_) => P::Float,
        Value::F64(_) => P::Double,
        Value::Decimal(_) => P::Decimal {
            precision: 38,
            scale: 18,
        },
        Value::Str(_) => P::String,
        Value::Bytea(_) => P::Binary,
        Value::Inet(_) => P::String,
        Value::Date(_) => P::Date,
        Value::Timestamp(_) => P::Timestamp,
        Value::Time(_) => P::Time,
        Value::Interval(_) => P::String,
        Value::Uuid(_) => P::Uuid,
        Value::Point(_) => P::String,
        Value::Map(_) | Value::List(_) | Value::Null => return None,
    })
}

/// Flatten the elements of every `Value::List` in `vals` (non-lists contribute nothing).
fn list_elements<'a>(vals: &[&'a Value]) -> Vec<&'a Value> {
    vals.iter()
        .copied()
        .flat_map(|v| match v {
            Value::List(xs) => xs.iter().collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

/// Flatten the values of every `Value::Map` in `vals` (non-maps contribute nothing).
fn map_values<'a>(vals: &[&'a Value]) -> Vec<&'a Value> {
    vals.iter()
        .copied()
        .flat_map(|v| match v {
            Value::Map(m) => m.values().collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

/// Hand out the next free Iceberg field-id and advance the counter.
fn take_id(c: &mut i32) -> i32 {
    let id = *c;
    *c += 1;
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn prim(t: &Type) -> &PrimitiveType {
        match t {
            Type::Primitive(p) => p,
            other => panic!("expected primitive, got {other:?}"),
        }
    }

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

    #[test]
    fn list_column_infers_element_from_samples() {
        let mut ids = 100;
        let s1 = Value::List(vec![Value::I64(1), Value::I64(2)]);
        let s2 = Value::List(vec![Value::I64(3)]);
        let t = iceberg_type(&DataType::List, &[&s1, &s2], &mut ids).unwrap();
        let Type::List(lt) = t else { panic!("expected list") };
        assert_eq!(prim(lt.element_field.field_type.as_ref()), &PrimitiveType::Long);
        assert!(ids > 100, "nested field-id should have been allocated");
    }

    #[test]
    fn list_with_mixed_elements_infers_string() {
        let mut ids = 0;
        let s = Value::List(vec![Value::I64(1), Value::Str("x".into())]);
        let t = iceberg_type(&DataType::List, &[&s], &mut ids).unwrap();
        let Type::List(lt) = t else { panic!("expected list") };
        assert_eq!(prim(lt.element_field.field_type.as_ref()), &PrimitiveType::String);
    }

    #[test]
    fn empty_list_defaults_to_string_element() {
        let mut ids = 0;
        let t = iceberg_type(&DataType::List, &[], &mut ids).unwrap();
        let Type::List(lt) = t else { panic!("expected list") };
        assert_eq!(prim(lt.element_field.field_type.as_ref()), &PrimitiveType::String);
    }

    #[test]
    fn map_column_infers_value_type_keys_are_string() {
        let mut ids = 0;
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Value::I32(1));
        m.insert("b".to_string(), Value::I32(2));
        let s = Value::Map(m);
        let t = iceberg_type(&DataType::Map, &[&s], &mut ids).unwrap();
        let Type::Map(mt) = t else { panic!("expected map") };
        assert_eq!(prim(mt.key_field.field_type.as_ref()), &PrimitiveType::String);
        assert_eq!(prim(mt.value_field.field_type.as_ref()), &PrimitiveType::Int);
        assert_ne!(mt.key_field.id, mt.value_field.id, "field-ids must be unique");
    }

    #[test]
    fn nested_list_of_lists_recurses() {
        let mut ids = 0;
        let s = Value::List(vec![Value::List(vec![Value::I64(1)])]);
        let t = iceberg_type(&DataType::List, &[&s], &mut ids).unwrap();
        let Type::List(outer) = t else { panic!("expected outer list") };
        let inner = match outer.element_field.field_type.as_ref() {
            Type::List(inner) => inner,
            other => panic!("expected inner list, got {other:?}"),
        };
        assert_eq!(prim(inner.element_field.field_type.as_ref()), &PrimitiveType::Long);
    }
}
