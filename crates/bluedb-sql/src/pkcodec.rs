//! Composite-primary-key encoding.
//!
//! gluesql's [`Key`](gluesql_core::data::Key) is scalar, so a composite primary
//! key `(a, b, …)` is mapped to a single hidden `__bluedb_pk BYTEA` column whose
//! bytes are an **order-preserving, self-terminating concatenation** of the
//! component keys:
//!
//! ```text
//! __bluedb_pk := concat_i  escape( component_i.to_cmp_be_bytes() ) 0x00 0x00
//! ```
//!
//! Each component is wrapped exactly like an index-entry value in
//! [`crate::keyspace`]: every `0x00` is escaped to `0x00 0xFF` and the segment is
//! terminated with `0x00 0x00`. Because each component's `to_cmp_be_bytes()` is
//! itself order-preserving, and the escape keeps that order while making the
//! segment prefix-free, the byte order of the whole `__bluedb_pk` equals the
//! lexicographic tuple order of `(a, b, …)` — the same comparison a Postgres
//! multicolumn B-tree gives — and component boundaries are unambiguous, so a
//! prefix predicate on the leading components is exact (`a = 5` never bleeds into
//! `a = 50`, even for variable-length `a`). This is the FoundationDB-tuple /
//! CockroachDB key-encoding pattern.

use gluesql_core::data::Key;

use crate::error::SqlError;

/// Append an order-preserving, self-terminating encoding of `bytes`: every
/// `0x00` escaped to `0x00 0xFF`, then a `0x00 0x00` terminator. (Same scheme as
/// `Keyspace::push_order_preserving`, kept local so the codec is self-contained.)
fn push_order_preserving(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Encode an ordered list of primary-key component values into the opaque
/// `__bluedb_pk` byte string. Byte order of the result matches lexicographic
/// tuple order of `components`.
///
/// Every component is escaped+terminated — including the last — so the boundary
/// after each is unambiguous (unlike a row's single trailing PK, which needs no
/// terminator because nothing follows it).
// Wired into the INSERT/predicate rewrite in Phase 2; proven standalone here.
#[allow(dead_code)]
pub(crate) fn encode_composite_key(components: &[Key]) -> Result<Vec<u8>, SqlError> {
    let mut out = Vec::new();
    for component in components {
        let bytes = component
            .to_cmp_be_bytes()
            .map_err(|err| SqlError::KeyEncode(err.to_string()))?;
        push_order_preserving(&mut out, &bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(components: &[Key]) -> Vec<u8> {
        encode_composite_key(components).unwrap()
    }

    #[test]
    fn orders_by_leading_then_trailing_component() {
        // (1,2) < (1,3) < (2,1) — the multicolumn-B-tree order.
        let k12 = enc(&[Key::I32(1), Key::I32(2)]);
        let k13 = enc(&[Key::I32(1), Key::I32(3)]);
        let k21 = enc(&[Key::I32(2), Key::I32(1)]);
        assert!(k12 < k13, "trailing component breaks the tie");
        assert!(k13 < k21, "leading component dominates");
    }

    #[test]
    fn variable_length_leading_component_is_prefix_free() {
        // The classic concatenation hazard: ("a","z") vs ("ab","a"). With raw
        // concatenation "a"+"z" and "ab"+"a" could interleave; the terminator
        // makes "a" < "ab" decide it, so ("a", _) always sorts before ("ab", _).
        let a_z = enc(&[Key::Str("a".into()), Key::Str("z".into())]);
        let ab_a = enc(&[Key::Str("ab".into()), Key::Str("a".into())]);
        assert!(a_z < ab_a, "shorter leading string sorts first regardless of trailing");
    }

    #[test]
    fn no_cross_boundary_collision() {
        // ("ab","c") and ("a","bc") must NOT encode to the same bytes, and must
        // order by the first differing component: "a" < "ab".
        let ab_c = enc(&[Key::Str("ab".into()), Key::Str("c".into())]);
        let a_bc = enc(&[Key::Str("a".into()), Key::Str("bc".into())]);
        assert_ne!(ab_c, a_bc, "boundaries must be unambiguous");
        assert!(a_bc < ab_c);
    }

    #[test]
    fn trailing_component_orders_within_equal_leading() {
        let a_z = enc(&[Key::Str("a".into()), Key::Str("z".into())]);
        let a_za = enc(&[Key::Str("a".into()), Key::Str("za".into())]);
        assert!(a_z < a_za, "equal leading, trailing decides");
    }

    #[test]
    fn mixed_component_types_order_per_component() {
        // (int, text): (1,"b") < (1,"c") < (2,"a")
        let one_b = enc(&[Key::I64(1), Key::Str("b".into())]);
        let one_c = enc(&[Key::I64(1), Key::Str("c".into())]);
        let two_a = enc(&[Key::I64(2), Key::Str("a".into())]);
        assert!(one_b < one_c);
        assert!(one_c < two_a);
    }

    #[test]
    fn embedded_zero_byte_is_escaped() {
        // A component whose bytes contain 0x00 must still terminate cleanly and
        // not collide with a shorter component.
        let with_zero = enc(&[Key::Bytea(vec![0x00]), Key::I32(1)]);
        let empty = enc(&[Key::Bytea(vec![]), Key::I32(1)]);
        assert_ne!(with_zero, empty);
        assert!(empty < with_zero, "empty bytea sorts before [0x00]");
    }
}
