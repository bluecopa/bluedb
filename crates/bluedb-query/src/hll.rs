//! Mergeable HyperLogLog sketches — approximate distinct-count that can be
//! **stored and unioned later** (incremental / cross-table rollups), unlike a
//! one-shot `approx_count_distinct`. Internal v1 format, dependency-free.
//!
//! * `hll_build(expr)` (aggregate) → a sketch as `BYTEA`.
//! * `hll_merge(sketch)` (aggregate) → the union of sketches as `BYTEA`.
//! * `hll_count(sketch)` (scalar) → the estimated distinct count (`BIGINT`).
//!
//! Sketch bytes: `[version, p, registers…]` (`2 + 2^p` bytes). Values are hashed
//! by their text rendering with a fixed FNV-1a + splitmix64 finalizer, so a sketch
//! is deterministic and mergeable across processes and runs. `p = 14` gives 16384
//! registers (~0.81% standard error).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Int64Builder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

const HLL_VERSION: u8 = 1;
const HLL_P: u8 = 14;
const HLL_M: usize = 1 << HLL_P;

/// Register `hll_build`, `hll_merge`, `hll_count` on `ctx`.
pub fn register(ctx: &mut SessionContext) -> DfResult<()> {
    ctx.register_udaf(Arc::new(AggregateUDF::new_from_impl(HllAgg::build())))?;
    ctx.register_udaf(Arc::new(AggregateUDF::new_from_impl(HllAgg::merge())))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(HllCount::new())))?;
    Ok(())
}

// --- the sketch -------------------------------------------------------------

#[derive(Debug)]
struct Hll {
    regs: Vec<u8>,
}

impl Hll {
    fn new() -> Self {
        Self {
            regs: vec![0u8; HLL_M],
        }
    }

    fn add_str(&mut self, s: &str) {
        let h = hash64(s.as_bytes());
        let idx = (h >> (64 - HLL_P as u64)) as usize; // top p bits
        let rank = ((h << HLL_P).leading_zeros() as u8)
            .saturating_add(1)
            .min(64 - HLL_P + 1);
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }

    fn merge(&mut self, other: &Hll) {
        for (a, b) in self.regs.iter_mut().zip(other.regs.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    fn estimate(&self) -> u64 {
        let m = HLL_M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.regs.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let mut e = alpha * m * m / sum;
        // Small-range correction (linear counting) — exact for tiny cardinalities.
        if e <= 2.5 * m {
            let zeros = self.regs.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                e = m * (m / zeros as f64).ln();
            }
        }
        e.round() as u64
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(2 + HLL_M);
        v.push(HLL_VERSION);
        v.push(HLL_P);
        v.extend_from_slice(&self.regs);
        v
    }

    /// Parse a sketch; `None` if the bytes aren't a v1/p-matching sketch.
    fn from_bytes(b: &[u8]) -> Option<Hll> {
        if b.len() != 2 + HLL_M || b[0] != HLL_VERSION || b[1] != HLL_P {
            return None;
        }
        Some(Hll {
            regs: b[2..].to_vec(),
        })
    }
}

/// Deterministic 64-bit hash: FNV-1a then a splitmix64 finalizer for avalanche.
fn hash64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

// --- aggregates: hll_build / hll_merge --------------------------------------

/// One impl for both aggregates; `build` hashes raw values, `merge` unions
/// incoming sketches. Both emit a sketch and merge partial-aggregation states the
/// same way (union of sketches), so only the input handling differs.
#[derive(Debug, PartialEq, Eq, Hash)]
struct HllAgg {
    name: &'static str,
    signature: Signature,
}
impl HllAgg {
    fn build() -> Self {
        Self {
            name: "hll_build",
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
    fn merge() -> Self {
        Self {
            name: "hll_merge",
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}
impl AggregateUDFImpl for HllAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Binary)
    }
    fn accumulator(&self, _: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(HllAccumulator {
            hll: Hll::new(),
            merges_sketches: self.name == "hll_merge",
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_sketch", args.name),
            DataType::Binary,
            true,
        ))])
    }
}

#[derive(Debug)]
struct HllAccumulator {
    hll: Hll,
    /// `true` for `hll_merge` (inputs are sketches), `false` for `hll_build`
    /// (inputs are raw values to hash).
    merges_sketches: bool,
}

impl HllAccumulator {
    /// Union every non-null serialized sketch in `arr` into `self`.
    fn absorb_sketches(&mut self, arr: &ArrayRef) -> DfResult<()> {
        let b = cast(arr, &DataType::Binary)?;
        let b = b.as_any().downcast_ref::<BinaryArray>().unwrap();
        for row in 0..b.len() {
            if !b.is_null(row) {
                if let Some(other) = Hll::from_bytes(b.value(row)) {
                    self.hll.merge(&other);
                }
            }
        }
        Ok(())
    }
}

impl Accumulator for HllAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        if self.merges_sketches {
            return self.absorb_sketches(&values[0]);
        }
        let s = cast(&values[0], &DataType::Utf8)?;
        let s = s.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..s.len() {
            if !s.is_null(row) {
                self.hll.add_str(s.value(row));
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        // Partial-aggregation states are always serialized sketches.
        self.absorb_sketches(&states[0])
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.hll.to_bytes()))])
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.hll.to_bytes())))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.hll.regs.capacity()
    }
}

// --- scalar: hll_count ------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct HllCount {
    signature: Signature,
}
impl HllCount {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for HllCount {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hll_count"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let sketches = cast(&arrays[0], &DataType::Binary)?;
        let sketches = sketches.as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut out = Int64Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            match (!sketches.is_null(row))
                .then(|| Hll::from_bytes(sketches.value(row)))
                .flatten()
            {
                Some(h) => out.append_value(h.estimate() as i64),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int64Type;

    async fn one_i64(sql: &str) -> i64 {
        let ctx = crate::analytical_context().unwrap();
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        b[0].column(0).as_primitive::<Int64Type>().value(0)
    }

    #[test]
    fn sketch_round_trips_and_estimates_small_exactly() {
        let mut h = super::Hll::new();
        for s in ["a", "b", "a", "c", "b"] {
            h.add_str(s);
        }
        // linear counting is exact at this scale
        assert_eq!(h.estimate(), 3);
        // serialize → parse → same estimate
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), 2 + super::HLL_M);
        assert_eq!(super::Hll::from_bytes(&bytes).unwrap().estimate(), 3);
        assert!(super::Hll::from_bytes(b"garbage").is_none());
    }

    #[test]
    fn merge_is_union() {
        let mut a = super::Hll::new();
        ["a", "b"].iter().for_each(|s| a.add_str(s));
        let mut b = super::Hll::new();
        ["b", "c", "d"].iter().for_each(|s| b.add_str(s));
        a.merge(&b);
        assert_eq!(a.estimate(), 4); // {a,b,c,d}
    }

    #[tokio::test]
    async fn build_count_via_sql() {
        let n =
            one_i64("SELECT hll_count(hll_build(c)) FROM (VALUES ('a'),('b'),('a'),('c')) t(c)")
                .await;
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn build_then_merge_rolls_up() {
        // Per-group sketches, then union them: distinct of {a,b,c} across groups = 3.
        let sql = "SELECT hll_count(hll_merge(s)) FROM \
                   (SELECT g, hll_build(c) AS s \
                    FROM (VALUES (1,'a'),(1,'b'),(2,'b'),(2,'c')) t(g,c) GROUP BY g)";
        assert_eq!(one_i64(sql).await, 3);
    }
}
