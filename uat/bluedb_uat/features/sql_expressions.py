"""Generated SQL expression acceptance cases."""

from __future__ import annotations

from ._sql_scalar import ScalarRow, sql_scalar_cases
from ._legacy import cases_by_id


def cases() -> list:
    rows: list[ScalarRow] = []

    for n in range(1, 51):
        rows.append((f"Arithmetic addition preserves integer result {n}", f"{n} + {n + 3}", n + n + 3))
        rows.append((f"Arithmetic subtraction preserves integer result {n}", f"{n + 20} - {n}", 20))
        rows.append((f"Arithmetic multiplication preserves integer result {n}", f"{n} * 3", n * 3))
        rows.append((f"Arithmetic modulo preserves integer result {n}", f"{n + 20} % 7", (n + 20) % 7))

    for n in range(1, 41):
        rows.append((f"Comparison greater-than returns true {n}", f"{n + 1} > {n}", True))
        rows.append((f"Comparison less-than returns true {n}", f"{n} < {n + 1}", True))
        rows.append((f"Comparison equality returns true {n}", f"{n} = {n}", True))
        rows.append((f"Comparison inequality returns true {n}", f"{n} <> {n + 1}", True))

    for n in range(1, 31):
        rows.append((f"BETWEEN includes bounded value {n}", f"{n} BETWEEN {n - 1} AND {n + 1}", True))
        rows.append((f"IN finds listed value {n}", f"{n} IN ({n - 1}, {n}, {n + 1})", True))
        rows.append((f"NOT IN excludes absent value {n}", f"{n} NOT IN ({n + 1}, {n + 2})", True))
        rows.append((f"CASE chooses true branch {n}", f"CASE WHEN {n} > 0 THEN 'positive' ELSE 'other' END", "positive"))

    for n in range(1, 21):
        rows.append((f"CAST parses integer text {n}", f"CAST('{n}' AS INTEGER)", n))
        rows.append((f"TRY_CAST returns null for invalid integer {n}", f"TRY_CAST('not-{n}' AS INTEGER)", None))
        rows.append((f"COALESCE returns first non-null value {n}", f"COALESCE(NULL, 'value-{n}')", f"value-{n}"))
        rows.append((f"NULLIF returns null for equal values {n}", f"NULLIF('same-{n}', 'same-{n}')", None))

    return cases_by_id("UAT-SQL-003") + sql_scalar_cases(
        prefix="UAT-SQL-EXPR",
        feature="sql.expressions",
        docs=["docs/sql/expressions.md"],
        rows_in=rows,
        profiles={"full"},
    )
