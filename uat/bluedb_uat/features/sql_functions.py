"""Generated SQL function acceptance cases."""

from __future__ import annotations

from ._sql_scalar import ScalarRow, sql_scalar_cases


def cases() -> list:
    rows: list[ScalarRow] = []

    for n in range(1, 31):
        rows.extend(
            [
                (f"ABS returns magnitude {n}", f"ABS(-{n})", n),
                (f"SIGN returns negative sign {n}", f"SIGN(-{n})", -1.0),
                (f"CEIL rounds up {n}", f"CEIL({n}.2)", float(n + 1)),
                (f"FLOOR rounds down {n}", f"FLOOR({n}.8)", float(n)),
                (f"ROUND keeps two decimals {n}", f"ROUND({n}.234, 2)", float(f"{n}.23")),
                (f"SQRT returns square root {n}", f"SQRT({n * n})", float(n)),
                (f"POWER raises exponent {n}", f"POWER({n}, 2)", float(n * n)),
                (f"MOD returns remainder {n}", f"MOD({n + 20}, 7)", float((n + 20) % 7)),
                (f"GCD returns greatest divisor {n}", f"GCD({n * 6}, {n * 9})", n * 3),
                (f"LCM returns least multiple {n}", f"LCM({n * 2}, {n * 3})", n * 6),
            ]
        )

    for n in range(1, 31):
        word = f"AbC{n}"
        rows.extend(
            [
                (f"LOWER normalizes text {n}", f"LOWER('{word}')", word.lower()),
                (f"UPPER normalizes text {n}", f"UPPER('{word}')", word.upper()),
                (f"INITCAP title-cases words {n}", f"INITCAP('hello world {n}')", f"Hello World {n}"),
                (f"LENGTH counts characters {n}", f"LENGTH('abc{n}')", len(f"abc{n}")),
                (f"LEFT returns prefix {n}", f"LEFT('abcdef{n}', 3)", "abc"),
                (f"RIGHT returns suffix {n}", f"RIGHT('abcdef{n}', {len(str(n))})", str(n)),
                (f"LPAD pads left {n}", f"LPAD('{n}', 3, '0')", str(n).rjust(3, "0")),
                (f"RPAD pads right {n}", f"RPAD('{n}', 3, '0')", str(n).ljust(3, "0")),
                (f"LTRIM removes leading spaces {n}", f"LTRIM('  value{n}')", f"value{n}"),
                (f"RTRIM removes trailing spaces {n}", f"RTRIM('value{n}  ')", f"value{n}"),
                (f"TRIM removes outer spaces {n}", f"TRIM('  value{n}  ')", f"value{n}"),
                (f"CONCAT combines strings {n}", f"CONCAT('a', '{n}')", f"a{n}"),
                (f"CONCAT_WS joins strings {n}", f"CONCAT_WS('-', 'a', '{n}', 'z')", f"a-{n}-z"),
                (f"REPLACE substitutes strings {n}", f"REPLACE('abc{n}abc', 'a', 'z')", f"zbc{n}zbc"),
                (f"REPEAT duplicates strings {n}", f"REPEAT('x{n}', 2)", f"x{n}x{n}"),
                (f"REVERSE reverses strings {n}", f"REVERSE('ab{n}')", f"{str(n)[::-1]}ba"),
                (f"ASCII returns code point {n}", "ASCII('A')", 65),
                (f"CHR returns character {n}", "CHR(65)", "A"),
                (f"SPLIT_PART returns requested field {n}", f"SPLIT_PART('a-{n}-z', '-', 2)", str(n)),
                (f"GREATEST returns largest numeric argument {n}", f"GREATEST({n}, {n + 2}, {n + 1})", n + 2),
            ]
        )

    return sql_scalar_cases(
        prefix="UAT-SQL-FUNC",
        feature="sql.functions",
        docs=["docs/sql/functions.md"],
        rows_in=rows,
        profiles={"full"},
    )
