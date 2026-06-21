"""Shared SQL scalar generated-case helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..core import UatContext, compact, require, require_2xx, rows
from ..matrix import make_case


ScalarRow = tuple[str, str, Any]


def sql_scalar_cases(
    *,
    prefix: str,
    feature: str,
    docs: list[str],
    rows_in: Iterable[ScalarRow],
    priority: str = "P2",
    profiles: set[str] | None = None,
) -> list:
    cases = []
    for index, (title, expression, expected) in enumerate(rows_in, start=1):
        scenario_id = f"{prefix}-{index:03d}"

        def run(ctx: UatContext, expression: str = expression, expected: Any = expected, title: str = title) -> None:
            resp = ctx.client.post("/query", {"sql": f"SELECT {expression} AS value"})
            require_2xx(resp, title)
            result_rows = rows(resp.json)
            require(len(result_rows) == 1, f"{title}: expected one row, got {resp.text}")
            actual = result_rows[0].get("value")
            if isinstance(expected, float):
                require(isinstance(actual, (int, float)) and abs(float(actual) - expected) < 1e-9, f"{title}: expected {expected!r}, got {actual!r}")
            else:
                require(actual == expected, f"{title}: expected {expected!r}, got {actual!r} from {resp.text}")
            ctx.add_evidence(title, compact(resp))

        cases.append(
            make_case(
                scenario_id=scenario_id,
                title=title,
                feature=feature,
                docs=docs,
                acceptance=[
                    "The documented SQL expression runs through POST /query.",
                    "The returned scalar value matches the documented expression semantics.",
                ],
                fn=run,
                priority=priority,
                profiles=profiles or {"full"},
            )
        )
    return cases

