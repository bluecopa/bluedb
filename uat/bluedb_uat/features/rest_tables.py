"""Generated REST table data-plane acceptance cases."""

from __future__ import annotations

from ..core import UatContext, compact, query, require, require_2xx, rows
from ..matrix import make_case
from ._legacy import cases_by_id


_READY: set[int] = set()


def ensure_table(ctx: UatContext) -> str:
    table = ctx.table("rest_matrix")
    if ctx.run_id in _READY:
        return table
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
            {"name": "status", "type": "TEXT"},
            {"name": "score", "type": "INTEGER"},
        ],
    )
    docs = [
        {
            "id": index,
            "name": f"user-{index:02d}",
            "age": 20 + index,
            "status": "active" if index % 2 == 0 else "pending",
            "score": index * 10,
        }
        for index in range(1, 31)
    ]
    inserted = ctx.client.post(f"/tables/{table}", docs)
    require_2xx(inserted, "seed REST matrix table")
    _READY.add(ctx.run_id)
    ctx.add_evidence("REST matrix seed", compact(inserted))
    return table


def cases() -> list:
    generated = cases_by_id(
        "UAT-REST-001",
        "UAT-REST-002",
        "UAT-REST-003",
        "UAT-REST-004",
        "UAT-REST-005",
        "UAT-REST-006",
        "UAT-REST-007",
    )
    rows_in: list[tuple[str, dict[str, str], list[int], str]] = []

    for index in range(1, 31):
        rows_in.append(
            (
                f"REST eq filter returns primary-key row {index}",
                {"id": f"eq.{index}", "select": "id", "order": "id.asc"},
                [index],
                "docs/api/rest.md#get-tablestable-select",
            )
        )

    for index in range(1, 21):
        threshold = 20 + index
        rows_in.append(
            (
                f"REST gte filter returns rows at or above age {threshold}",
                {"age": f"gte.{threshold}", "select": "id", "order": "id.asc", "limit": "3"},
                list(range(index, min(index + 3, 31))),
                "docs/api/rest.md#get-tablestable-select",
            )
        )

    for index in range(1, 21):
        score = index * 10
        rows_in.append(
            (
                f"REST lt filter returns rows below score {score}",
                {"score": f"lt.{score}", "select": "id", "order": "id.desc", "limit": "2"},
                list(range(index - 1, max(index - 3, 0), -1)),
                "docs/api/rest.md#get-tablestable-select",
            )
        )

    for index in range(1, 16):
        limit = 2 + (index % 4)
        offset = index
        rows_in.append(
            (
                f"REST pagination returns stable page offset {offset} limit {limit}",
                {"select": "id", "order": "id.asc", "limit": str(limit), "offset": str(offset)},
                list(range(offset + 1, offset + limit + 1)),
                "docs/api/rest.md#get-tablestable-select",
            )
        )

    for case_number, (title, params, expected_ids, doc_ref) in enumerate(rows_in, start=1):
        scenario_id = f"UAT-REST-MATRIX-{case_number:03d}"

        def run(
            ctx: UatContext,
            *,
            title: str = title,
            params: dict[str, str] = params,
            expected_ids: list[int] = expected_ids,
        ) -> None:
            table = ensure_table(ctx)
            resp = ctx.client.get(f"/tables/{table}?{query(params)}")
            require_2xx(resp, title)
            actual = [row.get("id") for row in rows(resp.json)]
            require(actual == expected_ids, f"{title}: expected ids {expected_ids}, got {actual} from {resp.text}")
            ctx.add_evidence(title, compact(resp))

        generated.append(
            make_case(
                scenario_id=scenario_id,
                title=title,
                feature="rest.tables",
                docs=[doc_ref],
                acceptance=[
                    "The documented REST table read endpoint accepts the query parameters.",
                    "The returned row IDs match the expected filter/order/pagination semantics.",
                ],
                fn=run,
                priority="P2",
                profiles={"full"},
            )
        )

    generated.extend(returning_and_count_cases(len(rows_in)))
    return generated


def returning_and_count_cases(offset: int) -> list:
    cases_out = []
    for index in range(1, 16):
        scenario_id = f"UAT-REST-MATRIX-{offset + index:03d}"

        def run(ctx: UatContext, *, index: int = index) -> None:
            table = ensure_table(ctx)
            resp = ctx.client.get(
                f"/tables/{table}?{query({'status': 'eq.active', 'limit': str(index), 'offset': '0'})}",
                {"Prefer": "count=exact"},
            )
            require_2xx(resp, f"REST exact count active limit {index}")
            require(resp.headers.get("content-range") == f"0-{index - 1}/15", f"Content-Range mismatch: {resp.headers}")
            require(len(rows(resp.json)) == index, f"limited active rows mismatch: {resp.text}")
            ctx.add_evidence(f"REST exact count active limit {index}", compact(resp))

        cases_out.append(
            make_case(
                scenario_id=scenario_id,
                title=f"REST exact count header survives active limit {index}",
                feature="rest.tables",
                docs=["docs/api/rest.md#pagination-total-prefer-countexact"],
                acceptance=[
                    "Prefer: count=exact returns a Content-Range total.",
                    "The limited response still returns the requested page size.",
                ],
                fn=run,
                priority="P2",
                profiles={"full"},
            )
        )
    return cases_out
