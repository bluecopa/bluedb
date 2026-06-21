#!/usr/bin/env python3
"""User-acceptance test suite for bluedb's documented HTTP surfaces.

This suite intentionally treats bluedb as a black box:
- starts the documented `bluedb-server` binary,
- uses only HTTP requests described by the docs,
- records scenario-level evidence,
- writes a Markdown report suitable for product/engineering review.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


PASS = "PASS"
FAIL = "FAIL"
BLOCKED = "BLOCKED"


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    text: str
    json: Any


@dataclass
class Evidence:
    label: str
    detail: str


@dataclass
class UatResult:
    scenario_id: str
    title: str
    persona: str
    business_value: str
    docs: list[str]
    acceptance_criteria: list[str]
    status: str
    priority: str
    elapsed_ms: int
    evidence: list[Evidence] = field(default_factory=list)
    failure: str = ""
    feature: str = ""
    profiles: set[str] = field(default_factory=lambda: {"full"})
    tags: set[str] = field(default_factory=set)


@dataclass
class UatCase:
    scenario_id: str
    title: str
    persona: str
    business_value: str
    docs: list[str]
    acceptance_criteria: list[str]
    priority: str
    fn: Callable[["UatContext"], None]
    feature: str = ""
    profiles: set[str] = field(default_factory=lambda: {"full"})
    tags: set[str] = field(default_factory=set)


class BluedbClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        req_headers = {"accept": "application/json"}
        if headers:
            req_headers.update(headers)
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            req_headers.setdefault("content-type", "application/json")

        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=req_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return Response(
                    status=resp.status,
                    headers={k.lower(): v for k, v in resp.headers.items()},
                    text=raw,
                    json=parse_json(raw),
                )
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            return Response(
                status=exc.code,
                headers={k.lower(): v for k, v in exc.headers.items()},
                text=raw,
                json=parse_json(raw),
            )

    def get(self, path: str, headers: dict[str, str] | None = None) -> Response:
        return self.request("GET", path, headers=headers)

    def post(
        self,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.request("POST", path, body=body, headers=headers)

    def put(
        self,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.request("PUT", path, body=body, headers=headers)

    def patch(
        self,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.request("PATCH", path, body=body, headers=headers)

    def delete(
        self,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.request("DELETE", path, body=body, headers=headers)


class ManagedServer:
    def __init__(
        self,
        binary: Path,
        port: int,
        *,
        keep_data: bool,
        data_dir: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ):
        self.binary = binary
        self.port = port
        self.keep_data = keep_data
        self.tmpdir = Path(tempfile.mkdtemp(prefix=f"bluedb-uat-{port}-"))
        self.data_dir = data_dir or (self.tmpdir / "data")
        self.log_path = self.tmpdir / "server.log"
        self.proc: subprocess.Popen[bytes] | None = None
        self.env = os.environ.copy()
        self.env.update(
            {
                "BLUEDB_ADDR": f"127.0.0.1:{port}",
                "BLUEDB_DATA_DIR": str(self.data_dir),
                "BLUEDB_FTS_SEAL_INTERVAL_MS": "250",
                "BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS": "100",
                "BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS": "500",
                "BLUEDB_TTL_SWEEP_INTERVAL_SECS": "1",
            }
        )
        if extra_env:
            self.env.update(extra_env)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> BluedbClient:
        log = self.log_path.open("wb")
        self.proc = subprocess.Popen(
            [str(self.binary)],
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=str(self.binary.parent.parent.parent),
        )
        client = BluedbClient(self.base_url)
        deadline = time.time() + 60
        last_error = ""
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"server exited early with {self.proc.returncode}; log:\n"
                    f"{self.log_tail()}"
                )
            try:
                resp = client.get("/health")
                if resp.status == 200:
                    return client
                last_error = f"HTTP {resp.status}: {resp.text[:200]}"
            except Exception as exc:  # noqa: BLE001 - startup polling
                last_error = str(exc)
            time.sleep(0.25)
        raise RuntimeError(f"server did not become healthy: {last_error}")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        if not self.keep_data:
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def log_tail(self, lines: int = 120) -> str:
        if not self.log_path.exists():
            return ""
        return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-lines:])


class UatContext:
    def __init__(self, client: BluedbClient, run_id: int):
        self.client = client
        self.run_id = run_id
        self.evidence: list[Evidence] = []

    def add_evidence(self, label: str, detail: str) -> None:
        self.evidence.append(Evidence(label, detail))

    def table(self, prefix: str) -> str:
        return f"uat_{prefix}_{self.run_id}"

    def create_table(self, name: str, columns: list[dict[str, Any]]) -> Response:
        resp = self.client.post("/schema/tables", {"name": name, "columns": columns})
        require_2xx(resp, f"create table {name}")
        self.add_evidence(f"Created table {name}", compact(resp))
        return resp


def parse_json(raw: str) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_2xx(resp: Response, context: str) -> None:
    require(200 <= resp.status < 300, f"{context}: HTTP {resp.status} {resp.text[:500]}")


def require_error(resp: Response, context: str) -> None:
    require(resp.status >= 400, f"{context}: expected error, got HTTP {resp.status} {resp.text[:500]}")


def rows(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("rows", "data", "documents", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def query(params: dict[str, str]) -> str:
    return urllib.parse.urlencode(params, safe=",")


def compact(resp: Response, limit: int = 700) -> str:
    body = resp.text.replace("\n", " ")
    if len(body) > limit:
        body = body[: limit - 3] + "..."
    return f"HTTP {resp.status} {body}"


def wait_until(description: str, timeout_secs: float, fn: Callable[[], bool]) -> None:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        if fn():
            return
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {description}")


AREA_LABELS = {
    "OPS": "Operations",
    "QS": "Quickstart",
    "REST": "REST data plane",
    "SCHEMA": "Schema DDL",
    "SQL": "SQL",
    "TENANT": "Tenancy",
    "SEARCH": "SQL search",
    "COLL": "Collections",
    "LEDGER": "Ledger",
    "EVIDENCE": "Evidence and graph",
    "LAKEHOUSE": "Lakehouse",
    "SEC": "Security",
    "ENV": "Environment",
}


def area_for(result: UatResult) -> str:
    if result.feature:
        top = result.feature.split(".", 1)[0]
        labels = {
            "collections": "Collections",
            "evidence": "Evidence chains",
            "graph": "Native graph store",
            "lakehouse": "Lakehouse",
            "ledger": "Ledger",
            "operations": "Operations",
            "quickstart": "Quickstart",
            "rest": "REST data plane and schema DDL",
            "security": "Security",
            "sql": "SQL",
            "tenancy": "Tenancy",
        }
        if result.feature.startswith("collections.search"):
            return "Collection search"
        if result.feature.startswith("sql.search"):
            return "SQL search"
        return labels.get(top, top.title())
    parts = result.scenario_id.split("-")
    key = parts[1] if len(parts) > 1 else "ENV"
    if key == "COLL" and len(parts) > 2 and parts[2] == "SEARCH":
        return "Collection search"
    return AREA_LABELS.get(key, key.title())


def scenario_platform_ready(ctx: UatContext) -> None:
    health = ctx.client.get("/health")
    require_2xx(health, "health")
    ctx.add_evidence("Health endpoint", compact(health))

    status = ctx.client.get("/admin/status")
    require_2xx(status, "admin status")
    require(isinstance(status.json, dict), f"status should be a JSON object: {status.text}")
    require("active" in json.dumps(status.json).lower(), f"expected active writer: {status.text}")
    ctx.add_evidence("Admin status", compact(status))


def scenario_admin_sql_disabled(ctx: UatContext) -> None:
    disabled = ctx.client.post("/admin/sql", {"sql": "SELECT 1"})
    require_error(disabled, "admin sql disabled by default")
    ctx.add_evidence("Admin SQL disabled", compact(disabled))


def scenario_quickstart_sql(ctx: UatContext) -> None:
    table = ctx.table("quickstart")
    created = ctx.client.post(
        "/schema/tables",
        {
            "name": table,
            "columns": [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "name", "type": "TEXT"},
            ],
        },
    )
    require_2xx(created, "quickstart CREATE TABLE through /schema/tables")
    ctx.add_evidence("CREATE TABLE through /schema/tables", compact(created))

    insert_ada = ctx.client.post("/sql", {"sql": f"INSERT INTO {table} VALUES (1, 'ada')"})
    require_2xx(insert_ada, "quickstart first INSERT through /sql")
    insert_lin = ctx.client.post("/sql", {"sql": f"INSERT INTO {table} VALUES (2, 'lin')"})
    require_2xx(insert_lin, "quickstart second INSERT through /sql")
    ctx.add_evidence("INSERT through /sql", f"{compact(insert_ada)} | {compact(insert_lin)}")

    selected = ctx.client.post("/sql", {"sql": f"SELECT name FROM {table} ORDER BY id"})
    require_2xx(selected, "quickstart SELECT through /sql")
    names = [row.get("name") for row in rows(selected.json) if isinstance(row, dict)]
    require(names == ["ada", "lin"], f"quickstart rows mismatch: {selected.text}")
    ctx.add_evidence("SELECT result", compact(selected))


def scenario_rest_application_crud(ctx: UatContext) -> None:
    table = ctx.table("users")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT", "nullable": False},
            {"name": "age", "type": "INTEGER"},
            {"name": "status", "type": "TEXT"},
            {"name": "attrs", "type": "JSON"},
        ],
    )

    inserted = ctx.client.post(
        f"/tables/{table}",
        {"id": 1, "name": "ada", "age": 36, "status": "active", "attrs": {"team": "eng"}},
        {"Prefer": "return=representation"},
    )
    require_2xx(inserted, "insert user with representation")
    require(rows(inserted.json)[0]["attrs"] == {"team": "eng"}, f"JSON did not round-trip: {inserted.text}")
    ctx.add_evidence("Inserted user", compact(inserted))

    batch = ctx.client.post(
        f"/tables/{table}",
        [
            {"id": 2, "name": "lin", "age": 29, "status": "active", "attrs": {"team": "ops"}},
            {"id": 3, "name": "sam", "age": 17, "status": "pending", "attrs": {"team": "ops"}},
        ],
    )
    require_2xx(batch, "batch insert")

    filtered = ctx.client.get(
        f"/tables/{table}?{query({'select': 'name,age', 'age': 'gte.18', 'order': 'age.desc', 'limit': '10'})}"
    )
    require_2xx(filtered, "filtered read")
    require([row.get("name") for row in rows(filtered.json)] == ["ada", "lin"], f"filter mismatch: {filtered.text}")
    ctx.add_evidence("Filtered read", compact(filtered))

    counted = ctx.client.get(f"/tables/{table}?limit=1", {"Prefer": "count=exact"})
    require_2xx(counted, "count exact")
    require("content-range" in counted.headers, f"missing Content-Range: {counted.headers}")
    ctx.add_evidence("Count header", counted.headers.get("content-range", "missing"))

    patched = ctx.client.patch(
        f"/tables/{table}?id=eq.1",
        {"age": 37},
        {"Prefer": "return=representation"},
    )
    require_2xx(patched, "patch user")
    require(rows(patched.json)[0].get("age") == 37, f"patch mismatch: {patched.text}")

    deleted = ctx.client.delete(f"/tables/{table}?age=lt.18", headers={"Prefer": "return=representation"})
    require_2xx(deleted, "delete user")
    require(rows(deleted.json)[0].get("id") == 3, f"delete mismatch: {deleted.text}")
    ctx.add_evidence("Patch/delete", f"{compact(patched)} | {compact(deleted)}")

    duplicate = ctx.client.post(f"/tables/{table}", {"id": 1, "name": "duplicate"})
    require(duplicate.status == 409, f"duplicate PK should be HTTP 409: {compact(duplicate)}")
    require(
        duplicate.json and duplicate.json.get("code") == "UNIQUE_VIOLATION",
        f"missing stable duplicate code: {duplicate.text}",
    )
    ctx.add_evidence("Duplicate key rejection", compact(duplicate))


def scenario_schema_validation_and_indexes(ctx: UatContext) -> None:
    table = ctx.table("schema")
    created = ctx.client.post(
        "/schema/tables",
        {
            "name": table,
            "columns": [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "email", "type": "TEXT"},
                {"name": "age", "type": "INTEGER"},
            ],
            "indexes": [{"name": f"{table}_email", "columns": ["email"]}],
        },
    )
    require_2xx(created, "create table with inline index")
    described = ctx.client.get(f"/schema/tables/{table}")
    require_2xx(described, "describe indexed table")
    require(any(idx.get("name") == f"{table}_email" for idx in described.json.get("indexes", [])), described.text)
    email_col = next((col for col in described.json.get("columns", []) if col.get("name") == "email"), None)
    require(email_col and email_col.get("indexed") is True, f"email column not marked indexed: {described.text}")

    age_index = ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_age", "columns": ["age"]})
    require_2xx(age_index, "create follow-up index")
    dropped = ctx.client.delete(f"/schema/tables/{table}/indexes/{table}_age")
    require_2xx(dropped, "drop follow-up index")

    bad_name = ctx.client.post(
        "/schema/tables",
        {"name": f"{table}-bad", "columns": [{"name": "id", "type": "INTEGER", "primaryKey": True}]},
    )
    require_error(bad_name, "invalid identifier")
    bad_type = ctx.client.post(
        "/schema/tables",
        {"name": ctx.table("badtype"), "columns": [{"name": "id", "type": "STRUCT", "primaryKey": True}]},
    )
    require_error(bad_type, "invalid column type")
    ctx.add_evidence("Schema/index lifecycle", f"{compact(created)} | {compact(described)} | {compact(age_index)} | {compact(dropped)}")
    ctx.add_evidence("Validation errors", f"{compact(bad_name)} | {compact(bad_type)}")


def scenario_rest_freshness_headers(ctx: UatContext) -> None:
    table = ctx.table("fresh")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
        ],
    )
    inserted = ctx.client.post(f"/tables/{table}", {"id": 1, "name": "ada"})
    require_2xx(inserted, "freshness insert")
    watermark = inserted.headers.get("x-bluedb-watermark")
    require(watermark, f"write response missing X-Bluedb-Watermark: {inserted.headers}")
    read = ctx.client.get(f"/tables/{table}?id=eq.1", {"X-Bluedb-Min-Watermark": watermark})
    require_2xx(read, "min-watermark read")
    require(rows(read.json)[0].get("name") == "ada", f"fresh read mismatch: {read.text}")
    require(read.headers.get("x-bluedb-watermark"), f"read response missing X-Bluedb-Watermark: {read.headers}")
    ctx.add_evidence("Watermark write/read", f"write={watermark} read={read.headers.get('x-bluedb-watermark')}")


def scenario_error_contracts(ctx: UatContext) -> None:
    missing = ctx.client.get(f"/tables/{ctx.table('missing')}?id=eq.1")
    require(missing.status == 404, f"missing table should be 404: {compact(missing)}")
    require(missing.json and missing.json.get("code") == "NOT_FOUND", f"missing table code mismatch: {missing.text}")

    table = ctx.table("errors")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "age", "type": "INTEGER"},
        ],
    )
    type_mismatch = ctx.client.post(f"/tables/{table}", {"id": 1, "age": "not-a-number"})
    require(type_mismatch.status == 400, f"type mismatch should be 400: {compact(type_mismatch)}")
    require(
        type_mismatch.json and type_mismatch.json.get("code") == "TYPE_MISMATCH",
        f"type mismatch code mismatch: {type_mismatch.text}",
    )
    parse = ctx.client.post("/sql", {"sql": "SELECT FROM"})
    require_error(parse, "bad SQL parse")
    ctx.add_evidence("Classified errors", f"{compact(missing)} | {compact(type_mismatch)} | {compact(parse)}")


def scenario_rest_empty_and_no_match_contracts(ctx: UatContext) -> None:
    table = ctx.table("rest_empty")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "label", "type": "TEXT"},
            {"name": "score", "type": "INTEGER"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": 1, "label": "a", "score": 10}, {"id": 2, "label": "b", "score": 20}]),
        "insert REST empty-contract rows",
    )
    empty = ctx.client.get(f"/tables/{table}?label=eq.missing&limit=10", {"Prefer": "count=exact"})
    require_2xx(empty, "empty exact count read")
    require(rows(empty.json) == [], f"empty read should return []: {empty.text}")
    require(empty.headers.get("content-range") == "*/0", f"empty Content-Range mismatch: {empty.headers}")
    patch_none = ctx.client.patch(f"/tables/{table}?id=eq.999", {"score": 99}, {"Prefer": "return=representation"})
    require_2xx(patch_none, "PATCH no-match representation")
    require(rows(patch_none.json) == [], f"PATCH no-match should return []: {patch_none.text}")
    delete_none = ctx.client.delete(f"/tables/{table}?id=eq.999", headers={"Prefer": "return=representation"})
    require_2xx(delete_none, "DELETE no-match representation")
    require(rows(delete_none.json) == [], f"DELETE no-match should return []: {delete_none.text}")
    ctx.add_evidence("REST empty/no-match behavior", f"{compact(empty)} | {compact(patch_none)} | {compact(delete_none)}")


def scenario_rest_json_path_projection_contract(ctx: UatContext) -> None:
    table = ctx.table("rest_json_proj")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "attrs", "type": "JSON"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{table}",
            [
                {"id": 1, "attrs": {"status": "active", "n": 1}},
                {"id": 2, "attrs": {"status": "archived", "n": 2}},
            ],
        ),
        "insert REST JSON projection rows",
    )
    projected = ctx.client.get(f"/tables/{table}?{query({'select': 'id,attrs->>status', 'order': 'id.asc'})}")
    require_2xx(projected, "REST JSON path projection")
    require(len(rows(projected.json)) == 2 and "active" in json.dumps(projected.json), f"JSON projection mismatch: {projected.text}")
    filtered = ctx.client.get(f"/tables/{table}?{query({'select': 'id,attrs->>status', 'attrs->>status': 'eq.active'})}")
    require_2xx(filtered, "REST JSON path projection plus filter")
    require(len(rows(filtered.json)) == 1 and rows(filtered.json)[0].get("id") == 1, f"JSON projection/filter mismatch: {filtered.text}")
    ctx.add_evidence("REST JSON path projection", f"{compact(projected)} | {compact(filtered)}")


def scenario_schema_unique_column_and_drop_workflow(ctx: UatContext) -> None:
    table = ctx.table("schema_unique")
    created = ctx.client.post(
        "/schema/tables",
        {
            "name": table,
            "columns": [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "email", "type": "TEXT", "unique": True},
            ],
        },
    )
    require_2xx(created, "create schema unique-column table")
    require_2xx(ctx.client.post(f"/tables/{table}", {"id": 1, "email": "ada@example.test"}), "insert unique email")
    duplicate = ctx.client.post(f"/tables/{table}", {"id": 2, "email": "ada@example.test"})
    require(duplicate.status == 409 and duplicate.json.get("code") == "UNIQUE_VIOLATION", f"unique column duplicate mismatch: {compact(duplicate)}")
    dropped = ctx.client.delete(f"/schema/tables/{table}")
    require_2xx(dropped, "drop schema table")
    described = ctx.client.get(f"/schema/tables/{table}")
    require(described.status == 404, f"describe after drop should be 404: {compact(described)}")
    ctx.add_evidence("Schema unique/drop lifecycle", f"{compact(created)} | duplicate={compact(duplicate)} | drop={compact(dropped)} | describe={compact(described)}")


def scenario_schema_search_index_endpoint_errors(ctx: UatContext) -> None:
    table = ctx.table("schema_search_idx")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "body", "type": "TEXT"},
        ],
    )
    fulltext = ctx.client.post(f"/schema/tables/{table}/fulltext-indexes", {"column": "body", "analyzer": "english"})
    require_2xx(fulltext, "schema fulltext index")
    trigram = ctx.client.post(f"/schema/tables/{table}/trigram-indexes", {"column": "body"})
    require_2xx(trigram, "schema trigram index")
    missing_column = ctx.client.post(f"/schema/tables/{table}/fulltext-indexes", {"column": "missing"})
    require_error(missing_column, "fulltext missing column")
    missing_table = ctx.client.post(f"/schema/tables/{ctx.table('no_such_search_table')}/trigram-indexes", {"column": "body"})
    require_error(missing_table, "trigram missing table")
    ctx.add_evidence("Schema search-index endpoints", f"{compact(fulltext)} | {compact(trigram)} | missing_col={compact(missing_column)} | missing_table={compact(missing_table)}")


def scenario_rest_pagination_count_header_contract(ctx: UatContext) -> None:
    table = ctx.table("rest_page")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "label", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": i, "label": f"row-{i}"} for i in range(1, 6)]),
        "insert REST pagination rows",
    )

    with_count = ctx.client.get(
        f"/tables/{table}?{query({'order': 'id.asc', 'limit': '2', 'offset': '0'})}",
        {"Prefer": "count=exact"},
    )
    require_2xx(with_count, "REST first counted page")
    require([row["id"] for row in rows(with_count.json)] == [1, 2], f"first page mismatch: {with_count.text}")
    require(with_count.headers.get("content-range") == "0-1/5", f"first page Content-Range mismatch: {with_count.headers}")

    next_page = ctx.client.get(
        f"/tables/{table}?{query({'order': 'id.asc', 'limit': '2', 'offset': '2'})}",
        {"Prefer": "count=exact"},
    )
    require_2xx(next_page, "REST second counted page")
    require([row["id"] for row in rows(next_page.json)] == [3, 4], f"second page mismatch: {next_page.text}")
    require(next_page.headers.get("content-range") == "2-3/5", f"second page Content-Range mismatch: {next_page.headers}")

    without_count = ctx.client.get(f"/tables/{table}?limit=1")
    require_2xx(without_count, "REST page without exact count")
    require("content-range" not in without_count.headers, f"unexpected Content-Range without Prefer: {without_count.headers}")
    ctx.add_evidence("REST pagination/count headers", f"{compact(with_count)} | {compact(next_page)} | no-count headers={without_count.headers}")


def scenario_rest_unfiltered_mutation_contract(ctx: UatContext) -> None:
    table = ctx.table("rest_bulk")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "status", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": i, "status": "open"} for i in range(1, 4)]),
        "insert REST bulk mutation rows",
    )

    patch_rejected = ctx.client.patch(f"/tables/{table}", {"status": "closed"}, {"Prefer": "return=representation"})
    require(patch_rejected.status == 400 and "no filters" in patch_rejected.text, f"unfiltered PATCH should be rejected: {compact(patch_rejected)}")
    delete_rejected = ctx.client.delete(f"/tables/{table}", headers={"Prefer": "return=representation"})
    require(delete_rejected.status == 400 and "no filters" in delete_rejected.text, f"unfiltered DELETE should be rejected: {compact(delete_rejected)}")

    patched = ctx.client.patch(f"/tables/{table}?id=gte.0", {"status": "closed"}, {"Prefer": "return=representation"})
    require_2xx(patched, "explicit bulk REST PATCH")
    require(sorted(row["id"] for row in rows(patched.json)) == [1, 2, 3], f"indexed bulk PATCH should affect all rows: {patched.text}")
    require({row["status"] for row in rows(patched.json)} == {"closed"}, f"PATCH status mismatch: {patched.text}")

    deleted = ctx.client.delete(f"/tables/{table}?id=gte.0", headers={"Prefer": "return=representation"})
    require_2xx(deleted, "explicit bulk REST DELETE")
    require(sorted(row["id"] for row in rows(deleted.json)) == [1, 2, 3], f"indexed bulk DELETE should affect all rows: {deleted.text}")

    remaining = ctx.client.get(f"/tables/{table}", {"Prefer": "count=exact"})
    require_2xx(remaining, "read after unfiltered DELETE")
    require(rows(remaining.json) == [], f"table should be empty after unfiltered DELETE: {remaining.text}")
    require(remaining.headers.get("content-range") == "*/0", f"remaining Content-Range mismatch: {remaining.headers}")
    ctx.add_evidence(
        "REST unfiltered guard and explicit bulk PATCH/DELETE",
        f"rejected_patch={compact(patch_rejected)} | rejected_delete={compact(delete_rejected)} | patch={compact(patched)} | delete={compact(deleted)} | remaining={compact(remaining)}",
    )


def scenario_schema_duplicate_and_missing_targets(ctx: UatContext) -> None:
    table = ctx.table("schema_dupe")
    payload = {
        "name": table,
        "columns": [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "code", "type": "TEXT"},
        ],
    }
    created = ctx.client.post("/schema/tables", payload)
    require_2xx(created, "create schema duplicate target")
    duplicate = ctx.client.post("/schema/tables", payload)
    require_error(duplicate, "duplicate table creation")
    missing_column = ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_missing", "columns": ["missing"]})
    require_error(missing_column, "index on missing column")
    missing_table = ctx.client.post(f"/schema/tables/{ctx.table('schema_no_such_table')}/indexes", {"name": "idx", "columns": ["id"]})
    require_error(missing_table, "index on missing table")
    ctx.add_evidence("Schema duplicate/missing targets", f"{compact(created)} | duplicate={compact(duplicate)} | missing_col={compact(missing_column)} | missing_table={compact(missing_table)}")


def scenario_schema_nullable_description_contract(ctx: UatContext) -> None:
    table = ctx.table("schema_desc")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT", "nullable": False},
            {"name": "note", "type": "TEXT", "nullable": True},
        ],
    )
    indexed = ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_name", "columns": ["name"]})
    require_2xx(indexed, "create nullable-description index")
    described = ctx.client.get(f"/schema/tables/{table}")
    require_2xx(described, "describe nullable/indexed schema")
    columns = {column["name"]: column for column in described.json.get("columns", [])}
    require(
        columns["id"].get("primary_key") is True
        or columns["id"].get("primaryKey") is True
        or columns["id"].get("primary") is True,
        f"primary key not described: {described.text}",
    )
    require(columns["name"].get("nullable") is False, f"non-null column not described: {described.text}")
    require(columns["note"].get("nullable") is True, f"nullable column not described: {described.text}")
    require(columns["name"].get("indexed") is True or columns["name"].get("index") is True, f"indexed column not described: {described.text}")
    ctx.add_evidence("Schema nullable/index description", f"{compact(indexed)} | {compact(described)}")


def scenario_sql_default_order_and_set_contract(ctx: UatContext) -> None:
    table = ctx.table("sql_order")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "label", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": 3, "label": "c"}, {"id": 1, "label": "a"}, {"id": 2, "label": "b"}]),
        "insert default-order rows",
    )

    selected = ctx.client.post("/sql", {"sql": f"SELECT id FROM {table}"})
    require_2xx(selected, "default primary-key ordered SELECT")
    require([row["id"] for row in rows(selected.json)] == [1, 2, 3], f"default order mismatch: {selected.text}")
    setting = ctx.client.post("/sql", {"sql": "SET default_null_order = 'nulls_first'"})
    require_2xx(setting, "documented SET default_null_order")
    ctx.add_evidence("SQL default order and SET", f"{compact(selected)} | {compact(setting)}")


def scenario_sql_rejects_ddl_transactions_and_query_ddl(ctx: UatContext) -> None:
    table = ctx.table("sql_reject")
    ddl = ctx.client.post("/sql", {"sql": f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"})
    require_error(ddl, "/sql DDL rejection")
    tx = ctx.client.post("/sql", {"sql": "BEGIN"})
    require_error(tx, "/sql transaction statement rejection")
    query_ddl = ctx.client.post("/query", {"sql": f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"})
    require_error(query_ddl, "/query DDL rejection")
    ctx.add_evidence("SQL write-surface guardrails", f"{compact(ddl)} | {compact(tx)} | {compact(query_ddl)}")


def scenario_sql_secondary_index_range_order(ctx: UatContext) -> None:
    table = ctx.table("sql_range")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "score", "type": "INTEGER"},
            {"name": "label", "type": "TEXT"},
        ],
    )
    index = ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_score", "columns": ["score"]})
    require_2xx(index, "create score index for SQL range/order")
    ctx.add_evidence("Created score index", compact(index))
    require_2xx(
        ctx.client.post(
            f"/tables/{table}",
            [
                {"id": 1, "score": 20, "label": "mid"},
                {"id": 2, "score": 10, "label": "low"},
                {"id": 3, "score": 30, "label": "high"},
            ],
        ),
        "insert SQL indexed-range rows",
    )

    selected = ctx.client.post("/sql", {"sql": f"SELECT id, score FROM {table} WHERE score >= $1 ORDER BY score ASC", "params": [20]})
    require_2xx(selected, "SQL indexed range with ORDER BY")
    require(rows(selected.json) == [{"id": 1, "score": 20}, {"id": 3, "score": 30}], f"indexed range mismatch: {selected.text}")
    ctx.add_evidence("SQL secondary-index range/order", f"{compact(index)} | {compact(selected)}")


def scenario_sql_full_text_rank_filter_pagination(ctx: UatContext) -> None:
    table = ctx.table("sql_fts_rank")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "body", "type": "TEXT"},
            {"name": "status", "type": "TEXT"},
        ],
    )
    status_index = ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_status", "columns": ["status"]})
    require_2xx(status_index, "SQL FTS structured-filter index")
    index = ctx.client.post(f"/schema/tables/{table}/fulltext-indexes", {"column": "body", "analyzer": "english"})
    require_2xx(index, "SQL full-text index for ranking")
    ctx.add_evidence("Created FTS ranking indexes", f"{compact(status_index)} | {compact(index)}")
    require_2xx(
        ctx.client.post(
            f"/tables/{table}",
            [
                {"id": 1, "body": "invoice overdue overdue payment", "status": "open"},
                {"id": 2, "body": "invoice overdue payment", "status": "open"},
                {"id": 3, "body": "invoice overdue archived", "status": "closed"},
            ],
        ),
        "insert SQL FTS ranking rows",
    )

    rank_expr = "ts_rank(to_tsvector('english', body), plainto_tsquery($1))"
    first = ctx.client.post(
        "/sql",
        {
            "sql": (
                f"SELECT id FROM {table} "
                "WHERE to_tsvector('english', body) @@ plainto_tsquery($1) AND status = $2 "
                f"ORDER BY {rank_expr} DESC LIMIT 1 OFFSET 0"
            ),
            "params": ["invoice overdue", "open"],
        },
    )
    require_2xx(first, "SQL FTS first ranked page")
    second = ctx.client.post(
        "/sql",
        {
            "sql": (
                f"SELECT id FROM {table} "
                "WHERE to_tsvector('english', body) @@ plainto_tsquery($1) AND status = $2 "
                f"ORDER BY {rank_expr} DESC LIMIT 1 OFFSET 1"
            ),
            "params": ["invoice overdue", "open"],
        },
    )
    require_2xx(second, "SQL FTS second ranked page")
    require(len(rows(first.json)) == 1 and len(rows(second.json)) == 1, f"expected paged ranked rows: {first.text} | {second.text}")
    require({rows(first.json)[0]["id"], rows(second.json)[0]["id"]} == {1, 2}, f"ranked pages mismatch: {first.text} | {second.text}")
    ctx.add_evidence("SQL FTS rank/filter/pagination", f"{compact(status_index)} | {compact(index)} | {compact(first)} | {compact(second)}")


def scenario_sql_full_text_join_and_missing_index_errors(ctx: UatContext) -> None:
    docs = ctx.table("sql_fts_guard")
    owners = ctx.table("sql_fts_owner")
    ctx.create_table(
        docs,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "owner_id", "type": "INTEGER"},
            {"name": "body", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/schema/tables/{docs}/fulltext-indexes", {"column": "body", "analyzer": "english"}), "SQL full-text guard index")
    ctx.create_table(
        owners,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/tables/{docs}", {"id": 1, "owner_id": 10, "body": "invoice overdue"}), "insert FTS guard doc")
    require_2xx(ctx.client.post(f"/tables/{owners}", {"id": 10, "name": "finance"}), "insert FTS guard owner")
    join = ctx.client.post(
        "/sql",
        {
            "sql": (
                f"SELECT d.id FROM {docs} d JOIN {owners} o ON d.owner_id = o.id "
                "WHERE to_tsvector('english', d.body) @@ plainto_tsquery($1)"
            ),
            "params": ["invoice"],
        },
    )
    require_error(join, "SQL FTS join rejection")

    no_index = ctx.table("sql_fts_noidx")
    ctx.create_table(
        no_index,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "body", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/tables/{no_index}", {"id": 1, "body": "invoice overdue"}), "insert FTS no-index doc")
    missing_index = ctx.client.post(
        "/sql",
        {
            "sql": f"SELECT id FROM {no_index} WHERE to_tsvector('english', body) @@ plainto_tsquery($1)",
            "params": ["invoice"],
        },
    )
    require_error(missing_index, "SQL FTS missing-index rejection")
    ctx.add_evidence("SQL FTS guardrails", f"join={compact(join)} | missing_index={compact(missing_index)}")


def scenario_sql_trigram_no_index_query_fallback(ctx: UatContext) -> None:
    table = ctx.table("sql_trgm_guard")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "body", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": 1, "body": "invoice overdue notice"}, {"id": 2, "body": "welcome"}]),
        "insert trigram guard rows",
    )

    online = ctx.client.post("/sql", {"sql": f"SELECT id FROM {table} WHERE body LIKE $1", "params": ["%overdue%"]})
    require_error(online, "SQL LIKE without trigram index")
    analytical = ctx.client.post("/query", {"sql": f"SELECT id FROM {table} WHERE body LIKE $1 ORDER BY id", "params": ["%overdue%"]})
    require_2xx(analytical, "query LIKE fallback without trigram index")
    require(rows(analytical.json) == [{"id": 1}], f"query LIKE fallback mismatch: {analytical.text}")
    ctx.add_evidence("SQL trigram guardrail and /query fallback", f"{compact(online)} | {compact(analytical)}")


def scenario_sql_query_safety(ctx: UatContext) -> None:
    table = ctx.table("query")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{table}",
            [{"id": 1, "name": "ada", "age": 36}, {"id": 2, "name": "lin", "age": 29}],
        ),
        "insert query rows",
    )
    require_2xx(
        ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_name", "columns": ["name"]}),
        "create name index for parameterized lookup",
    )

    selected = ctx.client.post(
        "/sql",
        {"sql": f"SELECT name FROM {table} WHERE id = $1", "params": [1]},
    )
    require_2xx(selected, "parameterized point select")
    require([row.get("name") for row in rows(selected.json)] == ["ada"], f"parameterized mismatch: {selected.text}")
    ctx.add_evidence("Parameterized select", compact(selected))

    injection = ctx.client.post(
        "/sql",
        {"sql": f"SELECT name FROM {table} WHERE name = $1", "params": ["ada' OR 1=1 --"]},
    )
    require_2xx(injection, "injection-shaped parameter")
    require(rows(injection.json) == [], f"parameter was interpolated unsafely: {injection.text}")
    ctx.add_evidence("Injection-shaped parameter", compact(injection))

    scan_rejected = ctx.client.post(
        "/sql",
        {"sql": f"SELECT name FROM {table} WHERE age > $1 ORDER BY name", "params": [30]},
    )
    require(scan_rejected.status == 400 and scan_rejected.json.get("code") == "NO_INDEX", f"/sql scan should be rejected: {compact(scan_rejected)}")
    analytical = ctx.client.post(
        "/query",
        {"sql": f"SELECT name FROM {table} WHERE age > $1 ORDER BY name", "params": [30]},
    )
    require_2xx(analytical, "same scan through /query")
    require([row.get("name") for row in rows(analytical.json)] == ["ada"], f"/query scan mismatch: {analytical.text}")
    query_write = ctx.client.post(
        "/query",
        {"sql": f"INSERT INTO {table} VALUES (3, 'sam', 17)"},
    )
    require(
        query_write.status == 400 and query_write.json.get("code") == "UNSUPPORTED_STATEMENT",
        f"/query write should be rejected: {compact(query_write)}",
    )
    ctx.add_evidence("SQL/query tier split", f"{compact(scan_rejected)} | {compact(analytical)} | {compact(query_write)}")

    multi = ctx.client.post("/sql", {"sql": f"SELECT name FROM {table}; SELECT age FROM {table}"})
    require_error(multi, "multi-statement /sql")
    ctx.add_evidence("Multi-statement rejection", compact(multi))


def scenario_sql_analytics_surface(ctx: UatContext) -> None:
    customers = ctx.table("customers")
    orders = ctx.table("orders_sql")
    ctx.create_table(
        customers,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "region", "type": "TEXT"},
        ],
    )
    ctx.create_table(
        orders,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "customer_id", "type": "INTEGER"},
            {"name": "total", "type": "INTEGER"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{customers}",
            [
                {"id": 1, "name": "ada", "region": "EU"},
                {"id": 2, "name": "lin", "region": "US"},
                {"id": 3, "name": "sam", "region": "EU"},
            ],
        ),
        "insert customers",
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{orders}",
            [
                {"id": 10, "customer_id": 1, "total": 50},
                {"id": 11, "customer_id": 1, "total": 70},
                {"id": 12, "customer_id": 2, "total": 30},
            ],
        ),
        "insert orders",
    )
    grouped = ctx.client.post(
        "/query",
        {
            "sql": (
                f"SELECT c.region, COUNT(*) AS n, SUM(o.total) AS total "
                f"FROM {customers} c JOIN {orders} o ON c.id = o.customer_id "
                "GROUP BY c.region HAVING COUNT(*) > 0 ORDER BY c.region"
            )
        },
    )
    require_2xx(grouped, "join/group aggregate")
    result = {row["region"]: row for row in rows(grouped.json)}
    require(result.get("EU", {}).get("n") == 2 and result.get("US", {}).get("n") == 1, f"grouped mismatch: {grouped.text}")

    cte = ctx.client.post(
        "/query",
        {"sql": f"WITH eu AS (SELECT name FROM {customers} WHERE region = 'EU') SELECT name FROM eu ORDER BY name"},
    )
    require_2xx(cte, "CTE query")
    require([row.get("name") for row in rows(cte.json)] == ["ada", "sam"], f"CTE mismatch: {cte.text}")
    window = ctx.client.post(
        "/query",
        {"sql": f"SELECT id, ROW_NUMBER() OVER (ORDER BY total) AS rn FROM {orders} ORDER BY id"},
    )
    require_2xx(window, "window query")
    require(len(rows(window.json)) == 3, f"window row count mismatch: {window.text}")
    ctx.add_evidence("Analytics", f"{compact(grouped)} | {compact(cte)} | {compact(window)}")


def scenario_sql_functions_and_expressions(ctx: UatContext) -> None:
    table = ctx.table("expr")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "qty", "type": "INTEGER"},
            {"name": "price", "type": "FLOAT"},
            {"name": "note", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{table}",
            [
                {"id": 1, "name": "ada", "qty": 5, "price": 12.345, "note": None},
                {"id": 2, "name": "lin", "qty": 1, "price": 2.0, "note": "ok"},
            ],
        ),
        "insert expression rows",
    )
    expr = ctx.client.post(
        "/sql",
        {
            "sql": (
                "SELECT UPPER(name) AS upper_name, LENGTH(name) AS name_len, "
                "qty + 1 AS next_qty, CASE WHEN qty >= 5 THEN 'bulk' ELSE 'small' END AS bucket, "
                f"COALESCE(note, 'none') AS note_text FROM {table} WHERE id = $1"
            ),
            "params": [1],
        },
    )
    require_2xx(expr, "expression query")
    ctx.add_evidence("Expression query", compact(expr))
    row = rows(expr.json)[0]
    require(row.get("upper_name") == "ADA", f"UPPER mismatch: {expr.text}")
    require(str(row.get("name_len")) == "3", f"LENGTH mismatch: {expr.text}")
    require(row.get("next_qty") == 6, f"arithmetic mismatch: {expr.text}")
    require(row.get("bucket") == "bulk" and row.get("note_text") == "none", f"CASE/COALESCE mismatch: {expr.text}")
    casts = ctx.client.post(
        "/sql",
        {"sql": f"SELECT CAST('42' AS INTEGER) AS n, ROUND(price) AS rounded FROM {table} WHERE id = 1"},
    )
    require_2xx(casts, "cast/round query")
    require(rows(casts.json)[0].get("n") == 42, f"CAST mismatch: {casts.text}")
    ctx.add_evidence("Functions and expressions", f"{compact(expr)} | {compact(casts)}")


def scenario_sql_returning_contract(ctx: UatContext) -> None:
    table = ctx.table("returning")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "score", "type": "INTEGER"},
        ],
    )
    inserted = ctx.client.post("/sql", {"sql": f"INSERT INTO {table} VALUES (10, 'ten', 10) RETURNING id, name"})
    require_2xx(inserted, "INSERT RETURNING")
    require(rows(inserted.json) == [{"id": 10, "name": "ten"}], f"INSERT RETURNING mismatch: {inserted.text}")
    updated = ctx.client.post("/sql", {"sql": f"UPDATE {table} SET score = 99 WHERE id = 10 RETURNING id, score"})
    require_2xx(updated, "UPDATE RETURNING")
    require(rows(updated.json) == [{"id": 10, "score": 99}], f"UPDATE RETURNING mismatch: {updated.text}")
    deleted = ctx.client.post("/sql", {"sql": f"DELETE FROM {table} WHERE id = 10 RETURNING name"})
    require_2xx(deleted, "DELETE RETURNING")
    require(rows(deleted.json) == [{"name": "ten"}], f"DELETE RETURNING mismatch: {deleted.text}")
    missing_update = ctx.client.post("/sql", {"sql": f"UPDATE {table} SET score = 1 WHERE id = 10 RETURNING id"})
    require_2xx(missing_update, "UPDATE RETURNING no match")
    require(rows(missing_update.json) == [], f"no-match UPDATE RETURNING should be []: {missing_update.text}")
    reread = ctx.client.post("/sql", {"sql": f"SELECT id FROM {table} WHERE id = 10"})
    require_2xx(reread, "read after DELETE RETURNING")
    require(rows(reread.json) == [], f"deleted row still visible: {reread.text}")
    ctx.add_evidence("SQL RETURNING", f"{compact(inserted)} | {compact(updated)} | {compact(deleted)} | no_match={compact(missing_update)}")


def scenario_sql_autobound_and_query_scan_contract(ctx: UatContext) -> None:
    table = ctx.table("autobound")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "label", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/tables/{table}", [{"id": i, "label": f"row-{i:03d}"} for i in range(1, 106)]),
        "insert auto-bound rows",
    )
    sql_browse = ctx.client.post("/sql", {"sql": f"SELECT id FROM {table}"})
    require_2xx(sql_browse, "/sql no-WHERE browse")
    browse_rows = rows(sql_browse.json)
    require(len(browse_rows) == 100, f"/sql no-WHERE browse should auto-bound to 100 rows: {sql_browse.text[:500]}")
    query_count = ctx.client.post("/query", {"sql": f"SELECT COUNT(*) AS n FROM {table}"})
    require_2xx(query_count, "/query full scan count")
    require(rows(query_count.json)[0].get("n") == 105, f"/query count mismatch: {query_count.text}")
    sql_sort = ctx.client.post("/sql", {"sql": f"SELECT id FROM {table} ORDER BY label"})
    require(sql_sort.status == 400 and sql_sort.json.get("code") == "NO_INDEX", f"/sql non-indexed ORDER BY should be NO_INDEX: {compact(sql_sort)}")
    query_sort = ctx.client.post("/query", {"sql": f"SELECT id FROM {table} ORDER BY label DESC LIMIT 1"})
    require_2xx(query_sort, "/query non-indexed ORDER BY")
    require(rows(query_sort.json)[0].get("id") == 105, f"/query ORDER BY mismatch: {query_sort.text}")
    ctx.add_evidence("SQL auto-bound/query scan", f"{compact(sql_browse)} | {compact(query_count)} | {compact(sql_sort)} | {compact(query_sort)}")


def scenario_sql_query_freshness_and_pragmas(ctx: UatContext) -> None:
    table = ctx.table("query_fresh")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "label", "type": "TEXT"},
        ],
    )
    pragma = ctx.client.post("/sql", {"sql": "PRAGMA bluedb_read_wait_seal_n = 2"})
    require_2xx(pragma, "read wait seal PRAGMA")
    inserted = ctx.client.post(f"/tables/{table}", {"id": 1, "label": "fresh"})
    require_2xx(inserted, "insert for /query freshness")
    watermark = inserted.headers.get("x-bluedb-watermark")
    require(watermark, f"insert missing watermark: {inserted.headers}")
    query_read = ctx.client.post(
        "/query",
        {"sql": f"SELECT id FROM {table} WHERE label = $1", "params": ["fresh"]},
        {"X-Bluedb-Min-Watermark": watermark},
    )
    require_2xx(query_read, "/query min-watermark read")
    require(rows(query_read.json) == [{"id": 1}], f"/query min-watermark mismatch: {query_read.text}")
    bad_pragma = ctx.client.post("/sql", {"sql": "PRAGMA bluedb_read_wait_seal_n = -1"})
    require_error(bad_pragma, "negative read wait seal PRAGMA")
    ctx.add_evidence("Query freshness/PRAGMA", f"{compact(pragma)} | watermark={watermark} | {compact(query_read)} | bad={compact(bad_pragma)}")


def scenario_sql_query_syntax_deep_surface(ctx: UatContext) -> None:
    users = ctx.table("syntax_users")
    orders = ctx.table("syntax_orders")
    admins = ctx.table("syntax_admins")
    ctx.create_table(
        users,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "user_id", "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
            {"name": "nickname", "type": "TEXT"},
        ],
    )
    ctx.create_table(
        orders,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "user_id", "type": "INTEGER"},
            {"name": "total", "type": "INTEGER"},
        ],
    )
    ctx.create_table(
        admins,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{users}",
            [
                {"id": 1, "user_id": 10, "name": "ada", "age": 36, "nickname": None},
                {"id": 2, "user_id": 20, "name": "lin", "age": 29, "nickname": "l"},
                {"id": 3, "user_id": 30, "name": "sam", "age": 17, "nickname": None},
            ],
        ),
        "insert syntax users",
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{orders}",
            [{"id": 101, "user_id": 10, "total": 50}, {"id": 102, "user_id": 10, "total": 70}, {"id": 103, "user_id": 20, "total": 30}],
        ),
        "insert syntax orders",
    )
    require_2xx(ctx.client.post(f"/tables/{admins}", [{"id": 1, "name": "ada"}, {"id": 2, "name": "root"}]), "insert admins")

    left_join = ctx.client.post(
        "/query",
        {"sql": f"SELECT u.name, o.total FROM {users} u LEFT JOIN {orders} o ON u.user_id = o.user_id WHERE u.name = 'sam'"},
    )
    require_2xx(left_join, "left join")
    require(rows(left_join.json)[0].get("name") == "sam" and rows(left_join.json)[0].get("total") is None, f"left join mismatch: {left_join.text}")
    using_join = ctx.client.post(
        "/query",
        {"sql": f"SELECT name, total FROM {users} JOIN {orders} USING (user_id) WHERE name = 'lin'"},
    )
    require_2xx(using_join, "join using")
    require(rows(using_join.json)[0].get("total") == 30, f"join using mismatch: {using_join.text}")
    page = ctx.client.post("/query", {"sql": f"SELECT name FROM {users} ORDER BY age DESC LIMIT 1 OFFSET 1"})
    require_2xx(page, "limit offset")
    require([row.get("name") for row in rows(page.json)] == ["lin"], f"limit/offset mismatch: {page.text}")
    distinct = ctx.client.post("/query", {"sql": f"SELECT DISTINCT age >= 18 AS adult FROM {users} ORDER BY adult"})
    require_2xx(distinct, "distinct expression")
    require([row.get("adult") for row in rows(distinct.json)] == [False, True], f"distinct mismatch: {distinct.text}")
    set_ops = ctx.client.post(
        "/query",
        {
            "sql": (
                f"SELECT name FROM {users} WHERE age >= 18 "
                f"INTERSECT SELECT name FROM {admins} ORDER BY name"
            )
        },
    )
    require_2xx(set_ops, "intersect")
    require([row.get("name") for row in rows(set_ops.json)] == ["ada"], f"intersect mismatch: {set_ops.text}")
    nulls = ctx.client.post("/query", {"sql": f"SELECT name FROM {users} ORDER BY nickname NULLS FIRST, id"})
    require_2xx(nulls, "nulls first")
    require([row.get("name") for row in rows(nulls.json)[:2]] == ["ada", "sam"], f"null ordering mismatch: {nulls.text}")
    ctx.add_evidence(
        "Deep query syntax",
        f"{compact(left_join)} | {compact(using_join)} | {compact(page)} | {compact(distinct)} | {compact(set_ops)} | {compact(nulls)}",
    )


def scenario_sql_subqueries_and_set_operations(ctx: UatContext) -> None:
    users = ctx.table("sub_users")
    orders = ctx.table("sub_orders")
    ctx.create_table(
        users,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ],
    )
    ctx.create_table(
        orders,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "user_id", "type": "INTEGER"},
            {"name": "total", "type": "INTEGER"},
        ],
    )
    require_2xx(ctx.client.post(f"/tables/{users}", [{"id": 1, "name": "ada", "age": 36}, {"id": 2, "name": "lin", "age": 29}, {"id": 3, "name": "sam", "age": 17}]), "insert subquery users")
    require_2xx(ctx.client.post(f"/tables/{orders}", [{"id": 1, "user_id": 1, "total": 20}, {"id": 2, "user_id": 1, "total": 40}, {"id": 3, "user_id": 2, "total": 15}]), "insert subquery orders")
    in_query = ctx.client.post("/query", {"sql": f"SELECT name FROM {users} WHERE id IN (SELECT user_id FROM {orders}) ORDER BY name"})
    require_2xx(in_query, "IN subquery")
    require([row.get("name") for row in rows(in_query.json)] == ["ada", "lin"], f"IN subquery mismatch: {in_query.text}")
    exists = ctx.client.post(
        "/query",
        {"sql": f"SELECT name FROM {users} u WHERE EXISTS (SELECT 1 FROM {orders} o WHERE o.user_id = u.id AND o.total > 30)"},
    )
    require_2xx(exists, "EXISTS subquery")
    require([row.get("name") for row in rows(exists.json)] == ["ada"], f"EXISTS mismatch: {exists.text}")
    scalar = ctx.client.post(
        "/query",
        {"sql": f"SELECT name, (SELECT COUNT(*) FROM {orders} o WHERE o.user_id = u.id) AS orders FROM {users} u ORDER BY id"},
    )
    require_2xx(scalar, "scalar subquery")
    require([row.get("orders") for row in rows(scalar.json)] == [2, 1, 0], f"scalar mismatch: {scalar.text}")
    union_all = ctx.client.post(
        "/query",
        {"sql": f"SELECT name FROM {users} WHERE id = 1 UNION ALL SELECT name FROM {users} WHERE id = 1"},
    )
    require_2xx(union_all, "UNION ALL")
    require([row.get("name") for row in rows(union_all.json)] == ["ada", "ada"], f"UNION ALL mismatch: {union_all.text}")
    except_query = ctx.client.post(
        "/query",
        {"sql": f"SELECT id FROM {users} EXCEPT SELECT user_id AS id FROM {orders} ORDER BY id"},
    )
    require_2xx(except_query, "EXCEPT")
    require([row.get("id") for row in rows(except_query.json)] == [3], f"EXCEPT mismatch: {except_query.text}")
    ctx.add_evidence("Subqueries and set ops", f"{compact(in_query)} | {compact(exists)} | {compact(scalar)} | {compact(union_all)} | {compact(except_query)}")


def scenario_sql_data_types_value_encoding(ctx: UatContext) -> None:
    table = ctx.table("types")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "flag", "type": "BOOLEAN"},
            {"name": "amount", "type": "DECIMAL"},
            {"name": "day", "type": "DATE"},
            {"name": "slot", "type": "TIME"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "uid", "type": "UUID"},
        ],
    )
    uid = "550e8400-e29b-41d4-a716-446655440000"
    inserted = ctx.client.post(
        "/sql",
        {
            "sql": (
                f"INSERT INTO {table} VALUES (1, TRUE, CAST('12.34' AS DECIMAL), "
                "DATE '2024-03-15', TIME '14:30:05', TIMESTAMP '2024-03-15 12:34:56', "
                f"CAST('{uid}' AS UUID))"
            )
        },
    )
    require_2xx(inserted, "insert typed row")
    read = ctx.client.get(f"/tables/{table}?id=eq.1")
    require_2xx(read, "read typed row over REST")
    row = rows(read.json)[0]
    require(row.get("flag") is True, f"boolean mismatch: {read.text}")
    require(str(row.get("amount")) == "12.34", f"decimal encoding mismatch: {read.text}")
    require(row.get("day") == "2024-03-15", f"date encoding mismatch: {read.text}")
    require(row.get("slot") in ("14:30:05", "14:30:05.0"), f"time encoding mismatch: {read.text}")
    require(row.get("ts") in ("2024-03-15T12:34:56", "2024-03-15 12:34:56"), f"timestamp encoding mismatch: {read.text}")
    require(row.get("uid") == uid, f"UUID encoding mismatch: {read.text}")
    aliases = ctx.client.post("/sql", {"sql": "SELECT CAST('9.99' AS DECIMAL) AS dec, DATE '2026-01-15' AS day"})
    require_2xx(aliases, "type literals")
    ctx.add_evidence("Typed value encoding", f"{compact(inserted)} | {compact(read)} | {compact(aliases)}")


def scenario_sql_metadata_and_limitations(ctx: UatContext) -> None:
    table = ctx.table("meta")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "email", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/tables/{table}", {"id": 1, "email": "ada@example.test"}), "insert metadata row")
    require_2xx(ctx.client.post(f"/schema/tables/{table}/indexes", {"name": f"{table}_email", "columns": ["email"]}), "metadata index")
    objects = ctx.client.post(
        "/sql",
        {"sql": f"SELECT OBJECT_NAME, OBJECT_TYPE FROM GLUE_OBJECTS WHERE OBJECT_NAME IN ('{table}', '{table}_email') ORDER BY OBJECT_NAME"},
    )
    require_2xx(objects, "GLUE_OBJECTS")
    object_types = {row.get("OBJECT_NAME"): row.get("OBJECT_TYPE") for row in rows(objects.json)}
    require(object_types.get(table) == "TABLE" and object_types.get(f"{table}_email") == "INDEX", f"GLUE_OBJECTS mismatch: {objects.text}")
    columns = ctx.client.post(
        "/sql",
        {"sql": f"SELECT COLUMN_NAME FROM GLUE_TABLE_COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY COLUMN_ID"},
    )
    require_2xx(columns, "GLUE_TABLE_COLUMNS")
    require([row.get("COLUMN_NAME") for row in rows(columns.json)] == ["id", "email"], f"columns mismatch: {columns.text}")
    # GLUE_INDEXES lists a `PRIMARY` row for the clustered primary-key index;
    # filter it out to get the user-declared secondary indexes (see metadata.md).
    indexes = ctx.client.post(
        "/sql",
        {"sql": f"SELECT INDEX_NAME FROM GLUE_INDEXES WHERE TABLE_NAME = '{table}' AND INDEX_NAME <> 'PRIMARY'"},
    )
    require_2xx(indexes, "GLUE_INDEXES")
    require(rows(indexes.json)[0].get("INDEX_NAME") == f"{table}_email", f"indexes mismatch: {indexes.text}")
    query_metadata = ctx.client.post("/query", {"sql": "SELECT TABLE_NAME FROM GLUE_TABLES"})
    require(
        query_metadata.status == 400 and query_metadata.json.get("code") == "UNSUPPORTED_STATEMENT",
        f"GLUE_* should be /sql-only: {compact(query_metadata)}",
    )

    pk_update = ctx.client.post("/sql", {"sql": f"UPDATE {table} SET id = 2 WHERE id = 1"})
    require_error(pk_update, "primary-key update limitation")
    # Recursive CTEs are analytical reads and belong on `/query`.
    recursive = ctx.client.post("/query", {"sql": "WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t ORDER BY n"})
    require_2xx(recursive, "recursive CTE")
    require([row.get("n") for row in rows(recursive.json)] == [1, 2, 3, 4, 5], f"recursive CTE mismatch: {recursive.text}")
    create_type = ctx.client.post("/sql", {"sql": "CREATE TYPE mood AS ENUM ('sad', 'ok')"})
    require_error(create_type, "CREATE TYPE limitation")
    ctx.add_evidence("Metadata and limitations", f"{compact(objects)} | {compact(columns)} | {compact(indexes)} | {compact(query_metadata)} | {compact(pk_update)} | {compact(recursive)} | {compact(create_type)}")


def scenario_json_and_tenant_isolation(ctx: UatContext) -> None:
    events = ctx.table("events")
    ctx.create_table(
        events,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "actor", "type": "TEXT"},
            {"name": "attrs", "type": "JSON"},
        ],
    )
    inserted = ctx.client.post(
        f"/tables/{events}",
        [
            {"id": 1, "actor": "ada", "attrs": {"status": "active", "tags": ["a", "b"], "n": 3}},
            {"id": 2, "actor": "lin", "attrs": {"status": "archived", "tags": ["b"], "n": 4}},
        ],
    )
    require_2xx(inserted, "insert JSON rows")

    filtered = ctx.client.get(f"/tables/{events}?{query({'attrs->>status': 'eq.active', 'order': 'id.asc'})}")
    require_2xx(filtered, "JSON path filter")
    require([row.get("id") for row in rows(filtered.json)] == [1], f"JSON path mismatch: {filtered.text}")
    ctx.add_evidence("JSON path filter", compact(filtered))

    tenant_table = ctx.table("tenant")
    tenant = {"X-Bluedb-Tenant": "acme"}
    created = ctx.client.post(
        "/schema/tables",
        {
            "name": tenant_table,
            "columns": [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "name", "type": "TEXT"},
            ],
        },
        tenant,
    )
    require_2xx(created, "tenant table create")
    require_2xx(ctx.client.post(f"/tables/{tenant_table}", {"id": 1, "name": "tenant-row"}, tenant), "tenant write")
    tenant_read = ctx.client.get(f"/tables/{tenant_table}?id=eq.1", tenant)
    require_2xx(tenant_read, "tenant read")
    default_read = ctx.client.get(f"/tables/{tenant_table}?id=eq.1")
    require(default_read.status in (400, 404), f"default tenant should not see tenant table: {compact(default_read)}")
    ctx.add_evidence("Tenant isolation", f"tenant={compact(tenant_read)} default={compact(default_read)}")


def scenario_json_advanced_queries(ctx: UatContext) -> None:
    events = ctx.table("json")
    ctx.create_table(
        events,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "attrs", "type": "JSON"},
        ],
    )
    require_2xx(
        ctx.client.post(
            f"/tables/{events}",
            [
                {"id": 1, "attrs": {"status": "active", "tags": ["a", "b"], "meta": {"region": "EU"}}},
                {"id": 2, "attrs": {"status": "archived", "tags": ["c"], "meta": {"region": "US"}}},
            ],
        ),
        "insert advanced JSON rows",
    )
    containment = ctx.client.post(
        "/query",
        {"sql": f"SELECT id FROM {events} WHERE attrs @> '{{\"status\":\"active\"}}' ORDER BY id"},
    )
    require_2xx(containment, "JSON containment")
    require([row.get("id") for row in rows(containment.json)] == [1], f"containment mismatch: {containment.text}")
    path = ctx.client.post(
        "/query",
        {"sql": f"SELECT jsonb_path_query(attrs, '$.meta.region') AS region FROM {events} WHERE id = 1"},
    )
    require_2xx(path, "JSON path query")
    require(rows(path.json)[0].get("region") in ('"EU"', "EU"), f"path query mismatch: {path.text}")
    array_path = ctx.client.post(
        "/query",
        {"sql": f"SELECT jsonb_path_query_array(attrs, '$.tags[*]') AS tags FROM {events} WHERE id = 1"},
    )
    require_2xx(array_path, "JSON path array")
    require("a" in json.dumps(rows(array_path.json)[0].get("tags")), f"path array mismatch: {array_path.text}")
    sql_json = ctx.client.post(
        "/sql",
        {"sql": f"SELECT id FROM {events} WHERE (attrs ->> 'status') = 'active'"},
    )
    require(sql_json.status == 400 and sql_json.json.get("code") == "NO_INDEX", f"JSON path on /sql should be rejected with NO_INDEX: {compact(sql_json)}")
    ctx.add_evidence("Advanced JSON", f"{compact(containment)} | {compact(path)} | {compact(array_path)} | /sql={compact(sql_json)}")


def scenario_sql_full_text_search(ctx: UatContext) -> None:
    docs = ctx.table("docs")
    ctx.create_table(
        docs,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "title", "type": "TEXT"},
            {"name": "body", "type": "TEXT"},
            {"name": "status", "type": "TEXT"},
        ],
    )
    require_2xx(
        ctx.client.post(f"/schema/tables/{docs}/fulltext-indexes", {"column": "body", "analyzer": "english"}),
        "declare fulltext index",
    )
    require_2xx(ctx.client.post(f"/schema/tables/{docs}/trigram-indexes", {"column": "body"}), "declare trigram index")
    require_2xx(
        ctx.client.post(
            f"/tables/{docs}",
            [
                {"id": 1, "title": "Invoice", "body": "invoice overdue payment", "status": "open"},
                {"id": 2, "title": "Greeting", "body": "hello database storage", "status": "open"},
            ],
        ),
        "insert searchable docs",
    )

    selected = ctx.client.post(
        "/sql",
        {
            "sql": (
                f"SELECT id, title FROM {docs} "
                "WHERE to_tsvector('english', body) @@ plainto_tsquery($1) "
                "ORDER BY id"
            ),
            "params": ["invoice overdue"],
        },
    )
    require_2xx(selected, "parameterized FTS query")
    require([row.get("id") for row in rows(selected.json)] == [1], f"FTS mismatch: {selected.text}")
    ctx.add_evidence("Parameterized FTS", compact(selected))

    like = ctx.client.post("/sql", {"sql": f"SELECT id FROM {docs} WHERE body LIKE '%overdue%'"})
    require_2xx(like, "trigram LIKE")
    require([row.get("id") for row in rows(like.json)] == [1], f"LIKE mismatch: {like.text}")
    ctx.add_evidence("Trigram LIKE", compact(like))


def scenario_sql_search_variants_and_mutations(ctx: UatContext) -> None:
    docs = ctx.table("fts_variants")
    ctx.create_table(
        docs,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "title", "type": "TEXT"},
            {"name": "body", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/schema/tables/{docs}/fulltext-indexes", {"column": "body", "analyzer": "english"}), "declare FTS")
    require_2xx(
        ctx.client.post(
            f"/tables/{docs}",
            [
                {"id": 1, "title": "Overdue", "body": "invoice overdue payment"},
                {"id": 2, "title": "Paid", "body": "invoice paid receipt"},
                {"id": 3, "title": "Past due", "body": "past due invoice"},
            ],
        ),
        "insert FTS variants",
    )
    boolean = ctx.client.post(
        "/sql",
        {
            "sql": f"SELECT id FROM {docs} WHERE to_tsvector('english', body) @@ to_tsquery($1) ORDER BY id",
            "params": ["invoice & !paid"],
        },
    )
    require_2xx(boolean, "to_tsquery variant")
    require([row.get("id") for row in rows(boolean.json)] == [1, 3], f"to_tsquery mismatch: {boolean.text}")
    web = ctx.client.post(
        "/sql",
        {
            "sql": f"SELECT id FROM {docs} WHERE to_tsvector('english', body) @@ websearch_to_tsquery($1) ORDER BY id",
            "params": ['"past due"'],
        },
    )
    require_2xx(web, "websearch_to_tsquery variant")
    require([row.get("id") for row in rows(web.json)] == [3], f"websearch mismatch: {web.text}")
    require_2xx(ctx.client.patch(f"/tables/{docs}?id=eq.1", {"body": "invoice paid"}), "FTS update")
    after_update = ctx.client.post(
        "/sql",
        {"sql": f"SELECT id FROM {docs} WHERE to_tsvector('english', body) @@ plainto_tsquery($1) ORDER BY id", "params": ["overdue"]},
    )
    require_2xx(after_update, "FTS after update")
    require(rows(after_update.json) == [], f"FTS update tombstone mismatch: {after_update.text}")
    ctx.add_evidence("FTS variants/mutations", f"{compact(boolean)} | {compact(web)} | {compact(after_update)}")


def scenario_collections_document_workflow(ctx: UatContext) -> None:
    orders = ctx.table("orders")
    inserted = ctx.client.post(
        f"/collections/{orders}/insert",
        {
            "documents": [
                {"customer": "ada", "amount": 120, "status": "pending", "tags": ["new", "priority"]},
                {"customer": "lin", "amount": 85, "status": "shipped", "tags": ["done"]},
            ]
        },
    )
    require_2xx(inserted, "collection insert")
    require(inserted.json.get("insertedCount") == 2, f"inserted count mismatch: {inserted.text}")
    ctx.add_evidence("Inserted collection documents", compact(inserted))

    found = ctx.client.post(f"/collections/{orders}/find", {"filter": {"status": "pending"}, "sort": {"amount": -1}})
    require_2xx(found, "collection find")
    require(rows(found.json)[0].get("customer") == "ada", f"find mismatch: {found.text}")

    updated = ctx.client.post(
        f"/collections/{orders}/update",
        {"filter": {"customer": "ada"}, "update": {"$inc": {"amount": 10}}},
    )
    require_2xx(updated, "collection update")
    require(updated.json.get("matchedCount") == 1, f"update mismatch: {updated.text}")

    counted = ctx.client.post(f"/collections/{orders}/count", {"filter": {"amount": {"$gte": 100}}})
    require_2xx(counted, "collection numeric count")
    require(counted.json.get("count") == 1, f"numeric range count mismatch: {counted.text}")
    ctx.add_evidence("Find/update/count", f"{compact(found)} | {compact(updated)} | {compact(counted)}")

    indexed = ctx.client.post(f"/collections/{orders}/createIndex", {"keys": {"status": 1}, "options": {"unique": False}})
    require_2xx(indexed, "collection index")
    unsupported = ctx.client.post(f"/collections/{orders}/find", {"filter": {"tags": {"$elemMatch": {"$eq": "new"}}}})
    require_error(unsupported, "unsupported operator")
    require(unsupported.json and unsupported.json.get("ok") == 0, f"Mongo-shaped error missing: {unsupported.text}")
    ctx.add_evidence("Index and unsupported operator", f"{compact(indexed)} | {compact(unsupported)}")


def scenario_collections_aggregation_and_lookup(ctx: UatContext) -> None:
    orders = ctx.table("orders_agg")
    customers = ctx.table("customers_agg")
    require_2xx(
        ctx.client.post(
            f"/collections/{customers}/insert",
            {"documents": [{"_id": "c1", "name": "ada", "tier": "gold"}, {"_id": "c2", "name": "lin", "tier": "silver"}]},
        ),
        "insert aggregate customers",
    )
    require_2xx(
        ctx.client.post(
            f"/collections/{orders}/insert",
            {
                "documents": [
                    {"_id": "o1", "customer": "ada", "amount": 120, "tags": ["priority", "new"]},
                    {"_id": "o2", "customer": "ada", "amount": 80, "tags": ["new"]},
                    {"_id": "o3", "customer": "lin", "amount": 40, "tags": ["done"]},
                ]
            },
        ),
        "insert aggregate orders",
    )

    def aggregate_ready() -> bool:
        grouped = ctx.client.post(
            f"/collections/{orders}/aggregate",
            {"pipeline": [{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "count": {"$sum": 1}}}]},
        )
        if grouped.status != 200:
            return False
        counts = {row.get("_id"): row.get("count") for row in rows(grouped.json)}
        return counts.get("new") == 2 and counts.get("priority") == 1

    wait_until("collection aggregate seal", 8, aggregate_ready)
    grouped = ctx.client.post(
        f"/collections/{orders}/aggregate",
        {"pipeline": [{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "count": {"$sum": 1}}}]},
    )
    require_2xx(grouped, "aggregate unwind/group")
    lookup = ctx.client.post(
        f"/collections/{orders}/aggregate",
        {"pipeline": [{"$lookup": {"from": customers, "localField": "customer", "foreignField": "name", "as": "customerDoc"}}]},
    )
    require_2xx(lookup, "aggregate lookup")
    require(any(row.get("customerDoc") for row in rows(lookup.json)), f"lookup did not attach customer docs: {lookup.text}")
    deleted = ctx.client.post(f"/collections/{orders}/delete", {"filter": {"customer": "lin"}, "multi": False})
    require_2xx(deleted, "collection delete")
    require(deleted.json.get("deletedCount") == 1, f"delete count mismatch: {deleted.text}")
    ctx.add_evidence("Aggregation/lookup/delete", f"{compact(grouped)} | {compact(lookup)} | {compact(deleted)}")


def scenario_collections_index_semantics(ctx: UatContext) -> None:
    products = ctx.table("products")
    indexed = ctx.client.post(
        f"/collections/{products}/createIndex",
        {"keys": {"sku": 1}, "options": {"unique": True, "type": "string"}},
    )
    require_2xx(indexed, "unique index")
    first = ctx.client.post(f"/collections/{products}/insert", {"documents": [{"_id": "p1", "sku": "A-1", "tags": ["blue", "sale"]}]})
    require_2xx(first, "unique indexed insert")
    duplicate = ctx.client.post(f"/collections/{products}/insert", {"documents": [{"_id": "p2", "sku": "A-1", "tags": ["red"]}]})
    require_error(duplicate, "unique index duplicate")

    multikey = ctx.client.post(f"/collections/{products}/createIndex", {"keys": {"tags": 1}, "options": {"type": "string"}})
    require_2xx(multikey, "multikey index")
    require_2xx(
        ctx.client.post(f"/collections/{products}/insert", {"documents": [{"_id": "p3", "sku": "B-2", "tags": ["blue", "new"]}]}),
        "second indexed insert",
    )
    found = ctx.client.post(f"/collections/{products}/find", {"filter": {"tags": "blue"}, "sort": {"sku": 1}})
    require_2xx(found, "multikey membership find")
    require([doc.get("sku") for doc in rows(found.json)] == ["A-1", "B-2"], f"multikey membership mismatch: {found.text}")
    ctx.add_evidence("Unique and multikey indexes", f"{compact(indexed)} | duplicate={compact(duplicate)} | {compact(multikey)} | {compact(found)}")


def scenario_collections_filter_semantics(ctx: UatContext) -> None:
    orders = ctx.table("orders_filter")
    require_2xx(
        ctx.client.post(f"/collections/{orders}/createIndex", {"keys": {"status": 1}, "options": {"type": "string"}}),
        "status filter index",
    )
    require_2xx(
        ctx.client.post(f"/collections/{orders}/createIndex", {"keys": {"amount": 1}, "options": {"type": "number"}}),
        "amount filter index",
    )
    require_2xx(
        ctx.client.post(f"/collections/{orders}/createIndex", {"keys": {"enabled": 1}, "options": {"type": "bool"}}),
        "enabled filter index",
    )
    inserted = ctx.client.post(
        f"/collections/{orders}/insert",
        {
            "documents": [
                {"_id": "f1", "status": "pending", "amount": 120, "enabled": True},
                {"_id": "f2", "status": "shipped", "amount": 85, "enabled": False},
                {"_id": "f3", "amount": 40, "enabled": True},
            ]
        },
    )
    require_2xx(inserted, "insert filter documents")

    in_query = ctx.client.post(
        f"/collections/{orders}/find",
        {"filter": {"status": {"$in": ["pending", "shipped"]}}, "sort": {"_id": 1}},
    )
    require_2xx(in_query, "$in filter")
    require([doc.get("_id") for doc in rows(in_query.json)] == ["f1", "f2"], f"$in mismatch: {in_query.text}")

    ne_query = ctx.client.post(
        f"/collections/{orders}/find",
        {"filter": {"status": {"$ne": "pending"}}, "sort": {"_id": 1}},
    )
    require_2xx(ne_query, "$ne filter")
    require([doc.get("_id") for doc in rows(ne_query.json)] == ["f2", "f3"], f"$ne mismatch: {ne_query.text}")

    missing_count = ctx.client.post(f"/collections/{orders}/count", {"filter": {"status": {"$exists": False}}})
    require_2xx(missing_count, "$exists false count")
    require(missing_count.json.get("count") == 1, f"$exists false mismatch: {missing_count.text}")

    or_query = ctx.client.post(
        f"/collections/{orders}/find",
        {"filter": {"$or": [{"status": "pending"}, {"enabled": False}]}, "sort": {"_id": 1}},
    )
    require_2xx(or_query, "$or filter")
    require([doc.get("_id") for doc in rows(or_query.json)] == ["f1", "f2"], f"$or mismatch: {or_query.text}")

    and_nin = ctx.client.post(
        f"/collections/{orders}/find",
        {
            "filter": {"$and": [{"amount": {"$gte": 80}}, {"status": {"$nin": ["pending"]}}]},
            "sort": {"_id": 1},
        },
    )
    require_2xx(and_nin, "$and/$nin/range filter")
    require([doc.get("_id") for doc in rows(and_nin.json)] == ["f2"], f"$and/$nin mismatch: {and_nin.text}")
    ctx.add_evidence(
        "Collection filter semantics",
        f"{compact(in_query)} | {compact(ne_query)} | {compact(missing_count)} | {compact(or_query)} | {compact(and_nin)}",
    )


def scenario_collections_update_operators(ctx: UatContext) -> None:
    docs = ctx.table("updates")
    inserted = ctx.client.post(
        f"/collections/{docs}/insert",
        {"documents": [{"_id": "u1", "amount": 1, "tags": ["a"], "status": "new", "remove_me": "x"}]},
    )
    require_2xx(inserted, "insert update target")
    updated = ctx.client.post(
        f"/collections/{docs}/update",
        {
            "filter": {"_id": "u1"},
            "update": {
                "$set": {"status": "active"},
                "$unset": {"remove_me": ""},
                "$inc": {"amount": 2},
                "$push": {"tags": "b"},
            },
        },
    )
    require_2xx(updated, "collection update operators")
    require(updated.json.get("matchedCount") == 1 and updated.json.get("modifiedCount") == 1, f"update count mismatch: {updated.text}")
    pulled = ctx.client.post(
        f"/collections/{docs}/update",
        {"filter": {"_id": "u1"}, "update": {"$pull": {"tags": "a"}}},
    )
    require_2xx(pulled, "collection pull operator")
    read = ctx.client.post(f"/collections/{docs}/find", {"filter": {"_id": "u1"}})
    require_2xx(read, "read updated document")
    doc = rows(read.json)[0]
    require(doc.get("status") == "active" and doc.get("amount") == 3, f"updated scalar fields mismatch: {read.text}")
    require(doc.get("tags") == ["b"], f"$push/$pull mismatch: {read.text}")
    require("remove_me" not in doc, f"$unset mismatch: {read.text}")

    replaced = ctx.client.post(
        f"/collections/{docs}/update",
        {"filter": {"_id": "u1"}, "update": {"status": "replaced", "amount": 10}},
    )
    require_2xx(replaced, "collection replacement update")
    replaced_read = ctx.client.post(f"/collections/{docs}/find", {"filter": {"_id": "u1"}})
    require_2xx(replaced_read, "read replacement document")
    replacement = rows(replaced_read.json)[0]
    require(replacement.get("_id") == "u1", f"replacement did not preserve _id: {replaced_read.text}")
    require(
        replacement.get("status") == "replaced" and replacement.get("amount") == 10 and "tags" not in replacement,
        f"replacement mismatch: {replaced_read.text}",
    )
    unsupported = ctx.client.post(
        f"/collections/{docs}/update",
        {"filter": {"_id": "u1"}, "update": {"$addToSet": {"tags": "c"}}},
    )
    require_error(unsupported, "unsupported collection update operator")
    ctx.add_evidence(
        "Collection update semantics",
        f"{compact(updated)} | {compact(pulled)} | {compact(read)} | {compact(replaced_read)} | {compact(unsupported)}",
    )


def scenario_collections_projection_pagination_upsert_delete(ctx: UatContext) -> None:
    coll = ctx.table("proj")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {
                "documents": [
                    {"_id": "p1", "name": "ada", "status": "new", "score": 30},
                    {"_id": "p2", "name": "lin", "status": "new", "score": 20},
                    {"_id": "p3", "name": "sam", "status": "old", "score": 10},
                ]
            },
        ),
        "insert projection docs",
    )
    projected = ctx.client.post(
        f"/collections/{coll}/find",
        {"filter": {}, "projection": {"name": 1}, "sort": {"score": -1}, "limit": 2, "skip": 1},
    )
    require_2xx(projected, "collection projection/limit/skip")
    docs = rows(projected.json)
    require([doc.get("name") for doc in docs] == ["lin", "sam"], f"projection page mismatch: {projected.text}")
    require(all(set(doc.keys()) == {"_id", "name"} for doc in docs), f"projection shape mismatch: {projected.text}")

    upsert = ctx.client.post(
        f"/collections/{coll}/update",
        {"filter": {"name": "zoe"}, "update": {"$set": {"name": "zoe", "status": "new", "score": 5}}, "upsert": True},
    )
    require_2xx(upsert, "collection upsert")
    require(upsert.json.get("matchedCount") == 0 and isinstance(upsert.json.get("upsertedId"), str), f"upsert mismatch: {upsert.text}")
    multi = ctx.client.post(
        f"/collections/{coll}/update",
        {"filter": {"status": "new"}, "update": {"$set": {"reviewed": True}}, "multi": True},
    )
    require_2xx(multi, "collection multi update")
    require(multi.json.get("matchedCount") == 3 and multi.json.get("modifiedCount") == 3, f"multi update mismatch: {multi.text}")
    delete_one = ctx.client.post(f"/collections/{coll}/delete", {"filter": {"status": "new"}, "multi": False})
    require_2xx(delete_one, "collection delete one")
    require(delete_one.json.get("deletedCount") == 1, f"delete one mismatch: {delete_one.text}")
    delete_rest = ctx.client.post(f"/collections/{coll}/delete", {"filter": {"status": "new"}, "multi": True})
    require_2xx(delete_rest, "collection delete many")
    require(delete_rest.json.get("deletedCount") == 2, f"delete many mismatch: {delete_rest.text}")
    ctx.add_evidence("Projection/upsert/delete", f"{compact(projected)} | {compact(upsert)} | {compact(multi)} | {compact(delete_one)} | {compact(delete_rest)}")


def scenario_collections_aggregation_stage_matrix(ctx: UatContext) -> None:
    coll = ctx.table("agg_matrix")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {
                "documents": [
                    {"_id": "a1", "region": "EU", "amount": 10, "kind": "retail"},
                    {"_id": "a2", "region": "EU", "amount": 30, "kind": "retail"},
                    {"_id": "a3", "region": "US", "amount": 20, "kind": "enterprise"},
                    {"_id": "a4", "region": "US", "amount": 40, "kind": "retail"},
                ]
            },
        ),
        "insert aggregate matrix docs",
    )

    def aggregate_ready() -> bool:
        probe = ctx.client.post(f"/collections/{coll}/aggregate", {"pipeline": [{"$count": "n"}]})
        return probe.status == 200 and rows(probe.json) and rows(probe.json)[0].get("n") == 4

    wait_until("aggregate matrix seal", 8, aggregate_ready)
    grouped = ctx.client.post(
        f"/collections/{coll}/aggregate",
        {
            "pipeline": [
                {"$match": {"kind": "retail"}},
                {"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "avg": {"$avg": "$amount"}, "min": {"$min": "$amount"}, "max": {"$max": "$amount"}, "count": {"$count": {}}}},
                {"$sort": {"total": -1}},
                {"$skip": 0},
                {"$limit": 2},
            ]
        },
    )
    require_2xx(grouped, "collection aggregate stage matrix")
    result = {doc.get("_id"): doc for doc in rows(grouped.json)}
    require(result.get("US", {}).get("total") == 40 and result.get("EU", {}).get("total") == 40, f"group matrix mismatch: {grouped.text}")
    projected = ctx.client.post(
        f"/collections/{coll}/aggregate",
        {"pipeline": [{"$match": {"region": "EU"}}, {"$project": {"who": "$region", "amount": 1}}, {"$addFields": {"tag": "$who"}}]},
    )
    require_2xx(projected, "collection aggregate project/addFields")
    require(all(doc.get("who") == "EU" and doc.get("tag") == "EU" for doc in rows(projected.json)), f"project/addFields mismatch: {projected.text}")
    unsupported = ctx.client.post(f"/collections/{coll}/aggregate", {"pipeline": [{"$facet": {"x": []}}]})
    require_error(unsupported, "unsupported aggregate stage")
    regex_rejected = ctx.client.post(f"/collections/{coll}/aggregate", {"pipeline": [{"$match": {"region": {"$regex": "E.*"}}}]})
    require_error(regex_rejected, "regex aggregate rejection")
    ctx.add_evidence("Aggregation stage matrix", f"{compact(grouped)} | {compact(projected)} | {compact(unsupported)} | {compact(regex_rejected)}")


def scenario_collections_compound_ttl_and_index_errors(ctx: UatContext) -> None:
    coll = ctx.table("idx_contract")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/createIndex",
            {"keys": {"region": 1, "status": 1}, "options": {"unique": False}},
        ),
        "collection compound index",
    )
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {
                "documents": [
                    {"_id": "i1", "region": "EU", "status": "open", "expires": 1},
                    {"_id": "i2", "region": "EU", "status": "closed", "expires": 4_102_444_800},
                    {"_id": "i3", "region": "US", "status": "open", "expires": 4_102_444_800},
                ]
            },
        ),
        "insert compound/ttl docs",
    )
    compound = ctx.client.post(
        f"/collections/{coll}/find",
        {"filter": {"region": "EU", "status": "open"}},
    )
    require_2xx(compound, "compound index equality")
    require([doc.get("_id") for doc in rows(compound.json)] == ["i1"], f"compound equality mismatch: {compound.text}")
    ttl = ctx.client.post(
        f"/collections/{coll}/createIndex",
        {"keys": {"expires": 1}, "options": {"expireAfterSeconds": 0, "type": "number"}},
    )
    require_2xx(ttl, "TTL index creation")
    wait_until(
        "TTL sweep removed expired doc",
        5,
        lambda: ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "i1"}}).status == 200
        and rows(ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "i1"}}).json) == [],
    )
    live = ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "i2"}})
    require_2xx(live, "TTL live doc remains")
    require([doc.get("_id") for doc in rows(live.json)] == ["i2"], f"TTL removed live doc: {live.text}")
    negative_ttl = ctx.client.post(
        f"/collections/{ctx.table('negative_ttl')}/createIndex",
        {"keys": {"expires": 1}, "options": {"expireAfterSeconds": -1}},
    )
    require_error(negative_ttl, "negative TTL rejected")
    dotted_compound = ctx.client.post(
        f"/collections/{ctx.table('dotted_compound')}/createIndex",
        {"keys": {"a.b": 1, "c": 1}},
    )
    require_error(dotted_compound, "dotted compound index rejected")
    dotted_multikey = ctx.client.post(
        f"/collections/{ctx.table('dotted_multi')}/createIndex",
        {"keys": {"a.b": 1}, "options": {"multikey": True}},
    )
    require_error(dotted_multikey, "dotted multikey index rejected")
    ctx.add_evidence("Compound/TTL/index errors", f"{compact(compound)} | {compact(ttl)} | live={compact(live)} | neg={compact(negative_ttl)} | dotted={compact(dotted_compound)} | {compact(dotted_multikey)}")


def scenario_collections_generated_ids_and_request_validation(ctx: UatContext) -> None:
    coll = ctx.table("ids")
    inserted = ctx.client.post(
        f"/collections/{coll}/insert",
        {"documents": [{"name": "ada"}, {"name": "lin"}]},
    )
    require_2xx(inserted, "collection generated id insert")
    ids = inserted.json.get("insertedIds", [])
    require(inserted.json.get("insertedCount") == 2 and len(ids) == 2, f"inserted ids mismatch: {inserted.text}")
    require(all(isinstance(item, str) and len(item) == 24 for item in ids), f"generated ids should be 24-char strings: {inserted.text}")
    require(len(set(ids)) == 2, f"generated ids should be distinct: {inserted.text}")
    first = ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": ids[0]}})
    require_2xx(first, "find by generated _id")
    require(rows(first.json)[0].get("_id") == ids[0] and rows(first.json)[0].get("name") == "ada", f"find by _id mismatch: {first.text}")
    empty = ctx.client.post(f"/collections/{coll}/insert", {"documents": []})
    require_2xx(empty, "empty collection insert")
    require(empty.json.get("insertedCount") == 0 and empty.json.get("insertedIds") == [], f"empty insert mismatch: {empty.text}")
    malformed = ctx.client.post(f"/collections/{coll}/insert", {"docs": []})
    require_error(malformed, "collection insert missing documents")
    ctx.add_evidence("Generated ids and request validation", f"{compact(inserted)} | find={compact(first)} | empty={compact(empty)} | malformed={compact(malformed)}")


def scenario_collections_regex_not_and_unsupported_filters(ctx: UatContext) -> None:
    coll = ctx.table("regex")
    require_2xx(
        ctx.client.post(f"/collections/{coll}/createIndex", {"keys": {"name": 1}, "options": {"type": "string"}}),
        "regex name index",
    )
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {"documents": [{"_id": "r1", "name": "ada"}, {"_id": "r2", "name": "lin"}, {"_id": "r3", "name": "adam"}]},
        ),
        "insert regex docs",
    )
    regex = ctx.client.post(f"/collections/{coll}/find", {"filter": {"name": {"$regex": "^ad"}}, "sort": {"name": 1}})
    require_2xx(regex, "collection regex filter")
    require([doc.get("name") for doc in rows(regex.json)] == ["ada", "adam"], f"regex mismatch: {regex.text}")
    not_query = ctx.client.post(f"/collections/{coll}/find", {"filter": {"$not": {"name": "ada"}}, "sort": {"_id": 1}})
    require_2xx(not_query, "collection top-level $not")
    require([doc.get("_id") for doc in rows(not_query.json)] == ["r2", "r3"], f"$not mismatch: {not_query.text}")
    unsupported_field = ctx.client.post(f"/collections/{coll}/find", {"filter": {"name": {"$type": "string"}}})
    require_error(unsupported_field, "unsupported $type filter")
    unsupported_top = ctx.client.post(f"/collections/{coll}/find", {"filter": {"$expr": {"$eq": ["$name", "ada"]}}})
    require_error(unsupported_top, "unsupported top-level $expr filter")
    ctx.add_evidence("Regex/$not/unsupported filters", f"{compact(regex)} | {compact(not_query)} | type={compact(unsupported_field)} | expr={compact(unsupported_top)}")


def scenario_collections_update_initializers(ctx: UatContext) -> None:
    coll = ctx.table("init_updates")
    require_2xx(ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "u1", "name": "ada"}]}), "insert initializer doc")
    initialized = ctx.client.post(
        f"/collections/{coll}/update",
        {"filter": {"_id": "u1"}, "update": {"$inc": {"visits": 2}, "$push": {"events": "created"}}},
    )
    require_2xx(initialized, "collection update initializes absent fields")
    read = ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "u1"}})
    require_2xx(read, "read initialized update doc")
    doc = rows(read.json)[0]
    require(doc.get("visits") == 2 and doc.get("events") == ["created"], f"$inc/$push initializer mismatch: {read.text}")
    pulled = ctx.client.post(f"/collections/{coll}/update", {"filter": {"_id": "u1"}, "update": {"$pull": {"events": "created"}}})
    require_2xx(pulled, "pull initialized array")
    after_pull = ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "u1"}})
    require_2xx(after_pull, "read after pull initialized array")
    require(rows(after_pull.json)[0].get("events") == [], f"$pull should leave empty array: {after_pull.text}")
    ctx.add_evidence("Update initializers", f"{compact(initialized)} | {compact(read)} | {compact(pulled)} | {compact(after_pull)}")


def scenario_collections_count_null_group_and_lookup_empty(ctx: UatContext) -> None:
    orders = ctx.table("agg_contract")
    customers = ctx.table("agg_customers_empty")
    require_2xx(
        ctx.client.post(f"/collections/{customers}/insert", {"documents": [{"_id": "c1", "name": "ada"}]}),
        "insert lookup customers",
    )
    require_2xx(
        ctx.client.post(
            f"/collections/{orders}/insert",
            {
                "documents": [
                    {"_id": "o1", "customer": "ada", "amount": 10},
                    {"_id": "o2", "customer": "lin", "amount": 30},
                    {"_id": "o3", "customer": "sam", "amount": 20},
                ]
            },
        ),
        "insert aggregate contract orders",
    )

    def aggregate_ready() -> bool:
        probe = ctx.client.post(f"/collections/{orders}/aggregate", {"pipeline": [{"$count": "n"}]})
        return probe.status == 200 and rows(probe.json) and rows(probe.json)[0].get("n") == 3

    wait_until("collection count/null-group seal", 8, aggregate_ready)
    counted = ctx.client.post(f"/collections/{orders}/aggregate", {"pipeline": [{"$count": "n"}]})
    require_2xx(counted, "collection aggregate $count")
    require(rows(counted.json) == [{"n": 3}], f"$count mismatch: {counted.text}")
    grouped = ctx.client.post(
        f"/collections/{orders}/aggregate",
        {"pipeline": [{"$group": {"_id": None, "total": {"$sum": "$amount"}, "count": {"$count": {}}}}]},
    )
    require_2xx(grouped, "collection aggregate null group")
    group_doc = rows(grouped.json)[0]
    require(group_doc.get("_id") is None and group_doc.get("total") == 60 and group_doc.get("count") == 3, f"null group mismatch: {grouped.text}")
    lookup = ctx.client.post(
        f"/collections/{orders}/aggregate",
        {"pipeline": [{"$lookup": {"from": customers, "localField": "customer", "foreignField": "name", "as": "customerDoc"}}]},
    )
    require_2xx(lookup, "lookup with no-match array")
    by_id = {doc.get("_id"): doc for doc in rows(lookup.json)}
    require(by_id.get("o1", {}).get("customerDoc"), f"matching lookup missing: {lookup.text}")
    require(by_id.get("o2", {}).get("customerDoc") == [] and by_id.get("o3", {}).get("customerDoc") == [], f"lookup no-match should be []: {lookup.text}")
    ctx.add_evidence("Aggregate count/null group/lookup empty", f"{compact(counted)} | {compact(grouped)} | {compact(lookup)}")


def scenario_collections_index_backfill_idempotency_and_bad_paths(ctx: UatContext) -> None:
    coll = ctx.table("idx_backfill")
    require_2xx(ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "b1", "kind": "tool"}]}), "insert before index")
    first_index = ctx.client.post(f"/collections/{coll}/createIndex", {"keys": {"kind": 1}, "options": {"type": "string"}})
    require_2xx(first_index, "create index after existing docs")
    second_index = ctx.client.post(f"/collections/{coll}/createIndex", {"keys": {"kind": 1}, "options": {"type": "string"}})
    require_2xx(second_index, "idempotent create index")
    backfilled = ctx.client.post(f"/collections/{coll}/find", {"filter": {"kind": "tool"}})
    require_2xx(backfilled, "find backfilled indexed doc")
    require([doc.get("_id") for doc in rows(backfilled.json)] == ["b1"], f"backfilled index mismatch: {backfilled.text}")
    bad_path = ctx.client.post(
        f"/collections/{coll}/createIndex",
        {"keys": {"x) TEXT; DROP TABLE nope; --": 1}},
    )
    require_error(bad_path, "malformed collection index path")
    intact = ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "b2", "kind": "tool"}]})
    require_2xx(intact, "insert after bad index path")
    ctx.add_evidence("Index backfill/idempotency/bad path", f"{compact(first_index)} | second={compact(second_index)} | find={compact(backfilled)} | bad={compact(bad_path)} | intact={compact(intact)}")


def scenario_collections_duplicate_id_and_count_contract(ctx: UatContext) -> None:
    coll = ctx.table("dupe_id")
    first = ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "same", "status": "open"}]})
    require_2xx(first, "insert collection _id")
    duplicate = ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "same", "status": "closed"}]})
    require_error(duplicate, "duplicate collection _id")
    require(duplicate.json and duplicate.json.get("ok") == 0, f"duplicate _id should use Mongo-shaped error: {duplicate.text}")
    all_count = ctx.client.post(f"/collections/{coll}/count", {"filter": {}})
    require_2xx(all_count, "collection count all")
    require(all_count.json.get("count") == 1, f"count all mismatch: {all_count.text}")
    filtered_count = ctx.client.post(f"/collections/{coll}/count", {"filter": {"status": "missing"}})
    require_2xx(filtered_count, "collection count filtered miss")
    require(filtered_count.json.get("count") == 0, f"filtered count mismatch: {filtered_count.text}")
    ctx.add_evidence("Duplicate _id and count contract", f"{compact(first)} | duplicate={compact(duplicate)} | all={compact(all_count)} | filtered={compact(filtered_count)}")


def scenario_collections_scalar_multikey_and_unsupported_update_errors(ctx: UatContext) -> None:
    coll = ctx.table("scalar_multikey")
    index = ctx.client.post(
        f"/collections/{coll}/createIndex",
        {"keys": {"tags": 1}, "options": {"type": "string", "multikey": True}},
    )
    require_2xx(index, "scalar-compatible multikey index")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {"documents": [{"_id": "scalar", "tags": "blue"}, {"_id": "array", "tags": ["blue", "red"]}]},
        ),
        "insert scalar and array multikey docs",
    )
    found = ctx.client.post(f"/collections/{coll}/find", {"filter": {"tags": "blue"}, "sort": {"_id": 1}})
    require_2xx(found, "find scalar and array values through multikey index")
    require([doc.get("_id") for doc in rows(found.json)] == ["array", "scalar"], f"scalar multikey mismatch: {found.text}")

    pop = ctx.client.post(f"/collections/{coll}/update", {"filter": {"_id": "array"}, "update": {"$pop": {"tags": 1}}})
    require_error(pop, "unsupported $pop update")
    rename = ctx.client.post(f"/collections/{coll}/update", {"filter": {"_id": "array"}, "update": {"$rename": {"tags": "labels"}}})
    require_error(rename, "unsupported $rename update")
    mul = ctx.client.post(f"/collections/{coll}/update", {"filter": {"_id": "array"}, "update": {"$mul": {"n": 2}}})
    require_error(mul, "unsupported $mul update")
    ctx.add_evidence("Scalar multikey and unsupported updates", f"{compact(index)} | find={compact(found)} | pop={compact(pop)} | rename={compact(rename)} | mul={compact(mul)}")


def scenario_collections_tenant_isolation(ctx: UatContext) -> None:
    coll = ctx.table("tenant_docs")
    tenant = {"X-Bluedb-Tenant": "acme"}
    default_insert = ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "default", "scope": "default"}]})
    require_2xx(default_insert, "default collection insert")
    tenant_insert = ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "tenant", "scope": "tenant"}]}, tenant)
    require_2xx(tenant_insert, "tenant collection insert")
    default_read = ctx.client.post(f"/collections/{coll}/find", {"filter": {}, "sort": {"_id": 1}})
    require_2xx(default_read, "default collection read")
    require([doc.get("_id") for doc in rows(default_read.json)] == ["default"], f"default collection leaked tenant doc: {default_read.text}")
    tenant_read = ctx.client.post(f"/collections/{coll}/find", {"filter": {}, "sort": {"_id": 1}}, tenant)
    require_2xx(tenant_read, "tenant collection read")
    require([doc.get("_id") for doc in rows(tenant_read.json)] == ["tenant"], f"tenant collection leaked default doc: {tenant_read.text}")
    tenant_count = ctx.client.post(f"/collections/{coll}/count", {"filter": {}}, tenant)
    require_2xx(tenant_count, "tenant collection count")
    require(tenant_count.json.get("count") == 1, f"tenant count mismatch: {tenant_count.text}")
    ctx.add_evidence("Collection tenant isolation", f"default={compact(default_read)} | tenant={compact(tenant_read)} | count={compact(tenant_count)}")


def scenario_collections_unwind_missing_and_lookup_dotted_path_errors(ctx: UatContext) -> None:
    coll = ctx.table("unwind_contract")
    foreign = ctx.table("lookup_foreign")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {"documents": [{"_id": "u1", "tags": ["x", "y"], "profile": {"name": "ada"}}, {"_id": "u2"}, {"_id": "u3", "tags": None}]},
        ),
        "insert unwind contract docs",
    )
    require_2xx(ctx.client.post(f"/collections/{foreign}/insert", {"documents": [{"_id": "f1", "name": "ada"}]}), "insert lookup foreign docs")

    def aggregate_ready() -> bool:
        probe = ctx.client.post(f"/collections/{coll}/aggregate", {"pipeline": [{"$unwind": "$tags"}, {"$count": "n"}]})
        return probe.status == 200 and rows(probe.json) and rows(probe.json)[0].get("n") == 2

    wait_until("collection unwind missing/null seal", 8, aggregate_ready)
    unwind_count = ctx.client.post(f"/collections/{coll}/aggregate", {"pipeline": [{"$unwind": "$tags"}, {"$count": "n"}]})
    require_2xx(unwind_count, "unwind ignores missing/null arrays")
    require(rows(unwind_count.json) == [{"n": 2}], f"unwind missing/null mismatch: {unwind_count.text}")
    dotted_lookup = ctx.client.post(
        f"/collections/{coll}/aggregate",
        {"pipeline": [{"$lookup": {"from": foreign, "localField": "profile.name", "foreignField": "name", "as": "matches"}}]},
    )
    require_error(dotted_lookup, "dotted $lookup field rejection")
    ctx.add_evidence("Collection unwind and lookup guardrail", f"{compact(unwind_count)} | dotted_lookup={compact(dotted_lookup)}")


def scenario_collections_unknown_operation_and_identifier_errors(ctx: UatContext) -> None:
    coll = ctx.table("op_errors")
    require_2xx(ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "ok"}]}), "insert collection operation-error doc")
    unknown = ctx.client.post(f"/collections/{coll}/replaceOne", {"filter": {"_id": "ok"}})
    require_error(unknown, "unknown collection operation")
    invalid_find = ctx.client.post("/collections/1bad/find", {"filter": {}})
    require(invalid_find.status == 400, f"invalid collection identifier should be 400: {compact(invalid_find)}")
    invalid_index = ctx.client.post("/collections/1bad/createIndex", {"keys": {"field": 1}})
    require(invalid_index.status == 400, f"invalid collection index identifier should be 400: {compact(invalid_index)}")
    still_works = ctx.client.post(f"/collections/{coll}/find", {"filter": {"_id": "ok"}})
    require_2xx(still_works, "collection remains usable after bad operation")
    require([doc.get("_id") for doc in rows(still_works.json)] == ["ok"], f"collection unusable after bad operation: {still_works.text}")
    ctx.add_evidence("Collection operation/identifier errors", f"unknown={compact(unknown)} | bad_find={compact(invalid_find)} | bad_index={compact(invalid_index)} | intact={compact(still_works)}")


def scenario_collection_search_workflow(ctx: UatContext) -> None:
    articles = ctx.table("articles")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {
                "documents": [
                    {
                        "_id": "a1",
                        "title": "Object storage databases",
                        "body": "A database built directly on object storage",
                        "author": "ada",
                        "year": 2023,
                    },
                    {
                        "_id": "a2",
                        "title": "Ledger internals",
                        "body": "Double entry accounting and transfer validation",
                        "author": "lin",
                        "year": 2021,
                    },
                ]
            },
        ),
        "insert articles",
    )
    mapping = ctx.client.post(
        f"/collections/{articles}/searchIndex",
        {
            "fields": {
                "title": {"analyzer": "english"},
                "body": {"analyzer": "english"},
                "author": {"type": "keyword"},
                "year": {"type": "integer"},
            }
        },
    )
    require_2xx(mapping, "declare search mapping")
    ctx.add_evidence("Search mapping", compact(mapping))

    described = ctx.client.get(f"/collections/{articles}/searchIndex")
    require_2xx(described, "describe search mapping")
    searched = ctx.client.post(
        f"/collections/{articles}/search",
        {
            "query": {"match": {"body": {"query": "database storage", "operator": "and"}}},
            "size": 5,
            "_source": ["title", "body", "author", "year"],
            "highlight": {"fields": {"body": {}}},
        },
    )
    require_2xx(searched, "search collection")
    hits = searched.json.get("hits", {}).get("hits", [])
    require(len(hits) == 1 and hits[0].get("_id") == "a1", f"search hits mismatch: {searched.text}")
    require("highlight" in hits[0], f"search highlight missing: {searched.text}")
    ctx.add_evidence("Search result", compact(searched))


def scenario_collection_search_dsl_variants(ctx: UatContext) -> None:
    articles = ctx.table("articles_dsl")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {
                "documents": [
                    {"_id": "a1", "title": "Object storage databases", "body": "database storage engine", "author": "ada", "year": 2023},
                    {"_id": "a2", "title": "Ledger transfers", "body": "accounting transfer validation", "author": "lin", "year": 2021},
                    {"_id": "a3", "title": "Database catalog", "body": "database catalog metadata", "author": "sam", "year": 2019},
                ]
            },
        ),
        "insert DSL articles",
    )
    mapping = ctx.client.post(
        f"/collections/{articles}/searchIndex",
        {
            "fields": {
                "title": {"analyzer": "standard"},
                "body": {"analyzer": "standard"},
                "author": {"type": "keyword"},
                "year": {"type": "integer"},
            }
        },
    )
    require_2xx(mapping, "declare DSL mapping")
    bool_query = ctx.client.post(
        f"/collections/{articles}/search",
        {
            "query": {
                "bool": {
                    "must": [{"match": {"body": {"query": "database", "operator": "and"}}}],
                    "filter": [{"range": {"year": {"gte": 2020}}}],
                    "must_not": [{"term": {"author": "sam"}}],
                }
            },
            "sort": [{"year": "desc"}],
            "_source": ["title", "year"],
            "size": 5,
        },
    )
    require_2xx(bool_query, "bool/range search")
    hits = bool_query.json.get("hits", {}).get("hits", [])
    require(len(hits) == 1 and hits[0].get("_id") == "a1", f"bool/range hits mismatch: {bool_query.text}")
    require(set(hits[0].get("_source", {}).keys()) == {"title", "year"}, f"_source projection mismatch: {bool_query.text}")

    no_source = ctx.client.post(f"/collections/{articles}/search", {"query": {"exists": {"field": "author"}}, "_source": False, "size": 1})
    require_2xx(no_source, "exists with _source false")
    first_hit = no_source.json.get("hits", {}).get("hits", [{}])[0]
    require("_source" not in first_hit, f"_source false should omit source: {no_source.text}")
    unsupported = ctx.client.post(f"/collections/{articles}/search", {"query": {"wildcard": {"title": "data*"}}})
    require_error(unsupported, "unsupported wildcard query")
    ctx.add_evidence("Search DSL variants", f"{compact(bool_query)} | {compact(no_source)} | {compact(unsupported)}")


def scenario_collection_search_error_contracts(ctx: UatContext) -> None:
    articles = ctx.table("articles_errors")
    no_mapping = ctx.client.get(f"/collections/{articles}/searchIndex")
    require(no_mapping.status == 404, f"missing search mapping should be 404: {compact(no_mapping)}")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {
                "documents": [
                    {"_id": "e1", "title": "Object storage databases", "body": "database storage engine", "author": "ada", "year": 2024}
                ]
            },
        ),
        "insert search error article",
    )
    mapping = ctx.client.post(
        f"/collections/{articles}/searchIndex",
        {
            "fields": {
                "title": {"analyzer": "standard"},
                "body": {"analyzer": "standard"},
                "author": {"type": "keyword"},
                "year": {"type": "integer"},
            }
        },
    )
    require_2xx(mapping, "declare error-contract mapping")

    unmapped = ctx.client.post(f"/collections/{articles}/search", {"query": {"term": {"missing": "x"}}})
    require_error(unmapped, "unmapped search field")
    range_text = ctx.client.post(f"/collections/{articles}/search", {"query": {"range": {"body": {"gte": "a"}}}})
    require(range_text.status == 400, f"range on analyzed text should be 400: {compact(range_text)}")
    sort_text = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"match_all": {}}, "sort": [{"author": "asc"}]},
    )
    require(sort_text.status == 400, f"sort on keyword/text should be 400: {compact(sort_text)}")
    phrase = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"match_phrase": {"body": "database storage"}}, "_source": ["title"], "size": 5},
    )
    require_2xx(phrase, "match_phrase search")
    hits = phrase.json.get("hits", {}).get("hits", [])
    require(len(hits) == 1 and hits[0].get("_id") == "e1", f"phrase search mismatch: {phrase.text}")
    ctx.add_evidence(
        "Collection search errors",
        f"missing={compact(no_mapping)} | unmapped={compact(unmapped)} | range={compact(range_text)} | sort={compact(sort_text)} | phrase={compact(phrase)}",
    )


def scenario_collection_search_pagination_sort_and_freshness(ctx: UatContext) -> None:
    articles = ctx.table("articles_features")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {
                "documents": [
                    {"_id": "s1", "title": "Alpha", "body": "database storage", "author": "ada", "year": 2020},
                    {"_id": "s2", "title": "Beta", "body": "database storage", "author": "lin", "year": 2021},
                    {"_id": "s3", "title": "Gamma", "body": "database storage", "author": "sam", "year": 2022},
                    {"_id": "s4", "title": "No year", "body": "database storage", "author": "zoe"},
                ]
            },
        ),
        "insert search feature docs before mapping",
    )
    mapping = ctx.client.post(
        f"/collections/{articles}/searchIndex",
        {
            "fields": {
                "title": {"analyzer": "standard"},
                "body": {"analyzer": "standard"},
                "author": {"type": "keyword"},
                "year": {"type": "integer"},
            }
        },
    )
    require_2xx(mapping, "declare feature mapping")
    require(mapping.json.get("backfilled") == 4, f"search mapping did not backfill existing docs: {mapping.text}")
    page = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"match_all": {}}, "sort": [{"year": "asc"}], "from": 1, "size": 2, "_source": ["title", "year"]},
    )
    require_2xx(page, "search pagination and numeric sort")
    hits = page.json.get("hits", {}).get("hits", [])
    require([hit.get("_id") for hit in hits] == ["s2", "s3"], f"search page mismatch: {page.text}")
    bare_sort = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"match_all": {}}, "sort": ["year"], "size": 10, "_source": ["title", "year"]},
    )
    require_2xx(bare_sort, "search bare field sort")
    bare_ids = [hit.get("_id") for hit in bare_sort.json.get("hits", {}).get("hits", [])]
    require(bare_ids[:3] == ["s1", "s2", "s3"] and bare_ids[-1] == "s4", f"bare sort/missing-last mismatch: {bare_sort.text}")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/update",
            {"filter": {"_id": "s1"}, "update": {"$set": {"body": "archived topic"}}},
        ),
        "search-indexed doc update",
    )
    after_update = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"match": {"body": {"query": "database storage", "operator": "and"}}}, "size": 10},
    )
    require_2xx(after_update, "search after collection update")
    require("s1" not in [hit.get("_id") for hit in after_update.json.get("hits", {}).get("hits", [])], f"updated doc still matched stale search index: {after_update.text}")
    require_2xx(ctx.client.post(f"/collections/{articles}/delete", {"filter": {"_id": "s2"}}), "search-indexed doc delete")
    after_delete = ctx.client.post(f"/collections/{articles}/search", {"query": {"term": {"author": "lin"}}, "size": 10})
    require_2xx(after_delete, "search after collection delete")
    require(after_delete.json.get("hits", {}).get("total", {}).get("value") == 0, f"deleted doc still matched search index: {after_delete.text}")
    ctx.add_evidence("Collection search features", f"{compact(mapping)} | {compact(page)} | {compact(bare_sort)} | {compact(after_update)} | {compact(after_delete)}")


def scenario_collection_search_term_range_exists_match_all(ctx: UatContext) -> None:
    articles = ctx.table("articles_exact")
    mapping = ctx.client.post(
        f"/collections/{articles}/searchIndex",
        {
            "fields": {
                "title": {"analyzer": "standard"},
                "body": {"analyzer": "standard"},
                "author": {"type": "keyword"},
                "year": {"type": "integer"},
            }
        },
    )
    require_2xx(mapping, "declare exact search mapping")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {
                "documents": [
                    {"_id": "e1", "title": "Alpha", "body": "storage database", "author": "ada", "year": 2020},
                    {"_id": "e2", "title": "Beta", "body": "ledger transfer", "author": "lin", "year": 2022},
                    {"_id": "e3", "title": "Gamma", "body": "database catalog", "year": 2024},
                ]
            },
        ),
        "insert exact search docs",
    )
    match_all = ctx.client.post(f"/collections/{articles}/search", {"query": {"match_all": {}}, "size": 10})
    require_2xx(match_all, "collection search match_all")
    require(match_all.json.get("hits", {}).get("total", {}).get("value") == 3, f"match_all total mismatch: {match_all.text}")
    term = ctx.client.post(f"/collections/{articles}/search", {"query": {"term": {"author": "ada"}}, "size": 10})
    require_2xx(term, "collection search term keyword")
    require([hit.get("_id") for hit in term.json.get("hits", {}).get("hits", [])] == ["e1"], f"term keyword mismatch: {term.text}")
    ranged = ctx.client.post(
        f"/collections/{articles}/search",
        {"query": {"range": {"year": {"gte": 2021, "lte": 2024}}}, "sort": [{"year": "asc"}], "size": 10},
    )
    require_2xx(ranged, "collection search integer range")
    require([hit.get("_id") for hit in ranged.json.get("hits", {}).get("hits", [])] == ["e2", "e3"], f"range search mismatch: {ranged.text}")
    exists = ctx.client.post(f"/collections/{articles}/search", {"query": {"exists": {"field": "author"}}, "size": 10})
    require_2xx(exists, "collection search exists")
    require(exists.json.get("hits", {}).get("total", {}).get("value") == 2, f"exists search mismatch: {exists.text}")
    ctx.add_evidence("Search term/range/exists/match_all", f"{compact(match_all)} | {compact(term)} | {compact(ranged)} | {compact(exists)}")


def scenario_collection_search_mapping_replacement_and_identifier_errors(ctx: UatContext) -> None:
    articles = ctx.table("articles_replace")
    require_2xx(
        ctx.client.post(
            f"/collections/{articles}/insert",
            {"documents": [{"_id": "m1", "title": "Searchable title", "body": "old body terms"}]},
        ),
        "insert mapping replacement docs",
    )
    first_mapping = ctx.client.post(f"/collections/{articles}/searchIndex", {"fields": {"body": {"analyzer": "standard"}}})
    require_2xx(first_mapping, "declare initial body mapping")
    body_hit = ctx.client.post(f"/collections/{articles}/search", {"query": {"match": {"body": "body"}}, "size": 5})
    require_2xx(body_hit, "search initial body mapping")
    require(body_hit.json.get("hits", {}).get("total", {}).get("value") == 1, f"initial body mapping mismatch: {body_hit.text}")
    replaced = ctx.client.post(f"/collections/{articles}/searchIndex", {"fields": {"title": {"analyzer": "standard"}}})
    require_2xx(replaced, "replace search mapping")
    described = ctx.client.get(f"/collections/{articles}/searchIndex")
    require_2xx(described, "describe replaced search mapping")
    require("title" in described.text and "body" not in json.dumps(described.json.get(articles, {}).get("mappings", {})), f"replacement mapping mismatch: {described.text}")
    body_unmapped = ctx.client.post(f"/collections/{articles}/search", {"query": {"match": {"body": "body"}}})
    require(body_unmapped.status == 400, f"old body field should be unmapped after replacement: {compact(body_unmapped)}")
    title_hit = ctx.client.post(f"/collections/{articles}/search", {"query": {"match": {"title": "Searchable"}}, "size": 5})
    require_2xx(title_hit, "search replaced title mapping")
    require(title_hit.json.get("hits", {}).get("total", {}).get("value") == 1, f"title mapping search mismatch: {title_hit.text}")
    bad_search = ctx.client.post("/collections/1bad/search", {"query": {"match_all": {}}})
    require(bad_search.status == 400, f"invalid collection search name should be 400: {compact(bad_search)}")
    bad_mapping = ctx.client.post("/collections/1bad/searchIndex", {"fields": {"body": {"analyzer": "standard"}}})
    require(bad_mapping.status == 400, f"invalid collection searchIndex name should be 400: {compact(bad_mapping)}")
    ctx.add_evidence("Search mapping replacement/identifier errors", f"{compact(first_mapping)} | initial={compact(body_hit)} | replaced={compact(replaced)} | described={compact(described)} | old={compact(body_unmapped)} | title={compact(title_hit)} | bad={compact(bad_search)}")


def scenario_collection_search_tenant_isolation(ctx: UatContext) -> None:
    coll = ctx.table("search_tenant")
    t1 = {"X-Bluedb-Tenant": "t1"}
    t2 = {"X-Bluedb-Tenant": "t2"}
    mapping = {"fields": {"body": {"analyzer": "standard"}, "tag": {"type": "keyword"}}}
    require_2xx(ctx.client.post(f"/collections/{coll}/searchIndex", mapping, t1), "tenant t1 search mapping")
    require_2xx(
        ctx.client.post(f"/collections/{coll}/insert", {"documents": [{"_id": "s1", "body": "tenant-only document", "tag": "private"}]}, t1),
        "tenant t1 search insert",
    )
    t1_search = ctx.client.post(f"/collections/{coll}/search", {"query": {"match": {"body": "tenant-only"}}, "size": 5}, t1)
    require_2xx(t1_search, "tenant t1 search")
    require(t1_search.json.get("hits", {}).get("total", {}).get("value") == 1, f"tenant t1 search mismatch: {t1_search.text}")
    t2_missing = ctx.client.post(f"/collections/{coll}/search", {"query": {"match": {"body": "tenant-only"}}}, t2)
    require(t2_missing.status == 404, f"tenant t2 should not see t1 mapping: {compact(t2_missing)}")
    t2_mapping = ctx.client.post(f"/collections/{coll}/searchIndex", mapping, t2)
    require_2xx(t2_mapping, "tenant t2 search mapping")
    require(t2_mapping.json.get("backfilled") == 0, f"tenant t2 should not backfill t1 docs: {t2_mapping.text}")
    t2_search = ctx.client.post(f"/collections/{coll}/search", {"query": {"match": {"body": "tenant-only"}}, "size": 5}, t2)
    require_2xx(t2_search, "tenant t2 search after own mapping")
    require(t2_search.json.get("hits", {}).get("total", {}).get("value") == 0, f"tenant t2 search leaked t1 docs: {t2_search.text}")
    ctx.add_evidence("Search tenant isolation", f"t1={compact(t1_search)} | t2_missing={compact(t2_missing)} | t2_mapping={compact(t2_mapping)} | t2={compact(t2_search)}")


def scenario_collection_search_phrase_highlight_and_source_controls(ctx: UatContext) -> None:
    coll = ctx.table("search_phrase")
    mapping = {"fields": {"body": {"analyzer": "standard"}, "title": {"analyzer": "standard"}}}
    require_2xx(ctx.client.post(f"/collections/{coll}/searchIndex", mapping), "phrase search mapping")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {
                "documents": [
                    {"_id": "p1", "title": "Quick", "body": "the quick brown fox jumps"},
                    {"_id": "p2", "title": "Reverse", "body": "the brown quick fox waits"},
                ]
            },
        ),
        "insert phrase search docs",
    )
    phrase = ctx.client.post(
        f"/collections/{coll}/search",
        {
            "query": {"match_phrase": {"body": "quick brown"}},
            "highlight": {"fields": {"body": {}}},
            "_source": ["body"],
            "size": 5,
        },
    )
    require_2xx(phrase, "collection match_phrase search")
    hits = phrase.json.get("hits", {}).get("hits", [])
    require([hit.get("_id") for hit in hits] == ["p1"], f"phrase search mismatch: {phrase.text}")
    require("<em>" in json.dumps(hits[0].get("highlight", {})), f"phrase highlight missing emphasis: {phrase.text}")
    require(set(hits[0].get("_source", {}).keys()) == {"body"}, f"source field-list mismatch: {phrase.text}")

    reversed_phrase = ctx.client.post(f"/collections/{coll}/search", {"query": {"match_phrase": {"body": "brown quick"}}, "size": 5})
    require_2xx(reversed_phrase, "collection reversed match_phrase search")
    require(reversed_phrase.json.get("hits", {}).get("total", {}).get("value") == 1, f"reversed phrase should match only p2: {reversed_phrase.text}")

    source_false = ctx.client.post(f"/collections/{coll}/search", {"query": {"match": {"body": "quick"}}, "_source": False, "size": 5})
    require_2xx(source_false, "collection search _source=false")
    require(all("_source" not in hit for hit in source_false.json.get("hits", {}).get("hits", [])), f"_source=false leaked source: {source_false.text}")
    ctx.add_evidence("Collection search phrase/highlight/source", f"{compact(phrase)} | reversed={compact(reversed_phrase)} | source_false={compact(source_false)}")


def scenario_collection_search_bool_should_and_unsupported_queries(ctx: UatContext) -> None:
    coll = ctx.table("search_bool_should")
    mapping = {"fields": {"body": {"analyzer": "standard"}, "tag": {"type": "keyword"}}}
    require_2xx(ctx.client.post(f"/collections/{coll}/searchIndex", mapping), "bool-should search mapping")
    require_2xx(
        ctx.client.post(
            f"/collections/{coll}/insert",
            {
                "documents": [
                    {"_id": "b1", "body": "cats database", "tag": "pet"},
                    {"_id": "b2", "body": "dogs database", "tag": "pet"},
                    {"_id": "b3", "body": "invoice approval", "tag": "finance"},
                ]
            },
        ),
        "insert bool-should search docs",
    )
    should = ctx.client.post(
        f"/collections/{coll}/search",
        {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"body": "cats"}},
                        {"term": {"tag": "finance"}},
                    ]
                }
            },
            "size": 10,
        },
    )
    require_2xx(should, "collection search bool should")
    require({hit.get("_id") for hit in should.json.get("hits", {}).get("hits", [])} == {"b1", "b3"}, f"bool should mismatch: {should.text}")

    multi_match = ctx.client.post(f"/collections/{coll}/search", {"query": {"multi_match": {"query": "cats", "fields": ["body"]}}})
    require_error(multi_match, "unsupported multi_match search query")
    query_string = ctx.client.post(f"/collections/{coll}/search", {"query": {"query_string": {"query": "body:cats"}}})
    require_error(query_string, "unsupported query_string search query")
    nested = ctx.client.post(f"/collections/{coll}/search", {"query": {"nested": {"path": "items", "query": {"match_all": {}}}}})
    require_error(nested, "unsupported nested search query")
    ctx.add_evidence("Collection search bool should/unsupported queries", f"{compact(should)} | multi={compact(multi_match)} | query_string={compact(query_string)} | nested={compact(nested)}")


def scenario_ledger_workflow(ctx: UatContext) -> None:
    ledger = 700 + (ctx.run_id % 1000)
    account_a = str(10_000_000_000_000 + ctx.run_id)
    account_b = str(20_000_000_000_000 + ctx.run_id)
    transfer_id = str(30_000_000_000_000 + ctx.run_id)
    accounts = ctx.client.post(
        "/ledger/accounts",
        [
            {"id": account_a, "ledger": ledger, "code": 10},
            {"id": account_b, "ledger": ledger, "code": 10},
        ],
    )
    require_2xx(accounts, "create ledger accounts")
    require([r.get("result") for r in accounts.json.get("results", [])] == ["created", "created"], accounts.text)

    transfer = ctx.client.post(
        "/ledger/transfers",
        {
            "id": transfer_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "amount": "100",
            "ledger": ledger,
            "code": 1,
        },
    )
    require_2xx(transfer, "create ledger transfer")
    require(transfer.json.get("results", [{}])[0].get("result") == "created", transfer.text)

    debit = ctx.client.get(f"/ledger/accounts/{account_a}")
    credit = ctx.client.get(f"/ledger/accounts/{account_b}")
    require_2xx(debit, "read debit account")
    require_2xx(credit, "read credit account")
    require(debit.json.get("debits_posted") == "100", f"debit mismatch: {debit.text}")
    require(credit.json.get("credits_posted") == "100", f"credit mismatch: {credit.text}")

    projection = ctx.client.post("/sql", {"sql": "SELECT amount FROM ledger_transfers WHERE id = $1", "params": [transfer_id]})
    require_2xx(projection, "ledger SQL projection")
    require(rows(projection.json)[0].get("amount") == "100", f"projection mismatch: {projection.text}")
    ctx.add_evidence("Ledger transfer and balances", f"{compact(transfer)} | debit={compact(debit)} | credit={compact(credit)}")
    ctx.add_evidence("SQL projection", compact(projection))


def scenario_ledger_duplicate_and_missing_workflow(ctx: UatContext) -> None:
    ledger = 1_700 + (ctx.run_id % 1000)
    account_a = str(40_000_000_000_000 + ctx.run_id)
    account_b = str(50_000_000_000_000 + ctx.run_id)
    transfer_id = str(60_000_000_000_000 + ctx.run_id)
    created = ctx.client.post(
        "/ledger/accounts",
        [{"id": account_a, "ledger": ledger, "code": 10}, {"id": account_b, "ledger": ledger, "code": 10}],
    )
    require_2xx(created, "ledger duplicate setup")
    replay = ctx.client.post("/ledger/accounts", {"id": account_a, "ledger": ledger, "code": 10})
    require_2xx(replay, "ledger account duplicate replay")
    require(replay.json.get("results", [{}])[0].get("result") == "exists", f"expected exists duplicate code: {replay.text}")
    transfer = ctx.client.post(
        "/ledger/transfers",
        {
            "id": transfer_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "amount": "25",
            "ledger": ledger,
            "code": 1,
        },
    )
    require_2xx(transfer, "ledger duplicate transfer setup")
    transfer_replay = ctx.client.post(
        "/ledger/transfers",
        {
            "id": transfer_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "amount": "25",
            "ledger": ledger,
            "code": 1,
        },
    )
    require_2xx(transfer_replay, "ledger transfer duplicate replay")
    require(transfer_replay.json.get("results", [{}])[0].get("result") == "exists", f"expected transfer exists: {transfer_replay.text}")
    missing = ctx.client.get(f"/ledger/accounts/{90_000_000_000_000 + ctx.run_id}")
    require(missing.status == 404, f"missing account should be 404: {compact(missing)}")
    ctx.add_evidence("Ledger duplicate/missing behavior", f"{compact(replay)} | {compact(transfer_replay)} | {compact(missing)}")


def scenario_ledger_two_phase_transfer_workflow(ctx: UatContext) -> None:
    ledger = 2_700 + (ctx.run_id % 1000)
    account_a = str(70_000_000_000_000 + ctx.run_id)
    account_b = str(80_000_000_000_000 + ctx.run_id)
    pending_id = str(81_000_000_000_000 + ctx.run_id)
    post_id = str(82_000_000_000_000 + ctx.run_id)
    pending_void_id = str(83_000_000_000_000 + ctx.run_id)
    void_id = str(84_000_000_000_000 + ctx.run_id)
    pending = 1 << 1
    post_pending = 1 << 2
    void_pending = 1 << 3
    require_2xx(
        ctx.client.post("/ledger/accounts", [{"id": account_a, "ledger": ledger, "code": 10}, {"id": account_b, "ledger": ledger, "code": 10}]),
        "ledger two-phase accounts",
    )
    reserved = ctx.client.post(
        "/ledger/transfers",
        {"id": pending_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "100", "ledger": ledger, "code": 1, "flags": pending},
    )
    require_2xx(reserved, "pending transfer")
    require(reserved.json.get("results", [{}])[0].get("result") == "created", f"pending result mismatch: {reserved.text}")
    debit_pending = ctx.client.get(f"/ledger/accounts/{account_a}")
    credit_pending = ctx.client.get(f"/ledger/accounts/{account_b}")
    require_2xx(debit_pending, "pending debit account")
    require(debit_pending.json.get("debits_pending") == "100" and credit_pending.json.get("credits_pending") == "100", f"pending balances mismatch: {debit_pending.text} | {credit_pending.text}")
    posted = ctx.client.post(
        "/ledger/transfers",
        {
            "id": post_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "pending_id": pending_id,
            "amount": "60",
            "ledger": ledger,
            "code": 1,
            "flags": post_pending,
        },
    )
    require_2xx(posted, "post pending transfer")
    require(posted.json.get("results", [{}])[0].get("result") == "created", f"post pending mismatch: {posted.text}")
    after_post = ctx.client.get(f"/ledger/accounts/{account_a}")
    require_2xx(after_post, "after post account")
    require(after_post.json.get("debits_pending") == "0" and after_post.json.get("debits_posted") == "60", f"post balances mismatch: {after_post.text}")

    reserved_void = ctx.client.post(
        "/ledger/transfers",
        {"id": pending_void_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "25", "ledger": ledger, "code": 1, "flags": pending},
    )
    require_2xx(reserved_void, "pending transfer to void")
    voided = ctx.client.post(
        "/ledger/transfers",
        {
            "id": void_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "pending_id": pending_void_id,
            "amount": "0",
            "ledger": ledger,
            "code": 1,
            "flags": void_pending,
        },
    )
    require_2xx(voided, "void pending transfer")
    require(voided.json.get("results", [{}])[0].get("result") == "created", f"void pending mismatch: {voided.text}")
    after_void = ctx.client.get(f"/ledger/accounts/{account_a}")
    require_2xx(after_void, "after void account")
    require(after_void.json.get("debits_pending") == "0" and after_void.json.get("debits_posted") == "60", f"void balances mismatch: {after_void.text}")
    projection = ctx.client.post("/sql", {"sql": "SELECT debits_pending, debits_posted FROM ledger_accounts WHERE id = $1", "params": [account_a]})
    require_2xx(projection, "two-phase ledger SQL projection")
    require(rows(projection.json)[0].get("debits_posted") == "60", f"two-phase projection mismatch: {projection.text}")
    ctx.add_evidence("Ledger two-phase", f"{compact(reserved)} | pending={compact(debit_pending)} | {compact(posted)} | {compact(voided)} | projection={compact(projection)}")


def scenario_ledger_linked_chain_and_constraint_errors(ctx: UatContext) -> None:
    ledger = 3_700 + (ctx.run_id % 1000)
    account_a = str(85_000_000_000_000 + ctx.run_id)
    account_b = str(86_000_000_000_000 + ctx.run_id)
    linked_id = str(87_000_000_000_000 + ctx.run_id)
    bad_id = str(88_000_000_000_000 + ctx.run_id)
    constrained = str(89_000_000_000_000 + ctx.run_id)
    constraint_xfer = str(91_000_000_000_000 + ctx.run_id)
    linked = 1 << 0
    debits_must_not_exceed_credits = 1 << 1
    require_2xx(
        ctx.client.post(
            "/ledger/accounts",
            [
                {"id": account_a, "ledger": ledger, "code": 10},
                {"id": account_b, "ledger": ledger, "code": 10},
                {"id": constrained, "ledger": ledger, "code": 10, "flags": debits_must_not_exceed_credits},
            ],
        ),
        "linked ledger accounts",
    )
    linked_batch = ctx.client.post(
        "/ledger/transfers",
        [
            {"id": linked_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "10", "ledger": ledger, "code": 1, "flags": linked},
            {"id": bad_id, "debit_account_id": account_a, "credit_account_id": str(99_000_000_000_000 + ctx.run_id), "amount": "5", "ledger": ledger, "code": 1},
        ],
    )
    require_2xx(linked_batch, "linked transfer batch")
    results = [item.get("result") for item in linked_batch.json.get("results", [])]
    require(results == ["linked_event_failed", "credit_account_not_found"], f"linked result mismatch: {linked_batch.text}")
    account_after = ctx.client.get(f"/ledger/accounts/{account_a}")
    require_2xx(account_after, "linked rollback account")
    require(account_after.json.get("debits_posted") == "0", f"linked chain should roll back valid member: {account_after.text}")
    constrained_transfer = ctx.client.post(
        "/ledger/transfers",
        {"id": constraint_xfer, "debit_account_id": constrained, "credit_account_id": account_b, "amount": "1", "ledger": ledger, "code": 1},
    )
    require_2xx(constrained_transfer, "constraint transfer result")
    require(constrained_transfer.json.get("results", [{}])[0].get("result") == "exceeds_credits", f"constraint code mismatch: {constrained_transfer.text}")
    ctx.add_evidence("Ledger linked/constraints", f"{compact(linked_batch)} | account={compact(account_after)} | constraint={compact(constrained_transfer)}")


def scenario_ledger_transfer_lookup_and_query_projection(ctx: UatContext) -> None:
    ledger = 4_700 + (ctx.run_id % 1000)
    account_a = str(100_000_000_000_000 + ctx.run_id)
    account_b = str(101_000_000_000_000 + ctx.run_id)
    transfer_id = str(102_000_000_000_000 + ctx.run_id)
    require_2xx(
        ctx.client.post("/ledger/accounts", [{"id": account_a, "ledger": ledger, "code": 10}, {"id": account_b, "ledger": ledger, "code": 10}]),
        "ledger lookup accounts",
    )
    created = ctx.client.post(
        "/ledger/transfers",
        {"id": transfer_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "77", "ledger": ledger, "code": 7},
    )
    require_2xx(created, "ledger lookup transfer")
    require(created.json.get("results", [{}])[0].get("result") == "created", f"transfer creation mismatch: {created.text}")
    lookup = ctx.client.get(f"/ledger/transfers/{transfer_id}")
    require_2xx(lookup, "GET ledger transfer")
    require(
        lookup.json.get("id") == transfer_id and lookup.json.get("amount") == "77" and lookup.json.get("debit_account_id") == account_a,
        f"transfer lookup mismatch: {lookup.text}",
    )
    query_projection = ctx.client.post(
        "/query",
        {"sql": "SELECT id, debit_account_id, credit_account_id, amount FROM ledger_transfers WHERE ledger = $1", "params": [ledger]},
    )
    require_2xx(query_projection, "ledger transfer /query projection")
    projection_rows = rows(query_projection.json)
    require(len(projection_rows) == 1 and projection_rows[0].get("id") == transfer_id and projection_rows[0].get("amount") == "77", f"ledger /query projection mismatch: {query_projection.text}")
    missing_transfer = ctx.client.get(f"/ledger/transfers/{103_000_000_000_000 + ctx.run_id}")
    require(missing_transfer.status == 404, f"missing transfer lookup should be 404: {compact(missing_transfer)}")
    ctx.add_evidence("Ledger transfer lookup/query", f"{compact(created)} | lookup={compact(lookup)} | query={compact(query_projection)} | missing={compact(missing_transfer)}")


def scenario_ledger_pending_error_codes(ctx: UatContext) -> None:
    ledger = 5_700 + (ctx.run_id % 1000)
    account_a = str(104_000_000_000_000 + ctx.run_id)
    account_b = str(105_000_000_000_000 + ctx.run_id)
    missing_post_id = str(106_000_000_000_000 + ctx.run_id)
    missing_pending_id = str(107_000_000_000_000 + ctx.run_id)
    pending_id = str(108_000_000_000_000 + ctx.run_id)
    post_id = str(109_000_000_000_000 + ctx.run_id)
    second_post_id = str(110_000_000_000_000 + ctx.run_id)
    pending = 1 << 1
    post_pending = 1 << 2
    require_2xx(
        ctx.client.post("/ledger/accounts", [{"id": account_a, "ledger": ledger, "code": 10}, {"id": account_b, "ledger": ledger, "code": 10}]),
        "ledger pending error accounts",
    )
    missing = ctx.client.post(
        "/ledger/transfers",
        {
            "id": missing_post_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "pending_id": missing_pending_id,
            "amount": "1",
            "ledger": ledger,
            "code": 1,
            "flags": post_pending,
        },
    )
    require_2xx(missing, "post missing pending transfer")
    require(missing.json.get("results", [{}])[0].get("result") == "pending_transfer_not_found", f"missing pending result mismatch: {missing.text}")
    reserved = ctx.client.post(
        "/ledger/transfers",
        {"id": pending_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "40", "ledger": ledger, "code": 1, "flags": pending},
    )
    require_2xx(reserved, "reserve pending for already-posted test")
    posted = ctx.client.post(
        "/ledger/transfers",
        {
            "id": post_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "pending_id": pending_id,
            "amount": "40",
            "ledger": ledger,
            "code": 1,
            "flags": post_pending,
        },
    )
    require_2xx(posted, "post pending transfer once")
    require(posted.json.get("results", [{}])[0].get("result") == "created", f"post pending should succeed: {posted.text}")
    already = ctx.client.post(
        "/ledger/transfers",
        {
            "id": second_post_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "pending_id": pending_id,
            "amount": "1",
            "ledger": ledger,
            "code": 1,
            "flags": post_pending,
        },
    )
    require_2xx(already, "post already-posted pending transfer")
    require(already.json.get("results", [{}])[0].get("result") == "pending_transfer_already_posted", f"already-posted result mismatch: {already.text}")
    ctx.add_evidence("Ledger pending error codes", f"missing={compact(missing)} | reserved={compact(reserved)} | posted={compact(posted)} | already={compact(already)}")


def scenario_ledger_credits_must_not_exceed_debits(ctx: UatContext) -> None:
    ledger = 6_700 + (ctx.run_id % 1000)
    account_a = str(111_000_000_000_000 + ctx.run_id)
    constrained_credit = str(112_000_000_000_000 + ctx.run_id)
    transfer_id = str(113_000_000_000_000 + ctx.run_id)
    credits_must_not_exceed_debits = 1 << 2
    require_2xx(
        ctx.client.post(
            "/ledger/accounts",
            [
                {"id": account_a, "ledger": ledger, "code": 10},
                {"id": constrained_credit, "ledger": ledger, "code": 10, "flags": credits_must_not_exceed_debits},
            ],
        ),
        "ledger credits constraint accounts",
    )
    rejected = ctx.client.post(
        "/ledger/transfers",
        {"id": transfer_id, "debit_account_id": account_a, "credit_account_id": constrained_credit, "amount": "1", "ledger": ledger, "code": 1},
    )
    require_2xx(rejected, "credits_must_not_exceed_debits transfer")
    require(rejected.json.get("results", [{}])[0].get("result") == "exceeds_debits", f"credits constraint result mismatch: {rejected.text}")
    constrained = ctx.client.get(f"/ledger/accounts/{constrained_credit}")
    require_2xx(constrained, "read constrained credit account")
    require(constrained.json.get("credits_posted") == "0", f"rejected transfer should not update constrained account: {constrained.text}")
    ctx.add_evidence("Ledger credits constraint", f"{compact(rejected)} | account={compact(constrained)}")


def scenario_ledger_user_data_roundtrip_and_sql_projection(ctx: UatContext) -> None:
    ledger = 7_700 + (ctx.run_id % 1000)
    account_a = str(114_000_000_000_000 + ctx.run_id)
    account_b = str(115_000_000_000_000 + ctx.run_id)
    transfer_id = str(116_000_000_000_000 + ctx.run_id)
    account_ud128 = "340282366920938463463374607431768211455"
    account_ud64 = "18446744073709551615"
    transfer_ud128 = "170141183460469231731687303715884105727"
    transfer_ud64 = "9007199254740993"
    accounts = ctx.client.post(
        "/ledger/accounts",
        [
            {
                "id": account_a,
                "ledger": ledger,
                "code": 10,
                "user_data_128": account_ud128,
                "user_data_64": account_ud64,
                "user_data_32": 42,
            },
            {"id": account_b, "ledger": ledger, "code": 10},
        ],
    )
    require_2xx(accounts, "ledger user_data accounts")
    canonical_account = ctx.client.get(f"/ledger/accounts/{account_a}")
    require_2xx(canonical_account, "ledger account user_data lookup")
    require(canonical_account.json.get("user_data_128") == account_ud128, f"account user_data_128 mismatch: {canonical_account.text}")
    require(canonical_account.json.get("user_data_64") == account_ud64, f"account user_data_64 mismatch: {canonical_account.text}")
    require(canonical_account.json.get("user_data_32") == 42, f"account user_data_32 mismatch: {canonical_account.text}")

    transfer = ctx.client.post(
        "/ledger/transfers",
        {
            "id": transfer_id,
            "debit_account_id": account_a,
            "credit_account_id": account_b,
            "amount": "12",
            "ledger": ledger,
            "code": 12,
            "user_data_128": transfer_ud128,
            "user_data_64": transfer_ud64,
            "user_data_32": 7,
        },
    )
    require_2xx(transfer, "ledger transfer user_data create")
    require(transfer.json.get("results", [{}])[0].get("result") == "created", f"transfer user_data result mismatch: {transfer.text}")
    canonical_transfer = ctx.client.get(f"/ledger/transfers/{transfer_id}")
    require_2xx(canonical_transfer, "ledger transfer user_data lookup")
    require(canonical_transfer.json.get("user_data_128") == transfer_ud128, f"transfer user_data_128 mismatch: {canonical_transfer.text}")
    require(canonical_transfer.json.get("user_data_64") == transfer_ud64, f"transfer user_data_64 mismatch: {canonical_transfer.text}")
    require(canonical_transfer.json.get("user_data_32") == 7, f"transfer user_data_32 mismatch: {canonical_transfer.text}")

    account_sql = ctx.client.post(
        "/sql",
        {"sql": "SELECT user_data_128, user_data_64, user_data_32 FROM ledger_accounts WHERE id = $1", "params": [account_a]},
    )
    require_2xx(account_sql, "ledger account user_data SQL projection")
    require(rows(account_sql.json)[0] == {"user_data_128": account_ud128, "user_data_64": account_ud64, "user_data_32": 42}, f"account SQL user_data mismatch: {account_sql.text}")
    transfer_sql = ctx.client.post(
        "/sql",
        {"sql": "SELECT user_data_128, user_data_64, user_data_32 FROM ledger_transfers WHERE id = $1", "params": [transfer_id]},
    )
    require_2xx(transfer_sql, "ledger transfer user_data SQL projection")
    require(rows(transfer_sql.json)[0] == {"user_data_128": transfer_ud128, "user_data_64": transfer_ud64, "user_data_32": 7}, f"transfer SQL user_data mismatch: {transfer_sql.text}")
    ctx.add_evidence("Ledger user_data canonical/SQL roundtrip", f"account={compact(canonical_account)} | transfer={compact(canonical_transfer)} | account_sql={compact(account_sql)} | transfer_sql={compact(transfer_sql)}")


def scenario_ledger_void_already_voided_error(ctx: UatContext) -> None:
    ledger = 8_700 + (ctx.run_id % 1000)
    account_a = str(117_000_000_000_000 + ctx.run_id)
    account_b = str(118_000_000_000_000 + ctx.run_id)
    pending_id = str(119_000_000_000_000 + ctx.run_id)
    void_id = str(120_000_000_000_000 + ctx.run_id)
    second_void_id = str(121_000_000_000_000 + ctx.run_id)
    pending = 1 << 1
    void_pending = 1 << 3
    require_2xx(ctx.client.post("/ledger/accounts", [{"id": account_a, "ledger": ledger, "code": 10}, {"id": account_b, "ledger": ledger, "code": 10}]), "ledger void accounts")
    reserved = ctx.client.post(
        "/ledger/transfers",
        {"id": pending_id, "debit_account_id": account_a, "credit_account_id": account_b, "amount": "18", "ledger": ledger, "code": 1, "flags": pending},
    )
    require_2xx(reserved, "reserve pending to void")
    first_void = ctx.client.post(
        "/ledger/transfers",
        {"id": void_id, "debit_account_id": account_a, "credit_account_id": account_b, "pending_id": pending_id, "amount": "0", "ledger": ledger, "code": 1, "flags": void_pending},
    )
    require_2xx(first_void, "first void pending")
    require(first_void.json.get("results", [{}])[0].get("result") == "created", f"first void mismatch: {first_void.text}")
    second_void = ctx.client.post(
        "/ledger/transfers",
        {"id": second_void_id, "debit_account_id": account_a, "credit_account_id": account_b, "pending_id": pending_id, "amount": "0", "ledger": ledger, "code": 1, "flags": void_pending},
    )
    require_2xx(second_void, "second void pending")
    require(second_void.json.get("results", [{}])[0].get("result") == "pending_transfer_already_voided", f"already-voided result mismatch: {second_void.text}")
    account = ctx.client.get(f"/ledger/accounts/{account_a}")
    require_2xx(account, "account after double void")
    require(account.json.get("debits_pending") == "0" and account.json.get("debits_posted") == "0", f"double void changed balances: {account.text}")
    ctx.add_evidence("Ledger already-voided pending", f"reserve={compact(reserved)} | first={compact(first_void)} | second={compact(second_void)} | account={compact(account)}")


def scenario_evidence_workflow(ctx: UatContext) -> None:
    chain = ctx.table("audit")
    created = ctx.client.put(f"/evidence/{chain}", {"verified": True})
    require_2xx(created, "create verified evidence chain")
    payload = base64.b64encode(b'{"user":"a"}').decode("ascii")
    appended = ctx.client.post(
        f"/evidence/{chain}/entries",
        {"events": [{"type": "access.granted", "payload_b64": payload}], "idem_key": "idem-1"},
    )
    require_2xx(appended, "append evidence")
    require(appended.json.get("base_seq") == 0 and appended.json.get("seqs") == [1], f"append mismatch: {appended.text}")
    replay = ctx.client.post(
        f"/evidence/{chain}/entries",
        {"events": [{"type": "access.granted", "payload_b64": payload}], "idem_key": "idem-1"},
    )
    require_2xx(replay, "idempotent replay")
    require(replay.json == appended.json, f"idempotent replay changed result: {replay.text}")

    digest = ctx.client.get(f"/evidence/{chain}/digest")
    require_2xx(digest, "digest")
    require(digest.json.get("size") == 1 and len(digest.json.get("root_hash", "")) == 64, f"digest mismatch: {digest.text}")
    proof = ctx.client.get(f"/evidence/{chain}/proof?seq=1&size=1")
    require_2xx(proof, "proof")
    redacted = ctx.client.post(f"/evidence/{chain}/entries/1/redact")
    require_2xx(redacted, "redact")
    entries = ctx.client.get(f"/evidence/{chain}/entries?from=1&to=1")
    require_2xx(entries, "entries")
    require(rows(entries.json)[0].get("redacted") is True, f"entry was not redacted: {entries.text}")
    deleted = ctx.client.delete(f"/evidence/{chain}/entries/1")
    require(deleted.status == 409, f"verified hard-delete should be rejected: {compact(deleted)}")
    ctx.add_evidence("Append/replay/digest/proof", f"{compact(appended)} | {compact(replay)} | {compact(digest)} | {compact(proof)}")
    ctx.add_evidence("Redaction/delete guard", f"{compact(redacted)} | {compact(entries)} | {compact(deleted)}")


def scenario_evidence_plain_chain_workflow(ctx: UatContext) -> None:
    chain = ctx.table("plain")
    created = ctx.client.put(f"/evidence/{chain}", {"verified": False})
    require_2xx(created, "create plain evidence chain")
    payload = base64.b64encode(b"plain").decode("ascii")
    appended = ctx.client.post(
        f"/evidence/{chain}/entries",
        {"events": [{"type": "plain.one", "payload_b64": payload}, {"type": "plain.two", "payload_b64": payload}]},
    )
    require_2xx(appended, "append plain evidence")
    digest = ctx.client.get(f"/evidence/{chain}/digest")
    require(digest.status == 400, f"plain chain digest should fail: {compact(digest)}")
    deleted = ctx.client.delete(f"/evidence/{chain}/entries/1")
    require_2xx(deleted, "hard-delete plain chain entry")
    entries = ctx.client.get(f"/evidence/{chain}/entries?from=1&to=2")
    require_2xx(entries, "read plain entries after delete")
    require([entry.get("seq") for entry in rows(entries.json)] == [2], f"plain hard delete did not remove seq 1: {entries.text}")
    ctx.add_evidence("Plain chain behavior", f"{compact(created)} | {compact(digest)} | {compact(deleted)} | {compact(entries)}")


def scenario_evidence_edges_and_idempotency_conflict(ctx: UatContext) -> None:
    chain = ctx.table("edge_audit")
    graph = ctx.table("edge_graph")
    created = ctx.client.put(f"/evidence/{chain}", {"verified": True})
    require_2xx(created, "create edge evidence chain")
    payload = base64.b64encode(b'{"doc":"A"}').decode("ascii")
    append_body = {
        "events": [
            {
                "type": "lineage.linked",
                "payload_b64": payload,
                "edges": [{"graph": graph, "src": "doc:A", "dst": "doc:B", "weight": 5, "type": "derived"}],
            }
        ],
        "idem_key": "edge-idem-1",
    }
    appended = ctx.client.post(f"/evidence/{chain}/entries", append_body)
    require_2xx(appended, "append evidence with edge")
    require(appended.json.get("seqs") == [1], f"append-with-edge sequence mismatch: {appended.text}")

    reachable = ctx.client.post(f"/graph/{graph}/reachable", {"from": ["doc:A"], "floor": 1, "directed": True})
    require_2xx(reachable, "graph reachable after evidence append")
    require(reachable.json.get("nodes") == ["doc:A", "doc:B"], f"edge projection mismatch: {reachable.text}")

    replay = ctx.client.post(f"/evidence/{chain}/entries", append_body)
    require_2xx(replay, "append-with-edge idempotent replay")
    require(replay.json == appended.json, f"append-with-edge replay changed result: {replay.text}")
    conflict = ctx.client.post(
        f"/evidence/{chain}/entries",
        {
            "events": [
                {
                    "type": "lineage.linked",
                    "payload_b64": base64.b64encode(b'{"doc":"different"}').decode("ascii"),
                    "edges": [{"graph": graph, "src": "doc:A", "dst": "doc:C", "weight": 5, "type": "derived"}],
                }
            ],
            "idem_key": "edge-idem-1",
        },
    )
    require(conflict.status == 409, f"idempotency conflict should be 409: {compact(conflict)}")
    ctx.add_evidence(
        "Evidence append-with-edges",
        f"{compact(appended)} | reachable={compact(reachable)} | replay={compact(replay)} | conflict={compact(conflict)}",
    )


def scenario_evidence_merkle_range_and_consistency(ctx: UatContext) -> None:
    chain = ctx.table("merkle")
    for i in range(1, 6):
        payload = base64.b64encode(f"payload-{i}".encode("utf-8")).decode("ascii")
        appended = ctx.client.post(f"/evidence/{chain}/entries", {"events": [{"type": "merkle.event", "payload_b64": payload}]})
        require_2xx(appended, f"append merkle event {i}")
        require(appended.json.get("seqs") == [i], f"dense sequence mismatch at {i}: {appended.text}")
    head = ctx.client.get(f"/evidence/{chain}/head")
    require_2xx(head, "evidence head")
    require(head.json.get("seq") == 5, f"head mismatch: {head.text}")
    page = ctx.client.get(f"/evidence/{chain}/entries?after=2&limit=2")
    require_2xx(page, "evidence after/limit page")
    require([entry.get("seq") for entry in rows(page.json)] == [3, 4], f"page mismatch: {page.text}")
    digest_before = ctx.client.get(f"/evidence/{chain}/digest")
    require_2xx(digest_before, "evidence digest before redact")
    require(digest_before.json.get("size") == 5 and len(digest_before.json.get("root_hash", "")) == 64, f"digest mismatch: {digest_before.text}")
    proof = ctx.client.get(f"/evidence/{chain}/proof?seq=3")
    require_2xx(proof, "evidence inclusion proof")
    require(isinstance(proof.json.get("audit_path"), list) and proof.json.get("size") == 5, f"proof mismatch: {proof.text}")
    consistency = ctx.client.get(f"/evidence/{chain}/consistency?from=2&to=5")
    require_2xx(consistency, "evidence consistency proof")
    require(consistency.json.get("first") == 2 and consistency.json.get("second") == 5 and isinstance(consistency.json.get("proof"), list), f"consistency mismatch: {consistency.text}")
    redacted = ctx.client.post(f"/evidence/{chain}/entries/3/redact")
    require_2xx(redacted, "evidence redaction")
    digest_after = ctx.client.get(f"/evidence/{chain}/digest")
    require_2xx(digest_after, "evidence digest after redact")
    require(digest_after.json.get("root_hash") == digest_before.json.get("root_hash"), f"redaction changed verified digest: {digest_before.text} -> {digest_after.text}")
    entries = ctx.client.get(f"/evidence/{chain}/entries?from=3&to=3")
    require_2xx(entries, "redacted entry read")
    redacted_entry = rows(entries.json)[0]
    require(redacted_entry.get("redacted") is True and "payload_b64" not in redacted_entry, f"redacted entry mismatch: {entries.text}")
    ctx.add_evidence("Evidence Merkle/range/consistency", f"head={compact(head)} | page={compact(page)} | digest={compact(digest_before)} | proof={compact(proof)} | consistency={compact(consistency)} | redacted={compact(entries)}")


def scenario_evidence_mode_tenant_and_signing_contracts(ctx: UatContext) -> None:
    chain = ctx.table("contracts")
    payload = base64.b64encode(b"tenant").decode("ascii")
    auto = ctx.client.post(f"/evidence/{chain}/entries", {"events": [{"type": "auto.verified", "payload_b64": payload}]})
    require_2xx(auto, "auto-create verified chain")
    require(auto.json.get("base_seq") == 0 and auto.json.get("seqs") == [1], f"auto-create mismatch: {auto.text}")
    conflict = ctx.client.put(f"/evidence/{chain}", {"verified": False})
    require(conflict.status == 409, f"mode conflict should be 409: {compact(conflict)}")
    require("E_CHAIN_MODE_CONFLICT" in conflict.text, f"mode conflict code missing: {conflict.text}")
    signed = ctx.client.get(f"/evidence/{chain}/digest/signed")
    require(signed.status == 501, f"signed digest should be 501 when signing off: {compact(signed)}")
    signing_key = ctx.client.get("/evidence/signing-key")
    require(signing_key.status == 501, f"signing key should be 501 when signing off: {compact(signing_key)}")
    tenant_headers = {"X-Bluedb-Tenant": "acme"}
    tenant_append = ctx.client.post(
        f"/evidence/{chain}/entries",
        {"events": [{"type": "tenant.start", "payload_b64": payload}]},
        tenant_headers,
    )
    require_2xx(tenant_append, "tenant evidence append")
    require(tenant_append.json.get("base_seq") == 0 and tenant_append.json.get("seqs") == [1], f"tenant sequence should start at 1: {tenant_append.text}")
    default_head = ctx.client.get(f"/evidence/{chain}/head")
    tenant_head = ctx.client.get(f"/evidence/{chain}/head", tenant_headers)
    require_2xx(default_head, "default evidence head")
    require_2xx(tenant_head, "tenant evidence head")
    require(default_head.json.get("seq") == 1 and tenant_head.json.get("seq") == 1, f"tenant/default heads mismatch: {default_head.text} | {tenant_head.text}")
    ctx.add_evidence("Evidence mode/tenant/signing", f"auto={compact(auto)} | conflict={compact(conflict)} | signed={compact(signed)} | key={compact(signing_key)} | tenant={compact(tenant_append)}")


def scenario_evidence_named_errors_and_invalid_ranges(ctx: UatContext) -> None:
    plain = ctx.table("plain_errors")
    verified = ctx.table("verified_errors")
    payload = base64.b64encode(b"error").decode("ascii")
    require_2xx(ctx.client.put(f"/evidence/{plain}", {"verified": False}), "create plain error chain")
    require_2xx(ctx.client.post(f"/evidence/{plain}/entries", {"events": [{"type": "plain.error", "payload_b64": payload}]}), "append plain error chain")
    plain_digest = ctx.client.get(f"/evidence/{plain}/digest")
    require(plain_digest.status == 400 and "E_NOT_VERIFIED" in plain_digest.text, f"plain digest should return E_NOT_VERIFIED: {compact(plain_digest)}")
    plain_proof = ctx.client.get(f"/evidence/{plain}/proof?seq=1&size=1")
    require(plain_proof.status == 400 and "E_NOT_VERIFIED" in plain_proof.text, f"plain proof should return E_NOT_VERIFIED: {compact(plain_proof)}")
    ctx.add_evidence("Plain chain not-verified errors", f"digest={compact(plain_digest)} | proof={compact(plain_proof)}")
    require_2xx(ctx.client.put(f"/evidence/{verified}", {"verified": True}), "create verified error chain")
    require_2xx(ctx.client.post(f"/evidence/{verified}/entries", {"events": [{"type": "verified.error", "payload_b64": payload}]}), "append verified error chain")
    hard_delete = ctx.client.delete(f"/evidence/{verified}/entries/1")
    require(hard_delete.status == 409 and "E_VERIFIED_NO_DELETE" in hard_delete.text, f"verified delete should return E_VERIFIED_NO_DELETE: {compact(hard_delete)}")
    ctx.add_evidence("Verified delete guard", compact(hard_delete))
    unknown_proof = ctx.client.get(f"/evidence/{verified}/proof?seq=99")
    ctx.add_evidence("Out-of-range proof response", compact(unknown_proof))
    require(unknown_proof.status == 404, f"unknown proof seq should be 404: {compact(unknown_proof)}")
    missing_redact = ctx.client.post(f"/evidence/{verified}/entries/99/redact")
    require(missing_redact.status == 404, f"missing redact should be 404: {compact(missing_redact)}")
    bad_from = ctx.client.get(f"/evidence/{verified}/entries?from=-1&to=1")
    require(bad_from.status == 400, f"negative from should be rejected: {compact(bad_from)}")
    bad_to = ctx.client.get(f"/evidence/{verified}/entries?from=1&to=-1")
    require(bad_to.status == 400, f"negative to should be rejected: {compact(bad_to)}")
    ctx.add_evidence("Evidence named errors/ranges", f"plain_digest={compact(plain_digest)} | plain_proof={compact(plain_proof)} | delete={compact(hard_delete)} | proof404={compact(unknown_proof)} | redact404={compact(missing_redact)} | ranges={compact(bad_from)} / {compact(bad_to)}")


def scenario_evidence_batch_append_dense_ranges(ctx: UatContext) -> None:
    chain = ctx.table("batch_range")
    events = [
        {"type": "batch.one", "payload_b64": base64.b64encode(b"one").decode("ascii")},
        {"type": "batch.two", "payload_b64": base64.b64encode(b"two").decode("ascii")},
        {"type": "batch.three", "payload_b64": base64.b64encode(b"three").decode("ascii")},
    ]
    appended = ctx.client.post(f"/evidence/{chain}/entries", {"events": events, "idem_key": "batch-1"})
    require_2xx(appended, "batch append evidence")
    require(appended.json.get("base_seq") == 0 and appended.json.get("seqs") == [1, 2, 3], f"batch append sequence mismatch: {appended.text}")
    replay = ctx.client.post(f"/evidence/{chain}/entries", {"events": events, "idem_key": "batch-1"})
    require_2xx(replay, "batch append idempotent replay")
    require(replay.json == appended.json, f"batch replay mismatch: {replay.text}")
    head = ctx.client.get(f"/evidence/{chain}/head")
    require_2xx(head, "batch evidence head")
    require(head.json.get("seq") == 3, f"batch head mismatch: {head.text}")
    window = ctx.client.get(f"/evidence/{chain}/entries?from=2&to=3")
    require_2xx(window, "batch evidence from/to range")
    require([entry.get("seq") for entry in rows(window.json)] == [2, 3], f"from/to range mismatch: {window.text}")
    page = ctx.client.get(f"/evidence/{chain}/entries?after=1&limit=1")
    require_2xx(page, "batch evidence after/limit")
    require([entry.get("seq") for entry in rows(page.json)] == [2], f"after/limit page mismatch: {page.text}")
    ctx.add_evidence("Evidence batch dense ranges", f"{compact(appended)} | replay={compact(replay)} | head={compact(head)} | window={compact(window)} | page={compact(page)}")


def scenario_evidence_plain_delete_retracts_graph_edges(ctx: UatContext) -> None:
    chain = ctx.table("plain_edge")
    graph = ctx.table("plain_edge_graph")
    payload = base64.b64encode(b"edge").decode("ascii")
    require_2xx(ctx.client.put(f"/evidence/{chain}", {"verified": False}), "create plain edge chain")
    appended = ctx.client.post(
        f"/evidence/{chain}/entries",
        {
            "events": [
                {
                    "type": "plain.edge",
                    "payload_b64": payload,
                    "edges": [{"graph": graph, "src": "A", "dst": "B", "weight": 5}],
                }
            ]
        },
    )
    require_2xx(appended, "append plain edge")
    before = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(before, "plain edge graph before delete")
    require(before.json.get("connected") is True and before.json.get("bottleneck") == 5, f"plain edge not projected: {before.text}")
    deleted = ctx.client.delete(f"/evidence/{chain}/entries/1")
    require_2xx(deleted, "hard-delete plain edge with default retraction")
    after = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(after, "plain edge graph after delete")
    require(after.json.get("connected") is False, f"hard-delete should retract edge by default: {after.text}")
    ctx.add_evidence("Evidence plain delete retracts graph", f"append={compact(appended)} | before={compact(before)} | delete={compact(deleted)} | after={compact(after)}")


def scenario_evidence_unknown_head_and_idempotent_redaction(ctx: UatContext) -> None:
    unknown = ctx.table("unknown_head")
    head = ctx.client.get(f"/evidence/{unknown}/head")
    require_2xx(head, "unknown evidence chain head")
    require(head.json.get("seq") == 0, f"unknown chain head should be seq 0: {head.text}")

    chain = ctx.table("redact_twice")
    payload = base64.b64encode(b"redact me").decode("ascii")
    appended = ctx.client.post(f"/evidence/{chain}/entries", {"events": [{"type": "redact.twice", "payload_b64": payload}]})
    require_2xx(appended, "append evidence before double redaction")
    first = ctx.client.post(f"/evidence/{chain}/entries/1/redact")
    require_2xx(first, "first redaction")
    second = ctx.client.post(f"/evidence/{chain}/entries/1/redact")
    require_2xx(second, "second redaction is idempotent")
    entries = ctx.client.get(f"/evidence/{chain}/entries?from=1&to=1")
    require_2xx(entries, "read double-redacted evidence")
    entry = rows(entries.json)[0]
    require(entry.get("redacted") is True and "payload_b64" not in entry, f"double-redacted entry mismatch: {entries.text}")
    ctx.add_evidence("Evidence unknown head/idempotent redaction", f"head={compact(head)} | append={compact(appended)} | first={compact(first)} | second={compact(second)} | entry={compact(entries)}")


def scenario_evidence_consistency_invalid_argument_contract(ctx: UatContext) -> None:
    chain = ctx.table("consistency_bad_args")
    payload = base64.b64encode(b"consistency").decode("ascii")
    appended = ctx.client.post(
        f"/evidence/{chain}/entries",
        {"events": [{"type": "c.one", "payload_b64": payload}, {"type": "c.two", "payload_b64": payload}]},
    )
    require_2xx(appended, "append consistency bad-arg docs")
    from_zero = ctx.client.get(f"/evidence/{chain}/consistency?from=0&to=2")
    require(from_zero.status == 400, f"consistency from=0 should be rejected: {compact(from_zero)}")
    reversed_range = ctx.client.get(f"/evidence/{chain}/consistency?from=2&to=1")
    require(reversed_range.status == 400, f"consistency from>to should be rejected: {compact(reversed_range)}")
    missing_to = ctx.client.get(f"/evidence/{chain}/consistency?from=1&to=99")
    require(missing_to.status in (400, 404), f"consistency to beyond head should be rejected: {compact(missing_to)}")
    ctx.add_evidence("Evidence consistency invalid arguments", f"append={compact(appended)} | from_zero={compact(from_zero)} | reversed={compact(reversed_range)} | missing_to={compact(missing_to)}")


def scenario_evidence_plain_delete_can_keep_graph_edges(ctx: UatContext) -> None:
    chain = ctx.table("plain_keep_edges")
    graph = ctx.table("plain_keep_graph")
    payload = base64.b64encode(b"edge").decode("ascii")
    require_2xx(ctx.client.put(f"/evidence/{chain}", {"verified": False}), "create plain keep-edge chain")
    appended = ctx.client.post(
        f"/evidence/{chain}/entries",
        {
            "events": [
                {
                    "type": "lineage.keep",
                    "payload_b64": payload,
                    "edges": [{"graph": graph, "src": "A", "dst": "B", "weight": 5}],
                }
            ]
        },
    )
    require_2xx(appended, "append plain edge to keep")
    before = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(before, "plain edge before non-retracting delete")
    require(before.json.get("connected") is True, f"plain edge missing before delete: {before.text}")
    deleted = ctx.client.delete(f"/evidence/{chain}/entries/1?retract_edges=false")
    require_2xx(deleted, "plain delete without edge retraction")
    after = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(after, "plain edge after non-retracting delete")
    require(after.json.get("connected") is True and after.json.get("bottleneck") == 5, f"edge should remain when retract_edges=false: {after.text}")
    ctx.add_evidence("Evidence plain delete keeps graph edges", f"append={compact(appended)} | before={compact(before)} | delete={compact(deleted)} | after={compact(after)}")


def scenario_graph_mutate_delete_wins_and_noop_contract(ctx: UatContext) -> None:
    graph = ctx.table("mutate_delete_wins")
    mutated = ctx.client.post(
        f"/graph/{graph}/mutate",
        {"upserts": [{"src": "A", "dst": "B", "weight": 8}], "deletes": [{"src": "A", "dst": "B"}]},
    )
    require_2xx(mutated, "graph mutate upsert/delete same edge")
    widest = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(widest, "graph widest after delete-wins mutate")
    require(widest.json.get("connected") is False, f"delete should win within graph mutate: {widest.text}")
    delete_missing = ctx.client.delete(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B"}]})
    require_2xx(delete_missing, "graph delete missing edge")
    require(delete_missing.json.get("deleted") == 0, f"delete missing edge should report 0: {delete_missing.text}")
    drop_missing = ctx.client.delete(f"/graph/{ctx.table('missing_graph')}")
    require_2xx(drop_missing, "drop missing graph")
    require(drop_missing.json.get("dropped") == 0, f"drop missing graph should report 0: {drop_missing.text}")
    ctx.add_evidence("Graph mutate delete-wins/no-op contract", f"mutate={compact(mutated)} | widest={compact(widest)} | delete_missing={compact(delete_missing)} | drop_missing={compact(drop_missing)}")


def scenario_graph_invalid_edge_request_contract(ctx: UatContext) -> None:
    graph = ctx.table("invalid_graph")
    missing_dst = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "weight": 1}]})
    require_error(missing_dst, "graph edge missing dst")
    bad_delete = ctx.client.delete(f"/graph/{graph}/edges", {"edges": [{"src": "A"}]})
    require_error(bad_delete, "graph delete edge missing dst")
    missing_from = ctx.client.post(f"/graph/{graph}/reachable", {"directed": True})
    require_error(missing_from, "graph reachable missing from")
    intact = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "weight": 1}]})
    require_2xx(intact, "graph still usable after invalid requests")
    ctx.add_evidence("Graph invalid request contract", f"missing_dst={compact(missing_dst)} | bad_delete={compact(bad_delete)} | missing_from={compact(missing_from)} | intact={compact(intact)}")


def scenario_graph_workflow(ctx: UatContext) -> None:
    graph = ctx.table("lineage")
    upserted = ctx.client.put(
        f"/graph/{graph}/edges",
        {"edges": [{"src": "A", "dst": "B", "weight": 5}, {"src": "B", "dst": "C", "weight": 3}, {"src": "A", "dst": "D", "weight": 1}]},
    )
    require_2xx(upserted, "graph edge upsert")
    reachable = ctx.client.post(f"/graph/{graph}/reachable", {"from": ["A"], "floor": 3, "directed": True})
    require_2xx(reachable, "graph reachable")
    require(reachable.json.get("nodes") == ["A", "B", "C"], f"reachable mismatch: {reachable.text}")
    widest = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "C", "directed": True})
    require_2xx(widest, "graph widest path")
    require(widest.json.get("connected") is True and widest.json.get("bottleneck") == 3, f"widest mismatch: {widest.text}")
    rewired = ctx.client.post(
        f"/graph/{graph}/mutate",
        {"upserts": [{"src": "A", "dst": "C", "weight": 9}], "deletes": [{"src": "A", "dst": "D"}]},
    )
    require_2xx(rewired, "graph mutate")
    dropped = ctx.client.delete(f"/graph/{graph}")
    require_2xx(dropped, "graph drop")
    require(dropped.json.get("dropped", 0) >= 2, f"graph drop count mismatch: {dropped.text}")
    ctx.add_evidence("Graph traversal/mutation", f"{compact(upserted)} | {compact(reachable)} | {compact(widest)} | {compact(rewired)} | {compact(dropped)}")


def scenario_graph_edge_delete_merge_and_disconnected(ctx: UatContext) -> None:
    graph = ctx.table("edge_contract")
    first = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "weight": 5}]})
    require_2xx(first, "graph initial upsert")
    max_merge = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "weight": 9}], "merge": "max"})
    require_2xx(max_merge, "graph max merge")
    lower_set = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "weight": 1}], "merge": "set"})
    require_2xx(lower_set, "graph set merge")
    widest = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(widest, "graph widest after merge")
    require(widest.json.get("connected") is True and widest.json.get("bottleneck") == 1, f"merge/set mismatch: {widest.text}")
    invalid_merge = ctx.client.put(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "weight": 1}], "merge": "bogus"})
    require_error(invalid_merge, "invalid graph merge")
    deleted = ctx.client.delete(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B"}]})
    require_2xx(deleted, "graph edge delete")
    require(deleted.json.get("deleted") == 1, f"graph delete mismatch: {deleted.text}")
    disconnected = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B"})
    require_2xx(disconnected, "graph disconnected widest")
    require(disconnected.json.get("connected") is False and "bottleneck" not in disconnected.json, f"disconnected widest mismatch: {disconnected.text}")
    self_path = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "A"})
    require_2xx(self_path, "graph self widest")
    require(self_path.json.get("connected") is True and "bottleneck" not in self_path.json, f"self path mismatch: {self_path.text}")
    ctx.add_evidence("Graph edge delete/merge/disconnected", f"{compact(first)} | {compact(max_merge)} | {compact(lower_set)} | {compact(widest)} | {compact(invalid_merge)} | {compact(deleted)} | {compact(disconnected)}")


def scenario_graph_asof_sandbox_workflow(ctx: UatContext) -> None:
    live_graph = ctx.table("live_lineage")
    asof_graph = ctx.table("asof_lineage")
    live_edges = ctx.client.put(
        f"/graph/{live_graph}/edges",
        {"edges": [{"src": "A", "dst": "B", "weight": 1}, {"src": "B", "dst": "C", "weight": 1}]},
    )
    require_2xx(live_edges, "write live graph edges")
    sandbox = ctx.client.put(
        f"/graph/{asof_graph}/edges",
        {"edges": [{"src": "A", "dst": "B", "weight": 1}]},
    )
    require_2xx(sandbox, "as-of sandbox graph write")
    live = ctx.client.post(f"/graph/{live_graph}/reachable", {"from": ["A"], "directed": True})
    asof = ctx.client.post(f"/graph/{asof_graph}/reachable", {"from": ["A"], "directed": True})
    require_2xx(live, "live graph reachable")
    require_2xx(asof, "as-of graph reachable")
    require(live.json.get("nodes") == ["A", "B", "C"], f"live graph mismatch: {live.text}")
    require(asof.json.get("nodes") == ["A", "B"], f"as-of graph mismatch: {asof.text}")
    dropped = ctx.client.delete(f"/graph/{asof_graph}")
    require_2xx(dropped, "drop as-of graph")
    ctx.add_evidence("Graph as-of sandbox", f"live_edges={compact(live_edges)} | sandbox={compact(sandbox)} | live={compact(live)} | asof={compact(asof)} | drop={compact(dropped)}")


def scenario_graph_tenant_isolation(ctx: UatContext) -> None:
    graph = ctx.table("tenant_lineage")
    tenant = {"X-Bluedb-Tenant": "acme"}
    upserted = ctx.client.put(
        f"/graph/{graph}/edges",
        {"edges": [{"src": "tenant:A", "dst": "tenant:B", "weight": 4}]},
        tenant,
    )
    require_2xx(upserted, "tenant graph edge upsert")
    tenant_widest = ctx.client.post(
        f"/graph/{graph}/widest-path",
        {"from": "tenant:A", "to": "tenant:B", "directed": True},
        tenant,
    )
    require_2xx(tenant_widest, "tenant graph widest path")
    require(tenant_widest.json.get("connected") is True, f"tenant graph should be connected: {tenant_widest.text}")
    default_widest = ctx.client.post(
        f"/graph/{graph}/widest-path",
        {"from": "tenant:A", "to": "tenant:B", "directed": True},
    )
    require(
        default_widest.status == 404 or (default_widest.status == 200 and default_widest.json.get("connected") is False),
        f"default tenant should not see tenant graph: {compact(default_widest)}",
    )
    ctx.add_evidence("Graph tenant isolation", f"tenant={compact(tenant_widest)} | default={compact(default_widest)}")


def scenario_graph_typed_parallel_and_undirected_edges(ctx: UatContext) -> None:
    graph = ctx.table("typed_parallel")
    upserted = ctx.client.put(
        f"/graph/{graph}/edges",
        {
            "edges": [
                {"src": "A", "dst": "B", "weight": 7, "type": "fast"},
                {"src": "A", "dst": "B", "weight": 3, "type": "slow"},
            ]
        },
    )
    require_2xx(upserted, "typed parallel graph edges")
    require(upserted.json.get("upserted") == 2, f"typed parallel upsert mismatch: {upserted.text}")
    widest_before = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B", "directed": True})
    require_2xx(widest_before, "typed parallel widest before delete")
    require(widest_before.json.get("connected") is True and widest_before.json.get("bottleneck") == 7, f"typed widest before mismatch: {widest_before.text}")
    deleted_fast = ctx.client.delete(f"/graph/{graph}/edges", {"edges": [{"src": "A", "dst": "B", "type": "fast"}]})
    require_2xx(deleted_fast, "delete one typed edge")
    require(deleted_fast.json.get("deleted") == 1, f"typed delete mismatch: {deleted_fast.text}")
    widest_after = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "A", "to": "B", "directed": True})
    require_2xx(widest_after, "typed parallel widest after delete")
    require(widest_after.json.get("connected") is True and widest_after.json.get("bottleneck") == 3, f"typed widest after mismatch: {widest_after.text}")
    reverse_directed = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "B", "to": "A", "directed": True})
    require_2xx(reverse_directed, "typed reverse directed widest")
    require(reverse_directed.json.get("connected") is False, f"directed reverse path should be disconnected: {reverse_directed.text}")
    reverse_undirected = ctx.client.post(f"/graph/{graph}/widest-path", {"from": "B", "to": "A", "directed": False})
    require_2xx(reverse_undirected, "typed reverse undirected widest")
    require(reverse_undirected.json.get("connected") is True and reverse_undirected.json.get("bottleneck") == 3, f"undirected reverse mismatch: {reverse_undirected.text}")
    ctx.add_evidence("Graph typed parallel/undirected", f"{compact(upserted)} | before={compact(widest_before)} | delete={compact(deleted_fast)} | after={compact(widest_after)} | directed={compact(reverse_directed)} | undirected={compact(reverse_undirected)}")


def scenario_graph_floor_multi_seed_and_unknown_seed(ctx: UatContext) -> None:
    graph = ctx.table("floor_seed")
    require_2xx(
        ctx.client.put(
            f"/graph/{graph}/edges",
            {"edges": [{"src": "A", "dst": "B", "weight": 2}, {"src": "C", "dst": "D", "weight": 5}]},
        ),
        "graph floor seed edges",
    )
    floor = ctx.client.post(f"/graph/{graph}/reachable", {"from": ["A", "C"], "floor": 3, "directed": True})
    require_2xx(floor, "graph reachable floor multi-seed")
    require(floor.json.get("nodes") == ["A", "C", "D"], f"floor multi-seed reachable mismatch: {floor.text}")
    no_floor = ctx.client.post(f"/graph/{graph}/reachable", {"from": ["A"], "directed": True})
    require_2xx(no_floor, "graph reachable no floor")
    require(no_floor.json.get("nodes") == ["A", "B"], f"reachable without floor mismatch: {no_floor.text}")
    unknown = ctx.client.post(f"/graph/{graph}/reachable", {"from": ["Z"], "directed": True})
    require_2xx(unknown, "graph reachable unknown seed")
    require(unknown.json.get("nodes") == ["Z"], f"unknown seed should reach itself only: {unknown.text}")
    ctx.add_evidence("Graph floor/multi-seed/unknown", f"floor={compact(floor)} | no_floor={compact(no_floor)} | unknown={compact(unknown)}")


def scenario_lakehouse_workflow(ctx: UatContext) -> None:
    table = ctx.table("mirror")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "amount", "type": "INTEGER"},
        ],
    )
    pragma = ctx.client.post("/sql", {"sql": f"PRAGMA lakehouse_mirror_table('{table}', on)"})
    require_2xx(pragma, "enable lakehouse mirror")
    inserted = ctx.client.post(f"/tables/{table}", [{"id": 1, "amount": 99}, {"id": 2, "amount": 101}])
    require_2xx(inserted, "insert mirrored rows")

    listed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        listed = ctx.client.get("/catalog/v1/namespaces/default/tables")
        if listed.status == 200 and table in listed.text:
            break
        time.sleep(0.25)
    require(listed is not None and listed.status == 200, f"catalog list failed: {listed.text if listed else ''}")
    require(table in listed.text, f"mirrored table not listed after seal: {listed.text}")
    loaded = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{table}")
    require_2xx(loaded, "load mirrored table")
    require("metadata" in loaded.text.lower(), f"catalog load missing metadata: {loaded.text[:500]}")
    ctx.add_evidence("Mirror enabled and catalog-visible", f"{compact(pragma)} | {compact(listed)} | {compact(loaded)}")


def scenario_lakehouse_tenant_catalog_workflow(ctx: UatContext) -> None:
    table = ctx.table("tenant_mirror")
    tenant = {"X-Bluedb-Tenant": "acme"}
    created = ctx.client.post(
        "/schema/tables",
        {
            "name": table,
            "columns": [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "amount", "type": "INTEGER"},
            ],
        },
        tenant,
    )
    require_2xx(created, "tenant mirror table")
    pragma = ctx.client.post("/sql", {"sql": f"PRAGMA lakehouse_mirror_table('{table}', on)"}, tenant)
    require_2xx(pragma, "tenant mirror pragma")
    require_2xx(ctx.client.post(f"/tables/{table}", [{"id": 1, "amount": 7}], tenant), "tenant mirror insert")

    listed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        listed = ctx.client.get("/catalog/v1/namespaces/acme/tables")
        if listed.status == 200 and table in listed.text:
            break
        time.sleep(0.25)
    require(listed is not None and listed.status == 200, f"tenant catalog list failed: {listed.text if listed else ''}")
    require(table in listed.text, f"tenant mirrored table not listed: {listed.text}")
    default_list = ctx.client.get("/catalog/v1/namespaces/default/tables")
    require_2xx(default_list, "default catalog list")
    require(table not in default_list.text, f"tenant table leaked into default namespace: {default_list.text}")
    ctx.add_evidence("Tenant catalog isolation", f"acme={compact(listed)} | default={compact(default_list)}")


def scenario_lakehouse_catalog_protocol_workflow(ctx: UatContext) -> None:
    table = ctx.table("catalog_protocol")
    global_on = ctx.client.post("/sql", {"sql": "PRAGMA lakehouse_mirror = on"})
    require_2xx(global_on, "enable global lakehouse mirror")
    ctx.create_table(
        table,
        [
            {"name": "id", "type": "INTEGER", "primaryKey": True},
            {"name": "body", "type": "TEXT"},
        ],
    )
    require_2xx(ctx.client.post(f"/tables/{table}", [{"id": 1, "body": "a"}, {"id": 2, "body": "b"}]), "insert globally mirrored rows")
    listed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        listed = ctx.client.get("/catalog/v1/namespaces/default/tables")
        if listed.status == 200 and table in listed.text:
            break
        time.sleep(0.25)
    require(listed is not None and listed.status == 200 and table in listed.text, f"global mirrored table not listed: {listed.text if listed else ''}")
    config = ctx.client.get("/catalog/v1/config")
    require_2xx(config, "catalog config")
    require(config.json == {"defaults": {}, "overrides": {}}, f"catalog config mismatch: {config.text}")
    namespaces = ctx.client.get("/catalog/v1/namespaces")
    require_2xx(namespaces, "catalog namespaces")
    require(["default"] in namespaces.json.get("namespaces", []), f"default namespace missing: {namespaces.text}")
    namespace = ctx.client.get("/catalog/v1/namespaces/default")
    require_2xx(namespace, "catalog namespace metadata")
    require(namespace.json.get("namespace") == ["default"], f"namespace response mismatch: {namespace.text}")
    loaded = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{table}")
    require_2xx(loaded, "catalog load table")
    require(loaded.json.get("metadata-location", "").endswith(".metadata.json"), f"metadata location mismatch: {loaded.text}")
    require("schema" in json.dumps(loaded.json.get("metadata", {})).lower(), f"load table missing schema metadata: {loaded.text[:500]}")
    missing = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{ctx.table('no_catalog_table')}")
    require(missing.status == 404, f"missing catalog table should be 404: {compact(missing)}")
    ctx.add_evidence("Catalog protocol", f"{compact(global_on)} | {compact(config)} | {compact(namespaces)} | {compact(namespace)} | {compact(listed)} | {compact(loaded)} | missing={compact(missing)}")


def scenario_lakehouse_opt_out_and_compaction_pragma(ctx: UatContext) -> None:
    included = ctx.table("mirror_included")
    excluded = ctx.table("mirror_excluded")
    target = ctx.client.post("/sql", {"sql": "PRAGMA lakehouse_target_file_bytes = 1048576"})
    require_2xx(target, "lakehouse target file bytes PRAGMA")
    global_on = ctx.client.post("/sql", {"sql": "PRAGMA lakehouse_mirror = on"})
    require_2xx(global_on, "lakehouse global mirror on")
    opt_out = ctx.client.post("/sql", {"sql": f"PRAGMA lakehouse_mirror_table('{excluded}', off)"})
    require_2xx(opt_out, "lakehouse table opt-out")
    for table in (included, excluded):
        ctx.create_table(
            table,
            [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "body", "type": "TEXT"},
            ],
        )
        require_2xx(ctx.client.post(f"/tables/{table}", {"id": 1, "body": table}), f"insert lakehouse {table}")

    listed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        listed = ctx.client.get("/catalog/v1/namespaces/default/tables")
        if listed.status == 200 and included in listed.text:
            break
        time.sleep(0.25)
    require(listed is not None and listed.status == 200 and included in listed.text, f"included mirrored table not listed: {listed.text if listed else ''}")
    require(excluded not in listed.text, f"opted-out table leaked into catalog: {listed.text}")
    included_load = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{included}")
    require_2xx(included_load, "load included lakehouse table")
    excluded_load = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{excluded}")
    require(excluded_load.status == 404, f"opted-out lakehouse table should not load: {compact(excluded_load)}")
    ctx.add_evidence("Lakehouse opt-out/target pragma", f"target={compact(target)} | global={compact(global_on)} | opt_out={compact(opt_out)} | list={compact(listed)} | included={compact(included_load)} | excluded={compact(excluded_load)}")


def scenario_lakehouse_global_off_with_table_opt_in(ctx: UatContext) -> None:
    included = ctx.table("mirror_table_on")
    excluded = ctx.table("mirror_global_off")
    global_off = ctx.client.post("/sql", {"sql": "PRAGMA lakehouse_mirror = off"})
    require_2xx(global_off, "lakehouse global mirror off")
    table_on = ctx.client.post("/sql", {"sql": f"PRAGMA lakehouse_mirror_table('{included}', on)"})
    require_2xx(table_on, "lakehouse table opt-in while global off")
    for table in (included, excluded):
        ctx.create_table(
            table,
            [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "body", "type": "TEXT"},
            ],
        )
        require_2xx(ctx.client.post(f"/tables/{table}", {"id": 1, "body": table}), f"insert lakehouse global-off {table}")

    listed = None
    deadline = time.time() + 8
    while time.time() < deadline:
        listed = ctx.client.get("/catalog/v1/namespaces/default/tables")
        if listed.status == 200 and included in listed.text:
            break
        time.sleep(0.25)
    require(listed is not None and listed.status == 200 and included in listed.text, f"table opt-in did not mirror while global off: {listed.text if listed else ''}")
    require(excluded not in listed.text, f"global-off table leaked into catalog: {listed.text}")
    included_load = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{included}")
    require_2xx(included_load, "load table-opted-in lakehouse table")
    excluded_load = ctx.client.get(f"/catalog/v1/namespaces/default/tables/{excluded}")
    require(excluded_load.status == 404, f"global-off table should not load: {compact(excluded_load)}")
    ctx.add_evidence("Lakehouse global off/table opt-in", f"global_off={compact(global_off)} | table_on={compact(table_on)} | list={compact(listed)} | included={compact(included_load)} | excluded={compact(excluded_load)}")


def scenario_admin_sql_enabled_workflow(binary: Path, port: int, keep_data: bool) -> UatResult:
    case = UatCase(
        scenario_id="UAT-OPS-003",
        title="Opt-in admin SQL supports raw DDL, scripts, and transactions",
        persona="Platform administrator",
        business_value="Operators have a documented escape hatch for controlled maintenance without weakening the default /sql contract.",
        docs=["docs/api/rest.md#post-adminsql-arbitrary-sql-off-by-default"],
        acceptance_criteria=[
            "The server starts with BLUEDB_ENABLE_ADMIN_SQL enabled.",
            "Raw DDL succeeds only through /admin/sql.",
            "A multi-statement transaction script commits two rows.",
            "A rollback script does not persist its mutation.",
            "The resulting rows are visible through the normal /sql data plane.",
        ],
        priority="P1",
        fn=lambda ctx: None,
    )
    start = time.time()
    server = ManagedServer(
        binary,
        port,
        keep_data=keep_data,
        extra_env={
            "BLUEDB_ENABLE_ADMIN_SQL": "1",
            "BLUEDB_AUTHZ_TOKENS": "admin=superuser",
        },
    )
    ctx: UatContext | None = None
    try:
        ctx = UatContext(server.start(), int(time.time() * 1000))
        table = ctx.table("admin_sql")
        admin = {"Authorization": "Bearer admin"}
        created = ctx.client.post(
            "/admin/sql",
            {"sql": f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"},
            admin,
        )
        require_2xx(created, "admin SQL DDL")
        committed = ctx.client.post(
            "/admin/sql",
            {"sql": f"BEGIN; INSERT INTO {table} VALUES (1, 'ada'); INSERT INTO {table} VALUES (2, 'lin'); COMMIT;"},
            admin,
        )
        require_2xx(committed, "admin SQL transaction commit")
        rolled_back = ctx.client.post(
            "/admin/sql",
            {"sql": f"BEGIN; DELETE FROM {table}; ROLLBACK;"},
            admin,
        )
        require_2xx(rolled_back, "admin SQL transaction rollback")
        selected = ctx.client.post("/sql", {"sql": f"SELECT name FROM {table} ORDER BY id"}, admin)
        require_2xx(selected, "normal SQL after admin SQL")
        require([row.get("name") for row in rows(selected.json)] == ["ada", "lin"], f"admin SQL rows mismatch: {selected.text}")
        unauth = ctx.client.post("/admin/sql", {"sql": "SELECT 1"})
        require(unauth.status in (401, 403), f"admin SQL without superuser should fail: {compact(unauth)}")
        ctx.add_evidence("Admin SQL enabled", f"{compact(created)} | {compact(committed)} | {compact(rolled_back)} | {compact(selected)}")
        ctx.add_evidence("Admin SQL authorization", compact(unauth))
        return make_result(case, PASS, start, ctx.evidence)
    except Exception as exc:  # noqa: BLE001 - report UAT failures
        evidence = ctx.evidence if ctx else []
        evidence.append(Evidence("Server log tail", server.log_tail()))
        return make_result(case, FAIL, start, evidence, format_failure(exc))
    finally:
        server.stop()


def scenario_admin_sql_ddl_surface_workflow(binary: Path, port: int, keep_data: bool) -> UatResult:
    case = UatCase(
        scenario_id="UAT-SQL-009",
        title="Admin SQL covers documented DDL, composite primary keys, views, and CTAS",
        persona="Platform administrator",
        business_value="Operators can validate the full documented SQL DDL surface while keeping normal /sql constrained.",
        docs=["docs/sql/statements.md", "docs/sql/query-guardrail.md", "docs/api/rest.md#post-adminsql-arbitrary-sql-off-by-default"],
        acceptance_criteria=[
            "Composite primary keys can be created and queried without exposing the hidden surrogate column.",
            "Composite primary-key filters work through both /sql and the /tables data plane.",
            "Updating a key component is rejected.",
            "ALTER TABLE add/rename/drop column and rename table preserve existing rows.",
            "CREATE VIEW / DROP VIEW works.",
            "CREATE TABLE AS SELECT and DROP TABLE IF EXISTS behave as documented.",
        ],
        priority="P1",
        fn=lambda ctx: None,
    )
    start = time.time()
    server = ManagedServer(
        binary,
        port,
        keep_data=keep_data,
        extra_env={
            "BLUEDB_ENABLE_ADMIN_SQL": "1",
            "BLUEDB_AUTHZ_TOKENS": "admin=superuser",
        },
    )
    ctx: UatContext | None = None
    try:
        ctx = UatContext(server.start(), int(time.time() * 1000))
        admin = {"Authorization": "Bearer admin"}
        memberships = ctx.table("memberships")
        created = ctx.client.post(
            "/admin/sql",
            {
                "sql": (
                    f"CREATE TABLE {memberships} ("
                    "org_id INTEGER, user_id INTEGER, role TEXT, age INTEGER, "
                    "PRIMARY KEY (org_id, user_id))"
                )
            },
            admin,
        )
        require_2xx(created, "composite primary-key DDL")
        require_2xx(
            ctx.client.post(
                "/sql",
                {
                    "sql": f"INSERT INTO {memberships} (org_id, user_id, role, age) VALUES ($1, $2, $3, $4)",
                    "params": [1, 7, "owner", 36],
                },
                admin,
            ),
            "insert composite row through /sql",
        )
        require_2xx(
            ctx.client.post(
                "/sql",
                {
                    "sql": f"INSERT INTO {memberships} (org_id, user_id, role, age) VALUES ($1, $2, $3, $4)",
                    "params": [1, 8, "viewer", 17],
                },
                admin,
            ),
            "insert second composite row",
        )
        point = ctx.client.post(
            "/sql",
            {"sql": f"SELECT * FROM {memberships} WHERE org_id = $1 AND user_id = $2", "params": [1, 7]},
            admin,
        )
        require_2xx(point, "composite point query")
        row = rows(point.json)[0]
        require(row.get("role") == "owner" and "__bluedb_pk" not in row, f"composite point mismatch: {point.text}")
        data_plane = ctx.client.get(f"/tables/{memberships}?org_id=eq.1&user_id=eq.7", admin)
        require_2xx(data_plane, "composite /tables data-plane query")
        require(rows(data_plane.json)[0].get("role") == "owner", f"composite data-plane mismatch: {data_plane.text}")
        key_update = ctx.client.post("/sql", {"sql": f"UPDATE {memberships} SET org_id = 2 WHERE org_id = 1 AND user_id = 7"}, admin)
        require_error(key_update, "composite key update rejection")

        altered = ctx.client.post(
            "/admin/sql",
            {"sql": f"ALTER TABLE {memberships} ADD COLUMN nickname TEXT; ALTER TABLE {memberships} RENAME COLUMN nickname TO handle;"},
            admin,
        )
        require_2xx(altered, "ALTER add/rename column")
        require_2xx(ctx.client.post("/sql", {"sql": f"UPDATE {memberships} SET handle = 'ada' WHERE org_id = 1 AND user_id = 7"}, admin), "update renamed column")
        dropped = ctx.client.post("/admin/sql", {"sql": f"ALTER TABLE {memberships} DROP COLUMN age;"}, admin)
        require_2xx(dropped, "ALTER drop non-key column")
        renamed_table = f"{memberships}_renamed"
        renamed = ctx.client.post("/admin/sql", {"sql": f"ALTER TABLE {memberships} RENAME TO {renamed_table};"}, admin)
        require_2xx(renamed, "ALTER rename table")
        renamed_read = ctx.client.post("/sql", {"sql": f"SELECT role, handle FROM {renamed_table} WHERE org_id = 1 AND user_id = 7"}, admin)
        require_2xx(renamed_read, "read renamed table")
        require(rows(renamed_read.json)[0].get("handle") == "ada", f"renamed table mismatch: {renamed_read.text}")

        view_name = ctx.table("active_members")
        view = ctx.client.post(
            "/admin/sql",
            {"sql": f"CREATE VIEW {view_name} AS SELECT org_id, user_id, role FROM {renamed_table} WHERE role = 'owner';"},
            admin,
        )
        require_2xx(view, "CREATE VIEW")
        view_read = ctx.client.post("/query", {"sql": f"SELECT role FROM {view_name} ORDER BY user_id"}, admin)
        require_2xx(view_read, "read view")
        require([row.get("role") for row in rows(view_read.json)] == ["owner"], f"view mismatch: {view_read.text}")
        drop_view = ctx.client.post("/admin/sql", {"sql": f"DROP VIEW {view_name};"}, admin)
        require_2xx(drop_view, "DROP VIEW")

        ctas = ctx.client.post(
            "/admin/sql",
            {"sql": f"CREATE TABLE {ctx.table('adult_members')} AS SELECT * FROM {renamed_table} WHERE role = 'owner';"},
            admin,
        )
        require_2xx(ctas, "CREATE TABLE AS SELECT")
        drop_missing = ctx.client.post("/admin/sql", {"sql": f"DROP TABLE IF EXISTS {ctx.table('missing_drop')};"}, admin)
        require_2xx(drop_missing, "DROP TABLE IF EXISTS")
        ctx.add_evidence(
            "Admin SQL DDL surface",
            f"{compact(created)} | point={compact(point)} | data={compact(data_plane)} | key_update={compact(key_update)} | alter={compact(altered)} {compact(dropped)} {compact(renamed)} | view={compact(view_read)} | ctas={compact(ctas)} | drop={compact(drop_missing)}",
        )
        return make_result(case, PASS, start, ctx.evidence)
    except Exception as exc:  # noqa: BLE001 - report UAT failures
        evidence = ctx.evidence if ctx else []
        evidence.append(Evidence("Server log tail", server.log_tail()))
        return make_result(case, FAIL, start, evidence, format_failure(exc))
    finally:
        server.stop()


def scenario_authorization_workflow(binary: Path, port: int, keep_data: bool) -> UatResult:
    case = UatCase(
        scenario_id="UAT-SEC-001",
        title="Bearer scopes and tenant-bound tokens protect production APIs",
        persona="Platform administrator",
        business_value="Production operators can expose bluedb without anonymous writes or cross-tenant access.",
        docs=["docs/api/rest.md#authorization", "docs/deployment/configuration.md#api-surface--authorization"],
        acceptance_criteria=[
            "Health remains public.",
            "Schema writes require an authorized token.",
            "Reader tokens cannot write.",
            "Writer tokens can write and reader tokens can read.",
            "Tenant-bound tokens cannot reach the default tenant without the tenant header.",
        ],
        priority="P0",
        fn=lambda ctx: None,
    )
    start = time.time()
    server = ManagedServer(
        binary,
        port,
        keep_data=keep_data,
        extra_env={
            "BLUEDB_AUTHZ_TOKENS": (
                "reader=data:read;"
                "writer=data:read,data:write,data:query;"
                "admin=superuser;"
                "acme=data:read,data:write,data:query,schema:admin,tenant:acme"
            )
        },
    )
    ctx: UatContext | None = None
    try:
        ctx = UatContext(server.start(), int(time.time() * 1000))
        table = ctx.table("auth")
        admin = {"Authorization": "Bearer admin"}
        reader = {"Authorization": "Bearer reader"}
        writer = {"Authorization": "Bearer writer"}
        acme = {"Authorization": "Bearer acme", "X-Bluedb-Tenant": "acme"}

        health = ctx.client.get("/health")
        require_2xx(health, "auth health")
        unauth = ctx.client.post(
            "/schema/tables",
            {
                "name": table,
                "columns": [
                    {"name": "id", "type": "INTEGER", "primaryKey": True},
                    {"name": "name", "type": "TEXT"},
                ],
            },
        )
        require(unauth.status in (401, 403), f"schema write without token should fail: {compact(unauth)}")
        created = ctx.client.post(
            "/schema/tables",
            {
                "name": table,
                "columns": [
                    {"name": "id", "type": "INTEGER", "primaryKey": True},
                    {"name": "name", "type": "TEXT"},
                ],
            },
            admin,
        )
        require_2xx(created, "schema write with admin")
        denied = ctx.client.post(f"/tables/{table}", {"id": 1, "name": "reader"}, reader)
        require(denied.status in (401, 403), f"reader write should fail: {compact(denied)}")
        wrote = ctx.client.post(f"/tables/{table}", {"id": 1, "name": "writer"}, writer)
        require_2xx(wrote, "writer table write")
        read = ctx.client.get(f"/tables/{table}?id=eq.1", reader)
        require_2xx(read, "reader table read")
        tenant_created = ctx.client.post(
            "/schema/tables",
            {
                "name": f"{table}_tenant",
                "columns": [
                    {"name": "id", "type": "INTEGER", "primaryKey": True},
                    {"name": "name", "type": "TEXT"},
                ],
            },
            acme,
        )
        require_2xx(tenant_created, "tenant-bound schema write")
        tenant_denied = ctx.client.get(f"/tables/{table}", {"Authorization": "Bearer acme"})
        require(tenant_denied.status in (401, 403), f"tenant-bound token should not reach default tenant: {compact(tenant_denied)}")
        ctx.add_evidence("Auth decisions", f"unauth={compact(unauth)} reader_write={compact(denied)} tenant_default={compact(tenant_denied)}")
        return make_result(case, PASS, start, ctx.evidence)
    except Exception as exc:  # noqa: BLE001 - report UAT failures
        evidence = ctx.evidence if ctx else []
        evidence.append(Evidence("Server log tail", server.log_tail()))
        return make_result(case, FAIL, start, evidence, format_failure(exc))
    finally:
        server.stop()


def scenario_restart_durability_workflow(binary: Path, port: int, keep_data: bool) -> UatResult:
    case = UatCase(
        scenario_id="UAT-OPS-002",
        title="Acknowledged writes survive a local server restart",
        persona="Platform operator",
        business_value="Evaluators can trust that single-node local storage is durable across process restarts.",
        docs=["docs/deployment/local.md", "docs/guarantees/consistency.md"],
        acceptance_criteria=[
            "A row written before shutdown is acknowledged.",
            "The server restarts against the same data directory.",
            "The row is readable after restart.",
        ],
        priority="P0",
        fn=lambda ctx: None,
    )
    start = time.time()
    root = Path(tempfile.mkdtemp(prefix="bluedb-uat-restart-"))
    data_dir = root / "data"
    ctx: UatContext | None = None
    first = ManagedServer(binary, port, keep_data=True, data_dir=data_dir)
    second: ManagedServer | None = None
    try:
        ctx = UatContext(first.start(), int(time.time() * 1000))
        table = ctx.table("restart")
        ctx.create_table(
            table,
            [
                {"name": "id", "type": "INTEGER", "primaryKey": True},
                {"name": "name", "type": "TEXT"},
            ],
        )
        write = ctx.client.post(f"/tables/{table}", {"id": 1, "name": "durable"})
        require_2xx(write, "restart durability write")
        ctx.add_evidence("Pre-restart write", compact(write))
        first.stop()

        second = ManagedServer(binary, port, keep_data=True, data_dir=data_dir)
        ctx.client = second.start()
        read = ctx.client.get(f"/tables/{table}?id=eq.1")
        require_2xx(read, "restart durability read")
        require(rows(read.json)[0].get("name") == "durable", f"durable row missing after restart: {read.text}")
        ctx.add_evidence("Post-restart read", compact(read))
        return make_result(case, PASS, start, ctx.evidence)
    except Exception as exc:  # noqa: BLE001 - report UAT failures
        evidence = ctx.evidence if ctx else []
        evidence.append(Evidence("First server log tail", first.log_tail()))
        if second:
            evidence.append(Evidence("Second server log tail", second.log_tail()))
        return make_result(case, FAIL, start, evidence, format_failure(exc))
    finally:
        first.stop()
        if second:
            second.stop()
        if not keep_data:
            shutil.rmtree(root, ignore_errors=True)


def cases() -> list[UatCase]:
    return [
        UatCase(
            "UAT-OPS-001",
            "Node starts and reports an active writer",
            "Platform operator",
            "Operators can tell whether a node is alive and write-ready before routing traffic.",
            ["docs/deployment/local.md", "docs/operations/admin.md"],
            ["GET /health returns 2xx.", "GET /admin/status returns JSON containing an active writer role."],
            "P0",
            scenario_platform_ready,
        ),
        UatCase(
            "UAT-OPS-004",
            "Raw admin SQL is disabled in the default server configuration",
            "Platform administrator",
            "The powerful escape hatch is not accidentally exposed during normal evaluation or production bootstrap.",
            ["docs/api/rest.md#post-adminsql-arbitrary-sql-off-by-default"],
            ["POST /admin/sql returns a user-visible error when BLUEDB_ENABLE_ADMIN_SQL is not enabled."],
            "P0",
            scenario_admin_sql_disabled,
        ),
        UatCase(
            "UAT-QS-001",
            "Quickstart schema and SQL flow works exactly as published",
            "New evaluator",
            "A first-time user can copy the quickstart and get a working table and query result.",
            ["docs/quickstart.md"],
            [
                "CREATE TABLE through /schema/tables succeeds.",
                "Two single-statement INSERT requests through /sql succeed.",
                "SELECT returns ada and lin in id order.",
            ],
            "P0",
            scenario_quickstart_sql,
        ),
        UatCase(
            "UAT-REST-001",
            "Application REST CRUD is usable for typed tables",
            "Application developer",
            "CRUD APIs support normal app workflows with filtering, pagination counts, JSON, updates, deletes, and stable duplicate errors.",
            ["docs/api/rest.md", "docs/sql/json.md"],
            [
                "Structured schema DDL creates a typed table.",
                "Insert returns representation when requested.",
                "GET filters/order/limit return expected rows.",
                "Prefer count=exact returns Content-Range.",
                "PATCH and DELETE return affected rows.",
                "Duplicate primary key returns HTTP 409 and UNIQUE_VIOLATION.",
            ],
            "P0",
            scenario_rest_application_crud,
        ),
        UatCase(
            "UAT-SCHEMA-001",
            "Structured schema DDL validates identifiers, types, and indexes",
            "Application developer",
            "Teams can safely expose typed DDL without raw SQL injection or index drift.",
            ["docs/api/rest.md#schema-ddl-endpoints"],
            [
                "Inline and follow-up index creation succeed.",
                "Describe shows indexed columns.",
                "Index drop succeeds.",
                "Invalid identifiers and unsupported types are rejected.",
            ],
            "P0",
            scenario_schema_validation_and_indexes,
        ),
        UatCase(
            "UAT-REST-002",
            "REST reads can be gated by write freshness watermarks",
            "Application developer",
            "Clients can read their own writes across read paths using documented watermark headers.",
            ["docs/api/rest.md#read-your-writes-freshness"],
            [
                "Mutating responses carry X-Bluedb-Watermark.",
                "A read with X-Bluedb-Min-Watermark returns the written row.",
                "Read responses carry a reflected watermark.",
            ],
            "P0",
            scenario_rest_freshness_headers,
        ),
        UatCase(
            "UAT-REST-003",
            "REST and SQL errors expose stable user-facing classifications",
            "Application developer",
            "Client applications can branch on documented error codes instead of parsing prose.",
            ["docs/api/rest.md#errors"],
            ["Missing tables return NOT_FOUND.", "Type mismatch returns TYPE_MISMATCH.", "Bad SQL is rejected."],
            "P0",
            scenario_error_contracts,
        ),
        UatCase(
            "UAT-REST-004",
            "REST empty reads and no-match mutations return stable empty representations",
            "Application developer",
            "Client grids and mutation workflows can treat empty result sets consistently across GET, PATCH, and DELETE.",
            ["docs/api/rest.md#get-tablestable-select", "docs/api/rest.md#returning-the-affected-rows-prefer-returnrepresentation"],
            [
                "An exact-count GET with no matches returns [] and Content-Range */0.",
                "PATCH with no matching rows returns [] when representation is requested.",
                "DELETE with no matching rows returns [] when representation is requested.",
            ],
            "P0",
            scenario_rest_empty_and_no_match_contracts,
        ),
        UatCase(
            "UAT-REST-005",
            "REST JSON path projection and filtering work on /tables",
            "Application developer",
            "Applications can expose JSON-backed fields in REST reads without moving to raw SQL.",
            ["docs/api/rest.md#json-path-filters-on-tables", "docs/sql/json.md"],
            [
                "JSON path projection returns extracted values.",
                "JSON path filters select only matching rows.",
            ],
            "P1",
            scenario_rest_json_path_projection_contract,
        ),
        UatCase(
            "UAT-REST-006",
            "REST pagination count headers honor exact-count preferences",
            "Application developer",
            "Client-side grids can page results and show totals only when exact counts are explicitly requested.",
            ["docs/api/rest.md#get-tablestable-select", "docs/api/rest.md#counting-rows-prefer-countexact"],
            [
                "limit/offset returns the expected primary-key ordered page.",
                "Prefer count=exact returns Content-Range with the correct window and total.",
                "A read without Prefer count=exact omits Content-Range.",
            ],
            "P0",
            scenario_rest_pagination_count_header_contract,
        ),
        UatCase(
            "UAT-REST-007",
            "REST rejects unfiltered PATCH/DELETE and allows explicit bulk filters",
            "Application developer",
            "Teams can catch accidental missing filters while still using an explicit documented bulk-mutation path.",
            ["docs/api/rest.md#patch-tablestable-update-rows", "docs/api/rest.md#delete-tablestable-delete-rows"],
            [
                "PATCH without filters is rejected with 400.",
                "DELETE without filters is rejected with 400.",
                "An explicit indexed all-row filter updates every row.",
                "An explicit indexed all-row filter deletes every row.",
                "return=representation reports the explicitly affected rows.",
                "A subsequent exact-count read reports an empty table.",
            ],
            "P0",
            scenario_rest_unfiltered_mutation_contract,
        ),
        UatCase(
            "UAT-SCHEMA-002",
            "Schema unique column and table drop workflow behaves as documented",
            "Application developer",
            "Teams can use structured DDL for uniqueness constraints and cleanup without raw SQL.",
            ["docs/api/rest.md#schema-ddl-endpoints", "docs/api/rest.md#create-a-table"],
            [
                "A unique column rejects duplicate values with UNIQUE_VIOLATION.",
                "DELETE /schema/tables/{table} drops the table.",
                "Describing a dropped table returns 404.",
            ],
            "P0",
            scenario_schema_unique_column_and_drop_workflow,
        ),
        UatCase(
            "UAT-SCHEMA-003",
            "Schema search index endpoints validate table and column targets",
            "Search application developer",
            "Search setup automation can declare full-text and trigram indexes and get clear errors for bad targets.",
            ["docs/api/rest.md#schema-ddl-endpoints", "docs/sql/full-text-search.md"],
            [
                "Full-text and trigram index declarations succeed on a valid text column.",
                "Missing columns and missing tables are rejected.",
            ],
            "P1",
            scenario_schema_search_index_endpoint_errors,
        ),
        UatCase(
            "UAT-SCHEMA-004",
            "Schema DDL rejects duplicate tables and missing index targets",
            "Application developer",
            "Schema automation gets clear failures for repeated creates and invalid index declarations.",
            ["docs/api/rest.md#schema-ddl-endpoints"],
            [
                "Creating a table once succeeds.",
                "Creating the same table again is rejected.",
                "Creating an index on a missing column is rejected.",
                "Creating an index on a missing table is rejected.",
            ],
            "P0",
            scenario_schema_duplicate_and_missing_targets,
        ),
        UatCase(
            "UAT-SCHEMA-005",
            "Schema description preserves nullability and inline index metadata",
            "Application developer",
            "Schema diff tooling can trust describe responses for generated client models and index planning.",
            ["docs/api/rest.md#schema-ddl-endpoints", "docs/api/rest.md#create-a-table"],
            [
                "Primary-key metadata is present.",
                "Non-null and nullable columns are distinguished.",
                "Inline indexed columns are reported as indexed.",
            ],
            "P1",
            scenario_schema_nullable_description_contract,
        ),
        UatCase(
            "UAT-SQL-001",
            "Parameterized SQL is safe and constrained",
            "Application developer",
            "Clients can run user-input-driven queries without string interpolation or accidental script execution.",
            ["docs/api/rest.md#post-sql-transactional-reads-writes-read-your-writes"],
            [
                "Bound parameter predicates work on the transactional /sql lookup path.",
                "Injection-shaped strings are treated as data.",
                "A non-indexed scan is rejected on /sql with NO_INDEX and succeeds on /query.",
                "A write sent to /query is rejected with UNSUPPORTED_STATEMENT.",
                "Multi-statement scripts are rejected on /sql.",
            ],
            "P0",
            scenario_sql_query_safety,
        ),
        UatCase(
            "UAT-SQL-002",
            "Analytical SQL supports joins, aggregates, CTEs, and windows",
            "Data application developer",
            "Evaluation users can validate that bluedb is more than point lookups and supports BI-style reads.",
            ["docs/sql/query-syntax.md", "docs/sql/query-guardrail.md"],
            [
                "JOIN plus GROUP BY/HAVING returns correct grouped rows.",
                "A non-recursive CTE returns expected rows.",
                "A window function query runs over inserted data.",
                "Analytical reads run through POST /query.",
            ],
            "P1",
            scenario_sql_analytics_surface,
        ),
        UatCase(
            "UAT-SQL-003",
            "SQL expressions and scalar functions work in application queries",
            "Application developer",
            "Users can write realistic computed projections without exporting data to another query engine.",
            ["docs/sql/expressions.md", "docs/sql/functions.md"],
            [
                "String functions, arithmetic, CASE, and COALESCE return expected values.",
                "CAST and ROUND run over inserted rows.",
            ],
            "P1",
            scenario_sql_functions_and_expressions,
        ),
        UatCase(
            "UAT-SQL-004",
            "Advanced JSON SQL operators return expected rows and projections",
            "Data application developer",
            "Applications can rely on documented JSON containment and path-query behavior for semi-structured data.",
            ["docs/sql/json.md", "docs/api/rest.md#json-path-filters-on-tables"],
            [
                "JSON containment filters return only matching rows.",
                "jsonb_path_query extracts a nested scalar.",
                "jsonb_path_query_array extracts array values.",
                "The same JSON operator is rejected on /sql with NO_INDEX.",
            ],
            "P1",
            scenario_json_advanced_queries,
        ),
        UatCase(
            "UAT-SQL-005",
            "SQL query syntax supports joins, ordering, distinct, pagination, and set intersections",
            "Data application developer",
            "Users can run BI-shaped reads from the documented SQL syntax without switching engines.",
            ["docs/sql/query-syntax.md", "docs/sql/expressions.md"],
            [
                "LEFT JOIN and JOIN USING return expected rows.",
                "LIMIT/OFFSET, DISTINCT, INTERSECT, and NULLS FIRST behave as documented.",
            ],
            "P1",
            scenario_sql_query_syntax_deep_surface,
        ),
        UatCase(
            "UAT-SQL-006",
            "SQL subqueries and set operations work for nested application reads",
            "Data application developer",
            "Applications can use IN, EXISTS, scalar subqueries, UNION ALL, and EXCEPT for normal relational workflows.",
            ["docs/sql/query-syntax.md#subqueries", "docs/sql/query-syntax.md#set-operations"],
            [
                "IN and EXISTS subqueries return expected users.",
                "Scalar subquery counts are correct.",
                "UNION ALL keeps duplicates and EXCEPT returns left-only rows.",
            ],
            "P1",
            scenario_sql_subqueries_and_set_operations,
        ),
        UatCase(
            "UAT-SQL-007",
            "Documented SQL data types and REST value encodings round-trip",
            "Application developer",
            "Clients can rely on canonical JSON values for booleans, decimals, temporal values, and UUIDs.",
            ["docs/sql/data-types.md", "docs/api/rest.md#value-encoding"],
            [
                "BOOLEAN, DECIMAL, DATE, TIME, TIMESTAMP, and UUID values insert successfully.",
                "REST reads return canonical encodings.",
                "Documented type literals run through /sql.",
            ],
            "P1",
            scenario_sql_data_types_value_encoding,
        ),
        UatCase(
            "UAT-SQL-008",
            "SQL metadata tables and unsupported-feature errors are detectable",
            "Platform administrator",
            "Schema introspection and compatibility checks can be automated through documented catalog tables and safe errors.",
            ["docs/sql/metadata.md", "docs/sql/limitations.md"],
            [
                "GLUE_OBJECTS, GLUE_TABLE_COLUMNS, and GLUE_INDEXES reflect live schema.",
                "/query rejects GLUE_* metadata tables as unsupported.",
                "Primary-key updates and CREATE TYPE are rejected.",
                "Recursive CTEs return expected rows through /query.",
            ],
            "P1",
            scenario_sql_metadata_and_limitations,
        ),
        UatCase(
            "UAT-SQL-010",
            "SQL RETURNING returns affected rows for insert, update, and delete",
            "Application developer",
            "Applications can avoid follow-up reads after writes and still receive the affected row data.",
            ["docs/api/rest.md#returning-get-the-affected-rows-back", "docs/sql/statements.md"],
            [
                "INSERT RETURNING returns inserted columns.",
                "UPDATE RETURNING returns updated rows.",
                "DELETE RETURNING returns the row captured before deletion.",
                "A no-match write with RETURNING returns [].",
            ],
            "P0",
            scenario_sql_returning_contract,
        ),
        UatCase(
            "UAT-SQL-011",
            "SQL and query surfaces enforce the documented lookup-vs-scan split",
            "Application developer",
            "Clients can route point reads to /sql and analytical scans to /query with predictable guardrails.",
            ["docs/api/rest.md#post-sql-transactional-reads-writes-read-your-writes", "docs/api/rest.md#post-query-analytical-reads-htap"],
            [
                "/sql no-WHERE browsing is auto-bound.",
                "/query can count all rows.",
                "/sql rejects non-indexed ORDER BY with NO_INDEX.",
                "/query can sort by a non-indexed column.",
            ],
            "P0",
            scenario_sql_autobound_and_query_scan_contract,
        ),
        UatCase(
            "UAT-SQL-012",
            "Analytical /query honors freshness watermarks and read-wait PRAGMA validation",
            "Application developer",
            "Apps using the HTAP path can enforce read-your-writes freshness and detect invalid read-wait configuration.",
            ["docs/api/rest.md#post-query-analytical-reads-htap", "docs/api/rest.md#read-your-writes-freshness"],
            [
                "PRAGMA bluedb_read_wait_seal_n accepts a non-negative value.",
                "A /query read with X-Bluedb-Min-Watermark sees the written row.",
                "A negative read-wait PRAGMA is rejected.",
            ],
            "P1",
            scenario_sql_query_freshness_and_pragmas,
        ),
        UatCase(
            "UAT-SQL-013",
            "SQL default row order and default null-order setting are documented",
            "Application developer",
            "Clients relying on deterministic browsing and engine settings can validate the documented defaults.",
            ["docs/sql/statements.md#default-row-order", "docs/sql/query-syntax.md#null-ordering"],
            [
                "A SELECT without ORDER BY returns rows in primary-key order.",
                "SET default_null_order accepts a documented value.",
            ],
            "P1",
            scenario_sql_default_order_and_set_contract,
        ),
        UatCase(
            "UAT-SQL-014",
            "Normal SQL surfaces reject DDL and transaction scripts",
            "Application developer",
            "Applications can expose /sql and /query without allowing schema mutation or explicit transaction control.",
            ["docs/api/rest.md#post-sql-transactional-reads-writes-read-your-writes", "docs/api/rest.md#post-query-analytical-reads-htap", "docs/sql/statements.md"],
            [
                "CREATE TABLE is rejected on /sql.",
                "BEGIN is rejected on /sql.",
                "CREATE TABLE is rejected on /query.",
            ],
            "P0",
            scenario_sql_rejects_ddl_transactions_and_query_ddl,
        ),
        UatCase(
            "UAT-SQL-015",
            "SQL indexed range predicates can order by the indexed column",
            "Application developer",
            "Transactional lookups can use secondary indexes for ordered range reads without falling back to scans.",
            ["docs/api/rest.md#post-sql-transactional-reads-writes-read-your-writes", "docs/sql/query-guardrail.md"],
            [
                "A secondary index can be declared through schema DDL.",
                "A range predicate over that indexed column succeeds on /sql.",
                "ORDER BY the same indexed column returns deterministic order.",
            ],
            "P0",
            scenario_sql_secondary_index_range_order,
        ),
        UatCase(
            "UAT-TENANT-001",
            "JSON reads and tenant isolation behave as documented",
            "SaaS application developer",
            "A multi-tenant app can store JSON and rely on tenant headers to isolate customer data.",
            ["docs/sql/json.md", "docs/api/rest.md#tenant-selection"],
            ["JSON path filters return matching rows.", "A table created under tenant acme is not visible from the default tenant."],
            "P0",
            scenario_json_and_tenant_isolation,
        ),
        UatCase(
            "UAT-SEARCH-001",
            "SQL full-text and trigram search are immediately usable",
            "Application developer",
            "Search experiences can use PostgreSQL-shaped FTS and LIKE over recently written content.",
            ["docs/sql/full-text-search.md", "docs/api/rest.md#post-sql-transactional-reads-writes-read-your-writes"],
            ["Parameterized plainto_tsquery finds the inserted document.", "Trigram LIKE finds the inserted document."],
            "P1",
            scenario_sql_full_text_search,
        ),
        UatCase(
            "UAT-SEARCH-002",
            "SQL full-text search handles query variants and index mutations",
            "Search application developer",
            "Search users can rely on more than the simplest plainto query and can update indexed content safely.",
            ["docs/sql/full-text-search.md"],
            [
                "to_tsquery boolean syntax returns the expected matching documents.",
                "websearch_to_tsquery phrase syntax returns the expected matching document.",
                "Updating indexed content removes stale matches.",
            ],
            "P1",
            scenario_sql_search_variants_and_mutations,
        ),
        UatCase(
            "UAT-SEARCH-003",
            "SQL full-text search composes rank, structured filters, and pagination",
            "Search application developer",
            "Search views can mix FTS rank ordering with application filters and stable page windows.",
            ["docs/sql/full-text-search.md#ranking", "docs/sql/full-text-search.md#filters-and-pagination"],
            [
                "A full-text index can be declared on a text column.",
                "FTS search can be combined with a structured status filter.",
                "ORDER BY ts_rank with LIMIT/OFFSET returns the expected page set.",
            ],
            "P1",
            scenario_sql_full_text_rank_filter_pagination,
        ),
        UatCase(
            "UAT-SEARCH-004",
            "SQL full-text guardrails reject joins and missing indexes",
            "Search application developer",
            "Search users receive explicit failures for unsupported FTS plans rather than slow or incorrect scans.",
            ["docs/sql/full-text-search.md#limitations", "docs/sql/query-guardrail.md"],
            [
                "FTS with a JOIN is rejected on /sql.",
                "FTS against an unindexed text column is rejected.",
            ],
            "P1",
            scenario_sql_full_text_join_and_missing_index_errors,
        ),
        UatCase(
            "UAT-SEARCH-005",
            "SQL trigram guardrail requires an index while /query can fall back",
            "Search application developer",
            "Applications can choose between indexed transactional search and analytical fallback behavior deliberately.",
            ["docs/sql/full-text-search.md#trigram-substring-search", "docs/api/rest.md#post-query-analytical-reads-htap"],
            [
                "/sql rejects LIKE substring search without a trigram index.",
                "/query can run the same LIKE predicate analytically and returns the expected row.",
            ],
            "P1",
            scenario_sql_trigram_no_index_query_fallback,
        ),
        UatCase(
            "UAT-COLL-001",
            "Mongo-shaped collection workflow supports basic document apps",
            "Document app developer",
            "Teams can evaluate the HTTP document API for insert/find/update/count/index workflows.",
            ["docs/collections/README.md"],
            [
                "insert returns inserted ids and count.",
                "find by equality returns the expected document.",
                "$inc update changes one matching document.",
                "numeric range count returns only matching documents.",
                "createIndex returns a name.",
                "unsupported operators return Mongo-shaped errors.",
            ],
            "P0",
            scenario_collections_document_workflow,
        ),
        UatCase(
            "UAT-COLL-002",
            "Collections aggregation, lookup, and delete support document workflows",
            "Document app developer",
            "Document API evaluators can test pipeline-style reads and mutation cleanup, not just simple find.",
            ["docs/collections/README.md#aggregation-stages"],
            [
                "$unwind plus $group returns expected counts.",
                "$lookup nests matched documents.",
                "delete removes one selected document.",
            ],
            "P1",
            scenario_collections_aggregation_and_lookup,
        ),
        UatCase(
            "UAT-COLL-003",
            "Collection indexes enforce uniqueness and support multikey membership",
            "Document app developer",
            "Applications can rely on documented unique and multikey index behavior for user-facing constraints.",
            ["docs/collections/README.md#indexing"],
            [
                "A unique index rejects duplicate field values.",
                "A multikey index can find documents by array membership.",
            ],
            "P0",
            scenario_collections_index_semantics,
        ),
        UatCase(
            "UAT-COLL-004",
            "Collection filters match Mongo-style comparison and logical semantics",
            "Document app developer",
            "Document applications can trust common filters, including missing-field behavior, before adopting the API.",
            ["docs/collections/README.md#find--count-filter-operators", "docs/collections/README.md#freshness"],
            [
                "$in, $ne, $exists, $or, $and, $nin, and range filters return the expected documents.",
                "$ne and $nin include documents where the field is missing.",
                "Indexed fields provide read-your-writes behavior for these filters.",
            ],
            "P0",
            scenario_collections_filter_semantics,
        ),
        UatCase(
            "UAT-COLL-005",
            "Collection updates support documented operators and replacement writes",
            "Document app developer",
            "Applications can mutate documents with predictable Mongo-shaped update semantics.",
            ["docs/collections/README.md#update-operators"],
            [
                "$set, $unset, $inc, $push, and $pull update one matching document.",
                "Full-document replacement preserves _id and removes absent fields.",
                "Unsupported update operators return an error.",
            ],
            "P0",
            scenario_collections_update_operators,
        ),
        UatCase(
            "UAT-COLL-006",
            "Collections support projection, pagination, upsert, and multi-row mutation",
            "Document app developer",
            "Document workflows can page projected results and perform documented upsert/multi mutation flows.",
            ["docs/collections/README.md#endpoints", "docs/collections/README.md#update-operators"],
            [
                "find projection keeps only requested fields plus _id.",
                "sort, limit, and skip page results.",
                "upsert inserts a missing document and multi update/delete affect all matches.",
            ],
            "P1",
            scenario_collections_projection_pagination_upsert_delete,
        ),
        UatCase(
            "UAT-COLL-007",
            "Collection aggregation supports the documented stage matrix and rejects unsupported stages",
            "Document app developer",
            "Analytics-style document reads can use the documented aggregation subset and detect unsupported MongoDB stages.",
            ["docs/collections/README.md#aggregation-stages"],
            [
                "$match, $group, $sort, $skip, $limit, $project, and $addFields work together.",
                "Unsupported $facet and aggregate $regex are rejected.",
            ],
            "P1",
            scenario_collections_aggregation_stage_matrix,
        ),
        UatCase(
            "UAT-COLL-008",
            "Collection compound, TTL, and nested-index contracts behave as documented",
            "Document app developer",
            "Applications can rely on compound equality, TTL expiry, and deterministic rejection of unsupported nested index shapes.",
            ["docs/collections/README.md#indexing"],
            [
                "Compound full-key equality finds the expected document.",
                "TTL index expires old documents and preserves live documents.",
                "Negative TTL and dotted compound/multikey indexes are rejected.",
            ],
            "P1",
            scenario_collections_compound_ttl_and_index_errors,
        ),
        UatCase(
            "UAT-COLL-009",
            "Collections generate stable ids and validate insert request bodies",
            "Document app developer",
            "Document clients can rely on server-generated ids and predictable request validation for migration tooling.",
            ["docs/collections/README.md#endpoints", "docs/collections/README.md#examples"],
            [
                "Inserts without _id return distinct generated string ids.",
                "Find by generated _id returns the document.",
                "An empty insert returns zero inserted ids.",
                "A malformed insert body is rejected.",
            ],
            "P0",
            scenario_collections_generated_ids_and_request_validation,
        ),
        UatCase(
            "UAT-COLL-010",
            "Collection filters cover regex, top-level not, and unsupported operator failures",
            "Document app developer",
            "Applications can exercise the documented Mongo-style filter matrix and detect unsupported query operators.",
            ["docs/collections/README.md#find--count-filter-operators"],
            [
                "$regex works on an indexed string field.",
                "Top-level $not excludes matching documents.",
                "$type and top-level $expr are rejected as unsupported.",
            ],
            "P1",
            scenario_collections_regex_not_and_unsupported_filters,
        ),
        UatCase(
            "UAT-COLL-011",
            "Collection update operators initialize missing fields predictably",
            "Document app developer",
            "Document mutation code can use $inc and $push on absent fields without a read-before-write.",
            ["docs/collections/README.md#update-operators"],
            [
                "$inc initializes a missing numeric field to zero before incrementing.",
                "$push initializes a missing array field to an empty array.",
                "$pull removes the pushed value.",
            ],
            "P1",
            scenario_collections_update_initializers,
        ),
        UatCase(
            "UAT-COLL-012",
            "Collection aggregation returns count, null-group totals, and empty lookup arrays",
            "Document app developer",
            "Analytics-style document workflows can depend on aggregate counts and left-join no-match semantics.",
            ["docs/collections/README.md#aggregation-stages"],
            [
                "$count returns the total document count.",
                "$group with _id:null aggregates all rows.",
                "$lookup nests [] when no foreign document matches.",
            ],
            "P1",
            scenario_collections_count_null_group_and_lookup_empty,
        ),
        UatCase(
            "UAT-COLL-013",
            "Collection indexes backfill existing docs, are idempotent, and reject bad paths",
            "Document app developer",
            "Index automation can be rerun safely and failed index declarations do not corrupt the collection.",
            ["docs/collections/README.md#indexing"],
            [
                "Creating an index after documents exist backfills them.",
                "Creating the same index again succeeds.",
                "Malformed index paths are rejected.",
                "The collection remains writable after a rejected index path.",
            ],
            "P1",
            scenario_collections_index_backfill_idempotency_and_bad_paths,
        ),
        UatCase(
            "UAT-COLL-014",
            "Collections reject duplicate _id values and count all or filtered documents",
            "Document app developer",
            "Document clients can rely on Mongo-style identity uniqueness and count responses for pagination totals.",
            ["docs/collections/README.md#endpoints", "docs/collections/README.md#find--count-filter-operators"],
            [
                "The first insert of a caller-supplied _id succeeds.",
                "A duplicate _id insert is rejected with a Mongo-shaped error.",
                "count with an empty filter returns all documents.",
                "count with a non-matching filter returns zero.",
            ],
            "P0",
            scenario_collections_duplicate_id_and_count_contract,
        ),
        UatCase(
            "UAT-COLL-015",
            "Collection scalar values participate in multikey indexes and unsupported updates fail clearly",
            "Document app developer",
            "Document models can mix scalar and array values on indexed fields while catching unsupported mutation operators.",
            ["docs/collections/README.md#indexing", "docs/collections/README.md#update-operators"],
            [
                "A multikey index can match both scalar and array values.",
                "$pop, $rename, and $mul are rejected as unsupported update operators.",
            ],
            "P1",
            scenario_collections_scalar_multikey_and_unsupported_update_errors,
        ),
        UatCase(
            "UAT-COLL-016",
            "Collection documents are isolated by tenant",
            "SaaS document app developer",
            "A shared collection name can be used across tenants without data leakage.",
            ["docs/collections/README.md#multi-tenancy", "docs/api/rest.md#tenant-selection"],
            [
                "The default tenant sees only default-tenant documents.",
                "A tenant header sees only that tenant's documents.",
                "Tenant-scoped count does not include default documents.",
            ],
            "P0",
            scenario_collections_tenant_isolation,
        ),
        UatCase(
            "UAT-COLL-017",
            "Collection aggregation ignores missing unwind values and rejects dotted lookup paths",
            "Document app developer",
            "Aggregation clients can understand array unwind behavior and unsupported lookup shapes before production use.",
            ["docs/collections/README.md#aggregation-stages"],
            [
                "$unwind emits elements from array fields while ignoring missing/null values.",
                "$lookup with dotted localField is rejected.",
            ],
            "P1",
            scenario_collections_unwind_missing_and_lookup_dotted_path_errors,
        ),
        UatCase(
            "UAT-COLL-018",
            "Collection operation and identifier errors leave collections usable",
            "Document app developer",
            "Bad client requests fail predictably without corrupting the target collection.",
            ["docs/collections/README.md#errors", "docs/collections/README.md#endpoints"],
            [
                "An unknown collection operation is rejected.",
                "Invalid collection identifiers are rejected.",
                "The original collection remains readable after invalid requests.",
            ],
            "P1",
            scenario_collections_unknown_operation_and_identifier_errors,
        ),
        UatCase(
            "UAT-COLL-SEARCH-001",
            "Elasticsearch-shaped collection search returns hits and highlights",
            "Search application developer",
            "Teams can validate search mappings, result envelopes, source projection, and highlighting.",
            ["docs/collections/search.md"],
            ["searchIndex can be declared and read.", "match query returns the expected hit.", "highlight is included for requested fields."],
            "P1",
            scenario_collection_search_workflow,
        ),
        UatCase(
            "UAT-COLL-SEARCH-002",
            "Collection search supports DSL filters, projection, and unsupported-query errors",
            "Search application developer",
            "Search evaluators can verify realistic filtering and error handling beyond a single match query.",
            ["docs/collections/search.md#query-dsl-compatibility", "docs/collections/search.md#_source"],
            [
                "bool + range + term clauses return the expected hit.",
                "_source projection and _source=false behave as documented.",
                "Unsupported wildcard query returns an error.",
            ],
            "P1",
            scenario_collection_search_dsl_variants,
        ),
        UatCase(
            "UAT-COLL-SEARCH-003",
            "Collection search rejects unmapped or unsupported query shapes predictably",
            "Search application developer",
            "Client teams get clear failures for unsupported Elasticsearch-style requests instead of silent bad results.",
            ["docs/collections/search.md#describing-a-mapping", "docs/collections/search.md#query-dsl-compatibility", "docs/collections/search.md#sort"],
            [
                "Reading a missing mapping returns 404.",
                "Unmapped fields, range on analyzed text, and text/keyword sort are rejected.",
                "match_phrase still returns the expected hit.",
            ],
            "P1",
            scenario_collection_search_error_contracts,
        ),
        UatCase(
            "UAT-COLL-SEARCH-004",
            "Collection search supports backfill, pagination, numeric sort, and live maintenance",
            "Search application developer",
            "Search-backed document apps can re-declare mappings, page hits, sort numerically, and trust update/delete maintenance.",
            ["docs/collections/search.md#declaring-a-search-mapping", "docs/collections/search.md#results-features", "docs/collections/search.md#freshness"],
            [
                "searchIndex backfills existing documents.",
                "from/size and numeric sort return the expected page.",
                "Missing sort fields sort last.",
                "Collection update/delete immediately update search results on the writer.",
            ],
            "P1",
            scenario_collection_search_pagination_sort_and_freshness,
        ),
        UatCase(
            "UAT-COLL-SEARCH-005",
            "Collection search supports exact term, range, exists, and match_all queries",
            "Search application developer",
            "Search-backed applications can use Elasticsearch-shaped exact and structured query primitives.",
            ["docs/collections/search.md#supported-query-types", "docs/collections/search.md#sort"],
            [
                "match_all reports the full matching total.",
                "term on a keyword field returns exact matches.",
                "range on an integer field honors boundaries and sort.",
                "exists excludes documents missing the mapped field.",
            ],
            "P1",
            scenario_collection_search_term_range_exists_match_all,
        ),
        UatCase(
            "UAT-COLL-SEARCH-006",
            "Collection search mapping replacement reindexes and invalid identifiers are rejected",
            "Search application developer",
            "Mapping migration code can replace mappings and detect bad collection names before search traffic is routed.",
            ["docs/collections/search.md#declaring-a-search-mapping", "docs/collections/search.md#errors"],
            [
                "An initial mapping indexes existing documents.",
                "Re-declaring the mapping replaces the old field set.",
                "Searching an old unmapped field is rejected.",
                "Invalid collection identifiers return 400 on search and searchIndex.",
            ],
            "P1",
            scenario_collection_search_mapping_replacement_and_identifier_errors,
        ),
        UatCase(
            "UAT-COLL-SEARCH-007",
            "Collection search mappings and documents are isolated by tenant",
            "SaaS search application developer",
            "Tenant-scoped search indices do not leak mappings or documents across customer namespaces.",
            ["docs/collections/search.md#authorization-and-tenancy"],
            [
                "A tenant can declare a mapping, insert a doc, and search it immediately.",
                "A second tenant does not see the first tenant's mapping.",
                "After declaring its own mapping, the second tenant backfills zero first-tenant docs.",
            ],
            "P0",
            scenario_collection_search_tenant_isolation,
        ),
        UatCase(
            "UAT-COLL-SEARCH-008",
            "Collection search supports phrase matching, highlight, and source controls",
            "Search application developer",
            "Search UIs can request precise phrase matches, highlighted snippets, and trimmed source payloads.",
            ["docs/collections/search.md#supported-query-types", "docs/collections/search.md#results-features", "docs/collections/search.md#_source"],
            [
                "match_phrase returns the ordered phrase hit.",
                "Highlight snippets include emphasized text.",
                "_source field-list returns only requested fields.",
                "_source=false omits stored source from hits.",
            ],
            "P1",
            scenario_collection_search_phrase_highlight_and_source_controls,
        ),
        UatCase(
            "UAT-COLL-SEARCH-009",
            "Collection search bool should clauses work and unsupported query families are rejected",
            "Search application developer",
            "Search clients can use the supported bool subset and detect unsupported Elasticsearch query families early.",
            ["docs/collections/search.md#query-dsl-compatibility", "docs/collections/search.md#errors"],
            [
                "bool.should returns documents matching either clause.",
                "multi_match is rejected.",
                "query_string is rejected.",
                "nested is rejected.",
            ],
            "P1",
            scenario_collection_search_bool_should_and_unsupported_queries,
        ),
        UatCase(
            "UAT-LEDGER-001",
            "Ledger transfers update balances and SQL projection atomically",
            "Fintech evaluator",
            "A ledger user can create accounts, post a transfer, read balances, and query the projection through SQL.",
            ["docs/api/ledger.md"],
            ["Accounts are created.", "Transfer is created.", "Debit/credit posted balances update.", "ledger_transfers SQL projection agrees."],
            "P0",
            scenario_ledger_workflow,
        ),
        UatCase(
            "UAT-LEDGER-002",
            "Ledger duplicate replays and missing lookups are explicit",
            "Fintech evaluator",
            "Ledger clients can safely retry creates and handle absent records predictably.",
            ["docs/api/ledger.md#result-codes"],
            [
                "Duplicate account replay returns exists.",
                "Duplicate transfer replay returns exists.",
                "Missing account lookup returns 404.",
            ],
            "P0",
            scenario_ledger_duplicate_and_missing_workflow,
        ),
        UatCase(
            "UAT-LEDGER-003",
            "Ledger two-phase pending, post, and void flows update balances and SQL projection",
            "Fintech evaluator",
            "Payment workflows can reserve funds, settle part of a reservation, void another reservation, and query the projection.",
            ["docs/api/ledger.md#two-phase-transfers", "docs/api/ledger.md#querying-balances-via-sql"],
            [
                "Pending transfer updates pending balances.",
                "post_pending_transfer releases pending and posts the selected amount.",
                "void_pending_transfer releases pending without posting.",
                "SQL projection agrees with canonical account state.",
            ],
            "P0",
            scenario_ledger_two_phase_transfer_workflow,
        ),
        UatCase(
            "UAT-LEDGER-004",
            "Ledger linked chains and account constraints return documented result codes",
            "Fintech evaluator",
            "Clients can safely batch linked operations and branch on TigerBeetle-style rejection codes.",
            ["docs/api/ledger.md#account-flags", "docs/api/ledger.md#result-codes"],
            [
                "A linked chain rolls back when a later event fails.",
                "The rolled-back member reports linked_event_failed.",
                "A must-not-exceed account constraint returns exceeds_credits.",
            ],
            "P0",
            scenario_ledger_linked_chain_and_constraint_errors,
        ),
        UatCase(
            "UAT-LEDGER-005",
            "Ledger transfer lookup and /query projection expose canonical transfer data",
            "Fintech evaluator",
            "Ledger users can retrieve a transfer by id and scan the SQL projection through the documented analytical surface.",
            ["docs/api/ledger.md#get-ledgertransfersid-look-up-a-transfer", "docs/api/ledger.md#querying-balances-via-sql"],
            [
                "GET /ledger/transfers/{id} returns the transfer with large integers encoded as strings.",
                "/query can read ledger_transfers by ledger.",
                "A missing transfer lookup returns 404.",
            ],
            "P0",
            scenario_ledger_transfer_lookup_and_query_projection,
        ),
        UatCase(
            "UAT-LEDGER-006",
            "Ledger pending transfer errors use documented result codes",
            "Fintech evaluator",
            "Payment clients can branch on pending-transfer lifecycle failures without parsing prose.",
            ["docs/api/ledger.md#two-phase-transfers", "docs/api/ledger.md#result-codes"],
            [
                "Posting a missing pending transfer returns pending_transfer_not_found.",
                "Posting a pending transfer once succeeds.",
                "Posting it again returns pending_transfer_already_posted.",
            ],
            "P0",
            scenario_ledger_pending_error_codes,
        ),
        UatCase(
            "UAT-LEDGER-007",
            "Ledger credits_must_not_exceed_debits constraint is enforced",
            "Fintech evaluator",
            "Account-level balance constraints protect configured accounts from invalid postings.",
            ["docs/api/ledger.md#account-flags", "docs/api/ledger.md#result-codes"],
            [
                "A credit that would exceed debits is rejected with exceeds_debits.",
                "The rejected transfer leaves the constrained account unchanged.",
            ],
            "P0",
            scenario_ledger_credits_must_not_exceed_debits,
        ),
        UatCase(
            "UAT-LEDGER-008",
            "Ledger user_data fields round-trip through canonical APIs and SQL projection",
            "Fintech evaluator",
            "Business metadata attached to accounts and transfers is preserved without JSON integer truncation.",
            ["docs/api/ledger.md#large-integers-cross-the-wire-as-strings", "docs/api/ledger.md#querying-balances-via-sql"],
            [
                "Account user_data_128, user_data_64, and user_data_32 round-trip through GET /ledger/accounts.",
                "Transfer user_data_128, user_data_64, and user_data_32 round-trip through GET /ledger/transfers.",
                "ledger_accounts and ledger_transfers SQL projections preserve the same values.",
            ],
            "P0",
            scenario_ledger_user_data_roundtrip_and_sql_projection,
        ),
        UatCase(
            "UAT-LEDGER-009",
            "Ledger voiding an already-voided pending transfer returns a precise result code",
            "Fintech evaluator",
            "Payment systems can distinguish already-voided reservations from other pending lifecycle failures.",
            ["docs/api/ledger.md#two-phase-transfers", "docs/api/ledger.md#result-codes"],
            [
                "A pending transfer can be voided once.",
                "A second void of the same pending returns pending_transfer_already_voided.",
                "The rejected second void does not change balances.",
            ],
            "P0",
            scenario_ledger_void_already_voided_error,
        ),
        UatCase(
            "UAT-EVIDENCE-001",
            "Evidence chains provide append, idempotency, proofs, and erasure",
            "Compliance/audit user",
            "Audit workflows can record tamper-evident entries and redact payloads without allowing verified hard deletes.",
            ["docs/evidence/chains.md"],
            [
                "Verified chain can be created.",
                "Append assigns dense sequence 1.",
                "Idempotent replay returns the same result.",
                "Digest and proof endpoints work.",
                "Redaction hides payload.",
                "Hard delete on verified chain is rejected.",
            ],
            "P0",
            scenario_evidence_workflow,
        ),
        UatCase(
            "UAT-EVIDENCE-002",
            "Plain evidence chains allow hard-delete but no Merkle proofs",
            "Compliance/audit user",
            "Teams can choose between tamper-evident verified chains and deniable plain chains.",
            ["docs/evidence/chains.md#verified-vs-plain-chains", "docs/evidence/chains.md#erasure"],
            [
                "Plain chain digest is rejected as not verified.",
                "Hard-delete removes an entry from a plain chain.",
                "Remaining entries preserve their sequence numbers.",
            ],
            "P1",
            scenario_evidence_plain_chain_workflow,
        ),
        UatCase(
            "UAT-EVIDENCE-004",
            "Evidence append-with-edges updates the graph atomically and idempotently",
            "Compliance/audit user",
            "Lineage events can be recorded once and immediately traversed as graph relationships.",
            ["docs/evidence/chains.md#append-semantics", "docs/evidence/graph.md#append-with-edges"],
            [
                "An event with edges appends to a verified chain.",
                "The graph traversal sees the edge after the append.",
                "Idempotent replay returns the same response.",
                "Reusing the idempotency key with different events returns a conflict.",
            ],
            "P0",
            scenario_evidence_edges_and_idempotency_conflict,
        ),
        UatCase(
            "UAT-EVIDENCE-006",
            "Evidence Merkle head, range paging, consistency proofs, and redaction invariants hold",
            "Compliance/audit user",
            "Audit consumers can page chains, verify growth, and redact payloads without changing the verified digest.",
            ["docs/evidence/chains.md#http-surface", "docs/evidence/chains.md#merkle-verification-verified-chains", "docs/evidence/chains.md#erasure"],
            [
                "Five appends produce dense seq 1..5 and head 5.",
                "after/limit paging returns the expected range.",
                "Digest, inclusion proof, and consistency proof return expected shapes.",
                "Redaction omits payload and keeps the digest root unchanged.",
            ],
            "P0",
            scenario_evidence_merkle_range_and_consistency,
        ),
        UatCase(
            "UAT-EVIDENCE-007",
            "Evidence mode conflicts, tenant sequence isolation, and signing-off behavior are explicit",
            "Compliance/audit user",
            "Audit clients can detect chain-mode mistakes, tenant isolation, and unsigned deployments without ambiguity.",
            ["docs/evidence/chains.md#verified-vs-plain-chains", "docs/evidence/chains.md#multi-tenancy", "docs/evidence/chains.md#digest-signing"],
            [
                "Appending to a missing chain auto-creates it as verified.",
                "Re-creating with a different mode returns E_CHAIN_MODE_CONFLICT.",
                "Signed digest and signing-key endpoints return 501 when signing is off.",
                "The same chain name starts at sequence 1 in another tenant.",
            ],
            "P0",
            scenario_evidence_mode_tenant_and_signing_contracts,
        ),
        UatCase(
            "UAT-EVIDENCE-010",
            "Evidence named errors and invalid range parameters are explicit",
            "Compliance/audit user",
            "Audit clients can distinguish not-verified, verified-delete, missing-entry, and malformed-range failures.",
            ["docs/evidence/chains.md#named-errors", "docs/evidence/chains.md#http-surface"],
            [
                "Plain-chain digest and proof return E_NOT_VERIFIED.",
                "Hard-delete on a verified chain returns E_VERIFIED_NO_DELETE.",
                "Unknown proof and redact targets return 404.",
                "Negative range parameters are rejected.",
            ],
            "P0",
            scenario_evidence_named_errors_and_invalid_ranges,
        ),
        UatCase(
            "UAT-EVIDENCE-011",
            "Evidence batch append assigns dense ranges and supports paging",
            "Compliance/audit user",
            "Batch event ingestion can rely on gap-free sequence assignment and idempotent retry behavior.",
            ["docs/evidence/chains.md#append-semantics", "docs/evidence/chains.md#get-evidencechainentries-read-entries"],
            [
                "A three-event batch returns seqs [1, 2, 3].",
                "Idempotent batch replay returns the same response.",
                "Head reports the batch size.",
                "from/to and after/limit reads return the expected ranges.",
            ],
            "P0",
            scenario_evidence_batch_append_dense_ranges,
        ),
        UatCase(
            "UAT-EVIDENCE-012",
            "Plain evidence hard-delete retracts graph edges by default",
            "Compliance/audit user",
            "Plain-chain erasure can remove both the entry and its graph projection when requested by policy.",
            ["docs/evidence/chains.md#erasure", "docs/evidence/graph.md#append-with-edges"],
            [
                "A plain-chain entry with edges updates the graph.",
                "Hard-delete succeeds on the plain chain.",
                "The graph edge is retracted by default.",
            ],
            "P1",
            scenario_evidence_plain_delete_retracts_graph_edges,
        ),
        UatCase(
            "UAT-EVIDENCE-015",
            "Evidence unknown heads and repeated redaction are stable",
            "Compliance/audit user",
            "Audit clients can safely inspect missing chains and retry redaction without creating inconsistent state.",
            ["docs/evidence/chains.md#http-surface", "docs/evidence/chains.md#erasure"],
            [
                "The head of an unknown chain reports seq 0.",
                "Redacting an entry once succeeds.",
                "Redacting the same entry again succeeds idempotently.",
                "The entry remains redacted and payload-free.",
            ],
            "P0",
            scenario_evidence_unknown_head_and_idempotent_redaction,
        ),
        UatCase(
            "UAT-EVIDENCE-016",
            "Evidence consistency proof arguments are validated",
            "Compliance/audit user",
            "Proof consumers receive explicit errors for impossible consistency ranges.",
            ["docs/evidence/chains.md#merkle-verification-verified-chains", "docs/evidence/chains.md#named-errors"],
            [
                "from=0 is rejected.",
                "from greater than to is rejected.",
                "A target beyond the current head is rejected.",
            ],
            "P0",
            scenario_evidence_consistency_invalid_argument_contract,
        ),
        UatCase(
            "UAT-EVIDENCE-017",
            "Plain evidence hard-delete can preserve graph edges when requested",
            "Compliance/audit user",
            "Data-retention policies can remove plain-chain payloads without automatically retracting lineage edges.",
            ["docs/evidence/chains.md#erasure", "docs/evidence/graph.md#append-with-edges"],
            [
                "A plain-chain entry with edges updates the graph.",
                "Hard-delete with retract_edges=false succeeds.",
                "The graph edge remains traversable after deletion.",
            ],
            "P1",
            scenario_evidence_plain_delete_can_keep_graph_edges,
        ),
        UatCase(
            "UAT-EVIDENCE-003",
            "Native graph store supports edge writes and traversals",
            "Audit/lineage user",
            "Users can validate graph lineage features connected to evidence workflows.",
            ["docs/evidence/graph.md"],
            [
                "Edge upsert succeeds.",
                "Reachable traversal respects weight floor.",
                "Widest path reports the expected bottleneck.",
                "Atomic mutate and graph drop succeed.",
            ],
            "P1",
            scenario_graph_workflow,
        ),
        UatCase(
            "UAT-EVIDENCE-008",
            "Native graph store supports merge modes, edge delete, and disconnected path semantics",
            "Audit/lineage user",
            "Lineage clients can maintain graph projections and interpret disconnected/self path responses correctly.",
            ["docs/evidence/graph.md#standalone-edge-writes", "docs/evidence/graph.md#widest-path-max-bottleneck-path"],
            [
                "merge=max and merge=set update an existing edge as documented.",
                "Invalid merge is rejected.",
                "DELETE /graph/{graph}/edges removes the edge.",
                "Disconnected and self widest-path responses omit bottleneck.",
            ],
            "P1",
            scenario_graph_edge_delete_merge_and_disconnected,
        ),
        UatCase(
            "UAT-EVIDENCE-009",
            "Native graph namespaces support as-of sandbox analysis and cheap teardown",
            "Audit/lineage user",
            "Users can analyze a point-in-time graph projection without mutating the live graph.",
            ["docs/evidence/graph.md#as-of-analysis-replacing-scratch"],
            [
                "A sandbox graph can contain a subset of live edges.",
                "Live and sandbox traversals return different isolated cuts.",
                "Deleting the sandbox graph succeeds without touching the live graph.",
            ],
            "P1",
            scenario_graph_asof_sandbox_workflow,
        ),
        UatCase(
            "UAT-EVIDENCE-005",
            "Native graph store is isolated by tenant",
            "Audit/lineage user",
            "Multi-tenant lineage data does not leak between tenant keyspaces.",
            ["docs/evidence/graph.md", "docs/api/rest.md#tenant-selection"],
            [
                "A tenant-scoped graph edge is traversable with the tenant header.",
                "The default tenant cannot traverse the tenant graph.",
            ],
            "P0",
            scenario_graph_tenant_isolation,
        ),
        UatCase(
            "UAT-EVIDENCE-013",
            "Native graph store supports typed parallel edges and undirected traversal",
            "Audit/lineage user",
            "Lineage graphs can model multiple relationships between the same nodes and traverse them directionally or undirectionally.",
            ["docs/evidence/graph.md#edge-model", "docs/evidence/graph.md#traversal"],
            [
                "Parallel edges with distinct types are stored separately.",
                "Deleting one typed edge leaves the other edge intact.",
                "Directed reverse traversal is disconnected.",
                "Undirected traversal follows the remaining reverse edge.",
            ],
            "P1",
            scenario_graph_typed_parallel_and_undirected_edges,
        ),
        UatCase(
            "UAT-EVIDENCE-014",
            "Native graph reachable respects floors, multiple seeds, and unknown seeds",
            "Audit/lineage user",
            "Graph consumers can use deterministic reachable sets for filtered and multi-root lineage exploration.",
            ["docs/evidence/graph.md#reachable-set"],
            [
                "Weight floor prunes low-weight edges.",
                "Multiple seeds are included and deduplicated.",
                "Unknown seed nodes reach themselves only.",
            ],
            "P1",
            scenario_graph_floor_multi_seed_and_unknown_seed,
        ),
        UatCase(
            "UAT-EVIDENCE-018",
            "Native graph mutate gives deletes precedence and no-op deletes are explicit",
            "Audit/lineage user",
            "Graph maintenance jobs can send idempotent mutations without accidentally resurrecting deleted edges.",
            ["docs/evidence/graph.md#atomic-mutate", "docs/evidence/graph.md#delete-edges"],
            [
                "When the same edge is in upserts and deletes, the delete wins.",
                "Deleting a missing edge reports deleted:0.",
                "Dropping a missing graph reports dropped:0.",
            ],
            "P1",
            scenario_graph_mutate_delete_wins_and_noop_contract,
        ),
        UatCase(
            "UAT-EVIDENCE-019",
            "Native graph invalid requests fail without corrupting the graph",
            "Audit/lineage user",
            "Lineage ingestion can reject malformed edge payloads and continue accepting valid writes.",
            ["docs/evidence/graph.md#edge-model", "docs/evidence/graph.md#errors"],
            [
                "An edge missing dst is rejected.",
                "A delete request missing dst is rejected.",
                "A reachable request missing from is rejected.",
                "A later valid edge write still succeeds.",
            ],
            "P1",
            scenario_graph_invalid_edge_request_contract,
        ),
        UatCase(
            "UAT-LAKEHOUSE-001",
            "Lakehouse mirror is visible through Iceberg REST catalog",
            "Data platform evaluator",
            "Warehouse integrations can discover a mirrored table through the catalog API.",
            ["docs/lakehouse/iceberg-mirror.md"],
            ["PRAGMA enables mirroring.", "Inserted rows trigger a seal.", "Catalog lists and loads the mirrored table."],
            "P1",
            scenario_lakehouse_workflow,
        ),
        UatCase(
            "UAT-LAKEHOUSE-002",
            "Lakehouse catalog isolates tenant namespaces",
            "Data platform evaluator",
            "Multi-tenant warehouse consumers discover only the namespace that owns a mirrored table.",
            ["docs/lakehouse/iceberg-mirror.md#multi-tenancy"],
            [
                "A tenant-scoped mirrored table appears in that tenant's namespace.",
                "The same table does not leak into the default namespace.",
            ],
            "P1",
            scenario_lakehouse_tenant_catalog_workflow,
        ),
        UatCase(
            "UAT-LAKEHOUSE-003",
            "Iceberg REST catalog exposes config, namespaces, table listing, and loadTable",
            "Data platform evaluator",
            "Warehouse clients can use the standard catalog protocol shape, not just a bespoke table-list endpoint.",
            ["docs/lakehouse/iceberg-mirror.md#reading-from-a-warehouse", "docs/lakehouse/iceberg-mirror.md#attaching-as-a-durable-rest-catalog"],
            [
                "Global mirror PRAGMA causes new tables to be mirrored.",
                "Catalog config, listNamespaces, getNamespace, listTables, and loadTable return expected protocol shapes.",
                "Loading a missing table returns 404.",
            ],
            "P1",
            scenario_lakehouse_catalog_protocol_workflow,
        ),
        UatCase(
            "UAT-LAKEHOUSE-004",
            "Lakehouse mirror supports opt-out tables and compaction target PRAGMA",
            "Data platform evaluator",
            "Warehouse operators can enable mirror-by-default while excluding sensitive tables and setting compaction sizing.",
            ["docs/lakehouse/iceberg-mirror.md#enabling-the-mirror", "docs/lakehouse/iceberg-mirror.md#compaction"],
            [
                "PRAGMA lakehouse_target_file_bytes is accepted.",
                "Global mirror-on causes a normal table to appear in the catalog.",
                "A table opted out with lakehouse_mirror_table(..., off) does not appear or load.",
            ],
            "P1",
            scenario_lakehouse_opt_out_and_compaction_pragma,
        ),
        UatCase(
            "UAT-LAKEHOUSE-005",
            "Lakehouse global mirror-off still allows per-table opt-in",
            "Data platform evaluator",
            "Warehouse operators can keep mirror-by-default disabled while selectively publishing approved tables.",
            ["docs/lakehouse/iceberg-mirror.md#enabling-the-mirror"],
            [
                "PRAGMA lakehouse_mirror = off is accepted.",
                "A table opted in with lakehouse_mirror_table(..., on) appears in the catalog.",
                "A non-opted-in table created while global mirror is off does not appear or load.",
            ],
            "P1",
            scenario_lakehouse_global_off_with_table_opt_in,
        ),
    ]


def run_case(case: UatCase, client: BluedbClient, run_id: int) -> UatResult:
    start = time.time()
    ctx = UatContext(client, run_id)
    try:
        case.fn(ctx)
        return make_result(case, PASS, start, ctx.evidence)
    except Exception as exc:  # noqa: BLE001 - report UAT failures
        return make_result(case, FAIL, start, ctx.evidence, format_failure(exc))


def make_result(
    case: UatCase,
    status: str,
    start: float,
    evidence: list[Evidence],
    failure: str = "",
) -> UatResult:
    return UatResult(
        scenario_id=case.scenario_id,
        title=case.title,
        persona=case.persona,
        business_value=case.business_value,
        docs=case.docs,
        acceptance_criteria=case.acceptance_criteria,
        status=status,
        priority=case.priority,
        elapsed_ms=int((time.time() - start) * 1000),
        evidence=list(evidence),
        failure=failure,
        feature=case.feature,
        profiles=set(case.profiles),
        tags=set(case.tags),
    )


def format_failure(exc: Exception) -> str:
    if os.environ.get("BLUEDB_UAT_TRACEBACK"):
        return f"{exc}\n{traceback.format_exc()}"
    return str(exc)


def write_report(
    path: Path,
    *,
    results: list[UatResult],
    command: str,
    server_bin: Path,
    git_revision: str,
    base_url: str,
    started_at: str,
    ended_at: str,
    server_log_tail: str,
    ha_note: str,
    profile: str = "full",
    target_total: int = 1000,
) -> None:
    passed = sum(1 for result in results if result.status == PASS)
    failed = sum(1 for result in results if result.status == FAIL)
    blocked = sum(1 for result in results if result.status == BLOCKED)
    total = len(results)
    p0_failures = [result for result in results if result.status == FAIL and result.priority == "P0"]
    decision = "NO-GO" if p0_failures else ("CONDITIONAL GO" if failed or blocked else "GO")

    lines: list[str] = []
    lines.append("# bluedb UAT Report")
    lines.append("")
    lines.append(f"- **Decision**: {decision}")
    lines.append(f"- **Started**: {started_at}")
    lines.append(f"- **Ended**: {ended_at}")
    lines.append(f"- **Server binary**: `{server_bin}`")
    lines.append(f"- **Git revision**: `{git_revision}`")
    lines.append(f"- **Base URL**: `{base_url}`")
    lines.append(f"- **Profile**: `{profile}`")
    lines.append(f"- **Target cases**: {target_total}")
    lines.append(f"- **Command**: `{command}`")
    lines.append(f"- **Summary**: {passed} passed, {failed} failed, {blocked} blocked, {total} total")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "This UAT pass treats bluedb as a black box and validates user-visible behavior "
        "from the public docs. It starts a single local `bluedb-server` using a local "
        "filesystem object store, then drives documented HTTP APIs. It does not import "
        "Rust crates or call internal test helpers."
    )
    lines.append("")
    lines.append(f"HA/failover coverage: {ha_note}")
    lines.append("")
    lines.append("## Coverage Matrix")
    lines.append("")
    lines.append("| Area | Passed | Failed | Blocked | Total |")
    lines.append("|---|---:|---:|---:|---:|")
    areas = sorted({area_for(result) for result in results})
    for area in areas:
        scoped = [result for result in results if area_for(result) == area]
        area_passed = sum(1 for result in scoped if result.status == PASS)
        area_failed = sum(1 for result in scoped if result.status == FAIL)
        area_blocked = sum(1 for result in scoped if result.status == BLOCKED)
        lines.append(f"| {area} | {area_passed} | {area_failed} | {area_blocked} | {len(scoped)} |")
    lines.append("")
    lines.append("## Feature Coverage")
    lines.append("")
    lines.append("| Feature | Passed | Failed | Blocked | Total |")
    lines.append("|---|---:|---:|---:|---:|")
    features = sorted({result.feature or area_for(result) for result in results})
    for feature in features:
        scoped = [result for result in results if (result.feature or area_for(result)) == feature]
        feature_passed = sum(1 for result in scoped if result.status == PASS)
        feature_failed = sum(1 for result in scoped if result.status == FAIL)
        feature_blocked = sum(1 for result in scoped if result.status == BLOCKED)
        lines.append(f"| {feature} | {feature_passed} | {feature_failed} | {feature_blocked} | {len(scoped)} |")
    lines.append("")
    lines.append("## Priority Coverage")
    lines.append("")
    lines.append("| Priority | Passed | Failed | Blocked | Total |")
    lines.append("|---|---:|---:|---:|---:|")
    for priority in sorted({result.priority for result in results}):
        scoped = [result for result in results if result.priority == priority]
        priority_passed = sum(1 for result in scoped if result.status == PASS)
        priority_failed = sum(1 for result in scoped if result.status == FAIL)
        priority_blocked = sum(1 for result in scoped if result.status == BLOCKED)
        lines.append(f"| {priority} | {priority_passed} | {priority_failed} | {priority_blocked} | {len(scoped)} |")
    lines.append("")
    lines.append("## Profile Coverage")
    lines.append("")
    lines.append("| Profile | Cases in report |")
    lines.append("|---|---:|")
    profile_names = sorted({profile for result in results for profile in result.profiles})
    for profile_name in profile_names:
        count = sum(1 for result in results if profile_name in result.profiles)
        lines.append(f"| {profile_name} | {count} |")
    lines.append("")
    lines.append("## Document Coverage")
    lines.append("")
    lines.append("| Document reference | Cases |")
    lines.append("|---|---:|")
    doc_refs = sorted({doc for result in results for doc in result.docs})
    for doc in doc_refs:
        count = sum(1 for result in results if doc in result.docs)
        lines.append(f"| `{escape_md(doc)}` | {count} |")
    lines.append("")
    lines.append("## Scenario Summary")
    lines.append("")
    lines.append("| ID | Priority | Scenario | Persona | Status | Time |")
    lines.append("|---|---|---|---|---|---:|")
    for result in results:
        lines.append(
            f"| {result.scenario_id} | {result.priority} | {escape_md(result.title)} | "
            f"{escape_md(result.persona)} | {result.status} | {result.elapsed_ms} ms |"
        )
    lines.append("")

    failures = [result for result in results if result.status == FAIL]
    if failures:
        lines.append("## Findings")
        lines.append("")
        for result in failures:
            lines.append(f"### {result.scenario_id}: {result.title}")
            lines.append("")
            lines.append(f"- **Priority**: {result.priority}")
            lines.append(f"- **Persona**: {result.persona}")
            lines.append(f"- **Failure**: {result.failure}")
            lines.append(f"- **Docs**: {', '.join(f'`{doc}`' for doc in result.docs)}")
            lines.append("")
    else:
        lines.append("## Findings")
        lines.append("")
        lines.append("No UAT failures were observed.")
        lines.append("")

    lines.append("## Detailed Results")
    lines.append("")
    for result in results:
        lines.append(f"### {result.scenario_id}: {result.title}")
        lines.append("")
        lines.append(f"- **Status**: {result.status}")
        lines.append(f"- **Priority**: {result.priority}")
        lines.append(f"- **Persona**: {result.persona}")
        lines.append(f"- **Business value**: {result.business_value}")
        lines.append(f"- **Docs**: {', '.join(f'`{doc}`' for doc in result.docs)}")
        lines.append("")
        lines.append("Acceptance criteria:")
        for criterion in result.acceptance_criteria:
            lines.append(f"- {criterion}")
        if result.failure:
            lines.append("")
            lines.append(f"Failure detail: `{result.failure}`")
        if result.evidence:
            lines.append("")
            lines.append("Evidence:")
            for item in result.evidence:
                lines.append(f"- **{item.label}**: `{item.detail}`")
        lines.append("")

    if server_log_tail:
        lines.append("## Server Log Tail")
        lines.append("")
        lines.append("```text")
        lines.append(server_log_tail)
        lines.append("```")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def escape_md(value: str) -> str:
    return value.replace("|", "\\|")


def current_git_revision() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:  # noqa: BLE001 - report metadata should not block UAT
        return "unknown"
    return proc.stdout.strip() or "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bluedb UAT scenarios and write a Markdown report.")
    parser.add_argument("--server-bin", default="target/debug/bluedb-server")
    parser.add_argument("--port", type=int, default=18180)
    parser.add_argument("--auth-port", type=int, default=18181)
    parser.add_argument("--restart-port", type=int, default=18182)
    parser.add_argument("--admin-port", type=int, default=18183)
    parser.add_argument("--admin-ddl-port", type=int, default=18184)
    parser.add_argument("--report", default=None, help="Markdown report path. Defaults to uat/reports/<timestamp>-uat-report.md")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--skip-auth", action="store_true")
    parser.add_argument("--fail-on-uat-failure", action="store_true", help="Return non-zero when any UAT scenario fails.")
    parser.add_argument(
        "--ha-note",
        default="Not covered by this single-node UAT runner; run Docker/Compose or Jepsen separately.",
    )
    args = parser.parse_args()

    server_bin = Path(args.server_bin).resolve()
    if not server_bin.exists():
        raise SystemExit(f"server binary does not exist: {server_bin}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    report_path = Path(args.report or f"uat/reports/{stamp}-uat-report.md").resolve()
    started_at = time.strftime("%Y-%m-%d %H:%M:%S %z")
    command = " ".join(sys.argv)
    results: list[UatResult] = []
    server = ManagedServer(server_bin, args.port, keep_data=args.keep_data)
    server_log_tail = ""

    try:
        client = server.start()
        run_id = int(time.time() * 1000)
        for case in cases():
            result = run_case(case, client, run_id)
            results.append(result)
            print(f"{result.status} {result.scenario_id} {result.title}", flush=True)
        server_log_tail = server.log_tail()
    except Exception as exc:  # noqa: BLE001 - report startup failures
        now = time.time()
        results.append(
            UatResult(
                scenario_id="UAT-ENV-001",
                title="UAT environment starts",
                persona="QA engineer",
                business_value="The acceptance suite can launch the black-box server.",
                docs=["docs/deployment/local.md"],
                acceptance_criteria=["The server binary starts and /health becomes reachable."],
                status=BLOCKED,
                priority="P0",
                elapsed_ms=0,
                evidence=[Evidence("Server log tail", server.log_tail())],
                failure=format_failure(exc),
            )
        )
        server_log_tail = server.log_tail()
    finally:
        server.stop()

    restart_result = scenario_restart_durability_workflow(server_bin, args.restart_port, args.keep_data)
    results.append(restart_result)
    print(f"{restart_result.status} {restart_result.scenario_id} {restart_result.title}", flush=True)

    admin_result = scenario_admin_sql_enabled_workflow(server_bin, args.admin_port, args.keep_data)
    results.append(admin_result)
    print(f"{admin_result.status} {admin_result.scenario_id} {admin_result.title}", flush=True)

    admin_ddl_result = scenario_admin_sql_ddl_surface_workflow(server_bin, args.admin_ddl_port, args.keep_data)
    results.append(admin_ddl_result)
    print(f"{admin_ddl_result.status} {admin_ddl_result.scenario_id} {admin_ddl_result.title}", flush=True)

    if not args.skip_auth:
        auth_result = scenario_authorization_workflow(server_bin, args.auth_port, args.keep_data)
        results.append(auth_result)
        print(f"{auth_result.status} {auth_result.scenario_id} {auth_result.title}", flush=True)

    ended_at = time.strftime("%Y-%m-%d %H:%M:%S %z")
    write_report(
        report_path,
        results=results,
        command=command,
        server_bin=server_bin,
        git_revision=current_git_revision(),
        base_url=f"http://127.0.0.1:{args.port}",
        started_at=started_at,
        ended_at=ended_at,
        server_log_tail=server_log_tail,
        ha_note=args.ha_note,
    )

    passed = sum(1 for result in results if result.status == PASS)
    failed = sum(1 for result in results if result.status == FAIL)
    blocked = sum(1 for result in results if result.status == BLOCKED)
    print(f"\nUAT report: {report_path}")
    print(f"Summary: {passed} passed, {failed} failed, {blocked} blocked, {len(results)} total")

    if blocked:
        return 2
    if failed and args.fail_on_uat_failure:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
