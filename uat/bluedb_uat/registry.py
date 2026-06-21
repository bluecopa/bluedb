"""Case registry and metadata enforcement."""

from __future__ import annotations

from collections.abc import Iterable
from importlib import import_module

from .core import UatCase
from .profiles import DEFAULT_PROFILE, included_in_profile, normalize_profiles


FEATURE_MODULES = [
    "quickstart",
    "ops",
    "security",
    "tenancy",
    "rest_schema",
    "rest_tables",
    "sql_statements",
    "sql_guardrails",
    "sql_query_syntax",
    "sql_expressions",
    "sql_functions",
    "sql_types",
    "sql_json",
    "sql_metadata",
    "sql_search",
    "collections_crud",
    "collections_filters",
    "collections_updates",
    "collections_indexes",
    "collections_aggregation",
    "collections_search",
    "ledger",
    "evidence_chains",
    "graph",
    "lakehouse",
]


PREFIX_FEATURES = {
    "OPS": "operations",
    "QS": "quickstart",
    "REST": "rest.tables",
    "SCHEMA": "rest.schema",
    "SQL": "sql",
    "TENANT": "tenancy",
    "SEARCH": "sql.search",
    "COLL": "collections",
    "LEDGER": "ledger",
    "EVIDENCE": "evidence",
    "LAKEHOUSE": "lakehouse",
    "SEC": "security",
    "ENV": "environment",
}


NEGATIVE_WORDS = (
    "reject",
    "error",
    "unsupported",
    "disabled",
    "guardrail",
    "invalid",
    "missing",
    "duplicate",
    "denied",
)


SPECIAL_CASES = {
    "UAT-OPS-002": {
        "title": "Acknowledged writes survive a local server restart",
        "feature": "operations.restart",
        "priority": "P0",
        "profiles": {"smoke", "core", "full"},
        "docs": ["docs/guarantees/consistency.md", "docs/deployment/local.md"],
    },
    "UAT-OPS-003": {
        "title": "Opt-in admin SQL supports raw DDL, scripts, and transactions",
        "feature": "operations.admin_sql",
        "priority": "P1",
        "profiles": {"core", "full"},
        "docs": ["docs/api/rest.md#post-adminsql-arbitrary-sql-off-by-default"],
    },
    "UAT-SQL-009": {
        "title": "Admin SQL covers documented DDL, composite primary keys, views, and CTAS",
        "feature": "sql.admin_ddl",
        "priority": "P1",
        "profiles": {"core", "full"},
        "docs": ["docs/sql/statements.md", "docs/api/rest.md#post-adminsql-arbitrary-sql-off-by-default"],
    },
    "UAT-SEC-001": {
        "title": "Bearer scopes and tenant-bound tokens protect production APIs",
        "feature": "security.authz",
        "priority": "P0",
        "profiles": {"smoke", "core", "full", "negative"},
        "docs": ["docs/api/rest.md#authorization", "docs/deployment/configuration.md#api-surface--authorization"],
    },
}


def generated_cases() -> list[UatCase]:
    all_cases: list[UatCase] = []
    for module_name in FEATURE_MODULES:
        module = import_module(f"bluedb_uat.features.{module_name}")
        module_cases = module.cases()
        for case in module_cases:
            apply_default_metadata(case, module_name)
            all_cases.append(case)
    return all_cases


def all_cases() -> list[UatCase]:
    cases = generated_cases()
    validate_cases(cases)
    return cases


def selected_cases(
    *,
    profile: str = DEFAULT_PROFILE,
    case_ids: set[str] | None = None,
    features: set[str] | None = None,
) -> list[UatCase]:
    selected: list[UatCase] = []
    for case in all_cases():
        if case_ids and case.scenario_id not in case_ids:
            continue
        if features and not any(case.feature == feature or case.feature.startswith(feature + ".") for feature in features):
            continue
        if not included_in_profile(case.profiles, profile):
            continue
        selected.append(case)
    return selected


def selected_special_case_ids(
    *,
    profile: str = DEFAULT_PROFILE,
    case_ids: set[str] | None = None,
    features: set[str] | None = None,
    skip_auth: bool = False,
) -> list[str]:
    ids: list[str] = []
    for scenario_id, meta in SPECIAL_CASES.items():
        if skip_auth and scenario_id == "UAT-SEC-001":
            continue
        if case_ids and scenario_id not in case_ids:
            continue
        feature = str(meta["feature"])
        if features and not any(feature == item or feature.startswith(item + ".") for item in features):
            continue
        if not included_in_profile(set(meta["profiles"]), profile):
            continue
        ids.append(scenario_id)
    return ids


def list_case_rows(skip_auth: bool = False) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for case in all_cases():
        rows.append(
            {
                "id": case.scenario_id,
                "feature": case.feature,
                "priority": case.priority,
                "profiles": ",".join(sorted(case.profiles)),
                "title": case.title,
                "docs": ",".join(case.docs),
            }
        )
    for scenario_id, meta in SPECIAL_CASES.items():
        if skip_auth and scenario_id == "UAT-SEC-001":
            continue
        rows.append(
            {
                "id": scenario_id,
                "feature": str(meta["feature"]),
                "priority": str(meta["priority"]),
                "profiles": ",".join(sorted(meta["profiles"])),
                "title": str(meta["title"]),
                "docs": ",".join(meta["docs"]),
            }
        )
    validate_ids(row["id"] for row in rows)
    return rows


def apply_default_metadata(case: UatCase, module_name: str | None = None) -> None:
    if not case.feature:
        case.feature = infer_feature(case.scenario_id, module_name)
    case.profiles = normalize_profiles(set(case.profiles))
    if case.profiles == {"full"}:
        if case.priority == "P0":
            case.profiles = {"smoke", "core", "full"}
        elif case.priority == "P1":
            case.profiles = {"core", "full"}
    title = case.title.lower()
    if any(word in title for word in NEGATIVE_WORDS):
        case.tags.add("negative")
        case.profiles.add("negative")
    if not case.docs:
        raise ValueError(f"{case.scenario_id} is missing docs metadata")
    if not case.feature:
        raise ValueError(f"{case.scenario_id} is missing feature metadata")


def infer_feature(scenario_id: str, module_name: str | None = None) -> str:
    if module_name:
        feature = module_name.replace("_", ".")
        return {"ops": "operations"}.get(feature, feature)
    parts = scenario_id.split("-")
    if len(parts) < 2:
        return "environment"
    if parts[1] == "COLL" and len(parts) > 2 and parts[2] == "SEARCH":
        return "collections.search"
    if parts[1] == "EVIDENCE" and scenario_id in {
        "UAT-EVIDENCE-003",
        "UAT-EVIDENCE-005",
        "UAT-EVIDENCE-008",
        "UAT-EVIDENCE-009",
        "UAT-EVIDENCE-013",
        "UAT-EVIDENCE-014",
        "UAT-EVIDENCE-018",
        "UAT-EVIDENCE-019",
    }:
        return "graph"
    return PREFIX_FEATURES.get(parts[1], parts[1].lower())


def validate_cases(cases: Iterable[UatCase]) -> None:
    materialized = list(cases)
    validate_ids(case.scenario_id for case in materialized)
    for case in materialized:
        if not case.feature:
            raise ValueError(f"{case.scenario_id} is missing feature")
        if not case.docs:
            raise ValueError(f"{case.scenario_id} is missing docs")
        if not case.profiles:
            raise ValueError(f"{case.scenario_id} is missing profiles")


def validate_ids(ids: Iterable[str]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for scenario_id in ids:
        if scenario_id in seen:
            duplicates.add(scenario_id)
        seen.add(scenario_id)
    if duplicates:
        raise ValueError(f"duplicate UAT case IDs: {sorted(duplicates)}")
