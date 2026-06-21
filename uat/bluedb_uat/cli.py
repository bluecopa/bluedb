"""Command-line runner for the modular UAT suite."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from . import legacy
from .core import BLOCKED, FAIL, PASS, Evidence, ManagedServer, UatResult, current_git_revision, format_failure, run_case
from .profiles import DEFAULT_PROFILE, VALID_PROFILES
from .registry import (
    SPECIAL_CASES,
    list_case_rows,
    selected_cases,
    selected_special_case_ids,
)


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
    parser.add_argument("--profile", choices=sorted(VALID_PROFILES), default=DEFAULT_PROFILE)
    parser.add_argument("--list-cases", action="store_true", help="Print registered cases and exit.")
    parser.add_argument("--case-id", action="append", default=[], help="Run only a case ID. May be repeated or comma-separated.")
    parser.add_argument("--feature", action="append", default=[], help="Run only a feature prefix. May be repeated or comma-separated.")
    parser.add_argument(
        "--ha-note",
        default="Not covered by this single-node UAT runner; run Docker/Compose or Jepsen separately.",
    )
    args = parser.parse_args()

    case_ids = split_filters(args.case_id)
    features = split_filters(args.feature)

    if args.list_cases:
        rows = list_case_rows(skip_auth=args.skip_auth)
        if case_ids:
            rows = [row for row in rows if row["id"] in case_ids]
        if features:
            rows = [
                row
                for row in rows
                if any(row["feature"] == feature or row["feature"].startswith(feature + ".") for feature in features)
            ]
        rows = [row for row in rows if args.profile in set(row["profiles"].split(","))]
        print("ID\tFEATURE\tPRIORITY\tPROFILES\tTITLE\tDOCS")
        for row in rows:
            print(f"{row['id']}\t{row['feature']}\t{row['priority']}\t{row['profiles']}\t{row['title']}\t{row['docs']}")
        print(f"\nTotal cases: {len(rows)}")
        return 0

    server_bin = Path(args.server_bin).resolve()
    if not server_bin.exists():
        raise SystemExit(f"server binary does not exist: {server_bin}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    report_path = Path(args.report or f"uat/reports/{stamp}-uat-report.md").resolve()
    started_at = time.strftime("%Y-%m-%d %H:%M:%S %z")
    command = " ".join(sys.argv)
    results: list[UatResult] = []
    server_log_tail = ""

    normal_cases = selected_cases(profile=args.profile, case_ids=case_ids, features=features)
    special_ids = selected_special_case_ids(
        profile=args.profile,
        case_ids=case_ids,
        features=features,
        skip_auth=args.skip_auth,
    )

    if normal_cases:
        server = ManagedServer(server_bin, args.port, keep_data=args.keep_data)
        try:
            client = server.start()
            run_id = int(time.time() * 1000)
            for case in normal_cases:
                result = run_case(case, client, run_id)
                results.append(result)
                print(f"{result.status} {result.scenario_id} {result.title}", flush=True)
            server_log_tail = server.log_tail()
        except Exception as exc:  # noqa: BLE001 - report startup failures
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
                    feature="environment",
                    profiles={args.profile},
                )
            )
            server_log_tail = server.log_tail()
        finally:
            server.stop()

    for special_id in special_ids:
        result = run_special_case(special_id, server_bin, args)
        apply_special_metadata(result, special_id)
        results.append(result)
        print(f"{result.status} {result.scenario_id} {result.title}", flush=True)

    ended_at = time.strftime("%Y-%m-%d %H:%M:%S %z")
    legacy.write_report(
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
        profile=args.profile,
        target_total=1000,
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


def run_special_case(scenario_id: str, server_bin: Path, args: argparse.Namespace) -> UatResult:
    if scenario_id == "UAT-OPS-002":
        return legacy.scenario_restart_durability_workflow(server_bin, args.restart_port, args.keep_data)
    if scenario_id == "UAT-OPS-003":
        return legacy.scenario_admin_sql_enabled_workflow(server_bin, args.admin_port, args.keep_data)
    if scenario_id == "UAT-SQL-009":
        return legacy.scenario_admin_sql_ddl_surface_workflow(server_bin, args.admin_ddl_port, args.keep_data)
    if scenario_id == "UAT-SEC-001":
        return legacy.scenario_authorization_workflow(server_bin, args.auth_port, args.keep_data)
    raise ValueError(f"unknown special case: {scenario_id}")


def apply_special_metadata(result: UatResult, scenario_id: str) -> None:
    meta = SPECIAL_CASES[scenario_id]
    result.feature = str(meta["feature"])
    result.profiles = set(meta["profiles"])
    if any(word in result.title.lower() for word in ("reject", "error", "disabled", "denied", "unauthorized")):
        result.tags.add("negative")


def split_filters(values: list[str]) -> set[str]:
    result: set[str] = set()
    for value in values:
        for item in value.split(","):
            stripped = item.strip()
            if stripped:
                result.add(stripped)
    return result


if __name__ == "__main__":
    sys.exit(main())
