"""Adapters for preserved broad UAT workflows."""

from __future__ import annotations

from .. import legacy


def cases_by_id(*scenario_ids: str) -> list:
    wanted = set(scenario_ids)
    selected = []
    for case in legacy.cases():
        if case.scenario_id in wanted:
            selected.append(case)
    found = {case.scenario_id for case in selected}
    missing = sorted(wanted - found)
    if missing:
        raise ValueError(f"unknown legacy UAT case IDs: {missing}")
    return selected
