"""Lakehouse UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-LAKEHOUSE-001",
        "UAT-LAKEHOUSE-002",
        "UAT-LAKEHOUSE-003",
        "UAT-LAKEHOUSE-004",
        "UAT-LAKEHOUSE-005",
    )
