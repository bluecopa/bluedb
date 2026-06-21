"""Ledger UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-LEDGER-001",
        "UAT-LEDGER-002",
        "UAT-LEDGER-003",
        "UAT-LEDGER-004",
        "UAT-LEDGER-005",
        "UAT-LEDGER-006",
        "UAT-LEDGER-007",
        "UAT-LEDGER-008",
        "UAT-LEDGER-009",
    )
