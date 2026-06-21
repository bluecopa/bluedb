"""Evidence chains UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-EVIDENCE-001",
        "UAT-EVIDENCE-002",
        "UAT-EVIDENCE-004",
        "UAT-EVIDENCE-006",
        "UAT-EVIDENCE-007",
        "UAT-EVIDENCE-010",
        "UAT-EVIDENCE-011",
        "UAT-EVIDENCE-012",
        "UAT-EVIDENCE-015",
        "UAT-EVIDENCE-016",
        "UAT-EVIDENCE-017",
    )
