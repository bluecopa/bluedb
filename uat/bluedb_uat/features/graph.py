"""Native graph UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-EVIDENCE-003",
        "UAT-EVIDENCE-005",
        "UAT-EVIDENCE-008",
        "UAT-EVIDENCE-009",
        "UAT-EVIDENCE-013",
        "UAT-EVIDENCE-014",
        "UAT-EVIDENCE-018",
        "UAT-EVIDENCE-019",
    )
