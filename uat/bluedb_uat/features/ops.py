"""Operations UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-OPS-001", "UAT-OPS-004")
