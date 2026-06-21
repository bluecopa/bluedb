"""Helpers for generating stable UAT cases from documented compatibility rows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .core import UatCase


def make_case(
    *,
    scenario_id: str,
    title: str,
    feature: str,
    docs: list[str],
    acceptance: list[str],
    fn: Callable[[Any], None],
    priority: str = "P2",
    persona: str = "Application developer",
    business_value: str = "The documented behavior is available through the public API.",
    profiles: set[str] | None = None,
    tags: set[str] | None = None,
) -> UatCase:
    return UatCase(
        scenario_id=scenario_id,
        title=title,
        persona=persona,
        business_value=business_value,
        docs=docs,
        acceptance_criteria=acceptance,
        priority=priority,
        fn=fn,
        feature=feature,
        profiles=profiles or {"full"},
        tags=tags or set(),
    )

