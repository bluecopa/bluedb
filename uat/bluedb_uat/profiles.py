"""Run profile definitions for the UAT suite."""

from __future__ import annotations


VALID_PROFILES = {"smoke", "core", "full", "negative"}
DEFAULT_PROFILE = "full"


def normalize_profiles(values: set[str] | None) -> set[str]:
    if not values:
        return {"full"}
    unknown = set(values) - VALID_PROFILES
    if unknown:
        raise ValueError(f"unknown UAT profiles: {sorted(unknown)}")
    return set(values)


def included_in_profile(case_profiles: set[str], selected: str) -> bool:
    if selected not in VALID_PROFILES:
        raise ValueError(f"unknown UAT profile: {selected}")
    return selected in case_profiles

