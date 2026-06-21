"""Shared UAT primitives.

The original 99-case harness lives in :mod:`bluedb_uat.legacy`; this module is
the stable import surface for new feature modules.
"""

from __future__ import annotations

from .legacy import (
    BLOCKED,
    FAIL,
    PASS,
    BluedbClient,
    Evidence,
    ManagedServer,
    Response,
    UatCase,
    UatContext,
    UatResult,
    compact,
    current_git_revision,
    format_failure,
    make_result,
    parse_json,
    query,
    require,
    require_2xx,
    require_error,
    rows,
    run_case,
    wait_until,
)


DEFAULT_PROFILES = {"full"}
CORE_PROFILES = {"core", "full"}
SMOKE_PROFILES = {"smoke", "core", "full"}
NEGATIVE_PROFILES = {"negative", "full"}

