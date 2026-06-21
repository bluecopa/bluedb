#!/usr/bin/env python3
"""Compatibility entrypoint for the modular bluedb UAT package."""

from __future__ import annotations

import sys
from pathlib import Path


if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from bluedb_uat.cli import main


if __name__ == "__main__":
    sys.exit(main())
