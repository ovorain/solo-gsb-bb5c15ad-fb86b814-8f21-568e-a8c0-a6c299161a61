"""Application configuration read from environment variables."""

from __future__ import annotations

import os
from pathlib import Path


def database_path() -> str:
    """Return the SQLite database path (env var DATABASE_PATH overrides default)."""
    return os.environ.get("DATABASE_PATH", str(Path(__file__).resolve().parent.parent / "data" / "events.db"))


def server_port() -> int:
    try:
        return int(os.environ.get("APP_PORT", "8000"))
    except ValueError:
        return 8000


def lease_duration_seconds() -> int:
    """Lifetime of a claim lease (env var LEASE_DURATION_SECONDS overrides default)."""
    try:
        value = int(os.environ.get("LEASE_DURATION_SECONDS", "30"))
    except ValueError:
        return 30
    return value if value > 0 else 30
