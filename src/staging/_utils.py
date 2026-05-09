"""Shared staging utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..storage import DuckDBWarehouse


def fmt_cnpj(value: str | int) -> str:
    """Format any CNPJ value (raw digits, integer, or already-formatted) to XX.XXX.XXX/XXXX-XX."""
    d = "".join(c for c in str(value) if c.isdigit()).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


@dataclass
class Check:
    name: str
    passed: bool
    severity: str  # "error" | "warning" | "info"
    value: str | None = None
    threshold: str | None = None
    message: str | None = None


def log_checks(
    db: "DuckDBWarehouse",
    checks: list[Check],
    dataset: str,
    task: str,
    reference_date: date,
) -> None:
    """Write a list of Check results to logs.validation_log."""
    now = datetime.now(timezone.utc)
    for c in checks:
        db.execute(
            """
            INSERT INTO logs.validation_log
                (reference_date, task, dataset, check_name, severity,
                 passed, value, threshold, message, logged_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                reference_date,
                task,
                dataset,
                c.name,
                c.severity,
                bool(c.passed),
                c.value,
                c.threshold,
                c.message,
                now,
            ],
        )
