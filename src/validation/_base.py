"""Shared validation helpers: freshness check and snapshot/timeseries validate patterns."""

from __future__ import annotations

from datetime import date

import pandas as pd

from ._utils import Check, log_checks
from ..storage import DuckDBWarehouse


def check_freshness(df: pd.DataFrame, date_col: str = "reference_date") -> Check:
    """Warn if the snapshot acquisition date is more than 7 days before today."""
    today = date.today()
    acquired = df[date_col].dropna().max()
    if pd.isna(acquired):
        return Check(
            name="freshness",
            passed=False,
            severity="warning",
            value=None,
            threshold=f"<= 7 days before {today}",
            message="cannot determine snapshot acquisition date",
        )
    gap = (pd.Timestamp(today) - pd.Timestamp(acquired)).days
    passed = gap <= 7
    return Check(
        name="freshness",
        passed=passed,
        severity="warning",
        value=str(acquired.date() if hasattr(acquired, "date") else acquired),
        threshold=f"<= 7 days before {today}",
        message=(
            None if passed else f"snapshot is {gap} days old (acquired: {acquired})"
        ),
    )


def validate_snapshot(
    db: DuckDBWarehouse,
    dataset: str,
    task: str,
    build_checks_fn,
) -> list[Check]:
    """Generic snapshot validate: read latest reference_date slice, run checks, raise on errors."""
    today = date.today()
    acquired_date = db.execute(f"SELECT MAX(reference_date) FROM {dataset}").fetchone()[
        0
    ]
    if acquired_date is None:
        checks = [
            Check(
                name="data_available",
                passed=False,
                severity="error",
                value=None,
                threshold="any snapshot",
                message=f"no snapshot found in {dataset}",
            )
        ]
        log_checks(db, checks, dataset, task, today)
        raise ValueError(f"{task}: error-level checks failed: data_available")
    df = db.execute(
        f"SELECT * FROM {dataset} WHERE reference_date = ?", [acquired_date]
    ).df()
    checks = build_checks_fn(df)
    log_checks(db, checks, dataset, task, today)
    failed = [c for c in checks if not c.passed and c.severity == "error"]
    if failed:
        names = ", ".join(c.name for c in failed)
        raise ValueError(f"{task}: error-level checks failed: {names}")
    return checks


def validate_timeseries(
    db: DuckDBWarehouse,
    dataset: str,
    task: str,
    build_checks_fn,
) -> list[Check]:
    """Generic timeseries validate: read full table, run checks, raise on errors."""
    today = date.today()
    df = db.execute(f"SELECT * FROM {dataset}").df()
    checks = build_checks_fn(df)
    log_checks(db, checks, dataset, task, today)
    failed = [c for c in checks if not c.passed and c.severity == "error"]
    if failed:
        names = ", ".join(c.name for c in failed)
        raise ValueError(f"{task}: error-level checks failed: {names}")
    return checks
