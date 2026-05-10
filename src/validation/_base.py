"""Shared validation infrastructure: ValidateResult dataclass, log writer, gate, and SQL check helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ..storage import DuckDBWarehouse


@dataclass
class ValidateResult:
    check_name: str
    passed: bool
    severity: Literal["error", "warning", "info"]
    value: str | None = None
    threshold: str | None = None
    message: str | None = None


def write_results(
    db: "DuckDBWarehouse",
    results: list[ValidateResult],
    task: str,
    dataset: str,
    reference_date: date,
) -> None:
    """Insert a list of ValidateResult rows into logs.validation_log."""
    for r in results:
        db.execute(
            """
            INSERT INTO logs.validation_log
                (reference_date, task, dataset, check_name, severity,
                 passed, value, threshold, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                reference_date,
                task,
                dataset,
                r.check_name,
                r.severity,
                r.passed,
                r.value,
                r.threshold,
                r.message,
            ],
        )


def check_and_gate(
    db: "DuckDBWarehouse",
    checks: list[ValidateResult],
    task: str,
    dataset: str,
    reference_date: date,
) -> list[ValidateResult]:
    """Write checks to the log and raise if any error-level check failed."""
    write_results(db, checks, task, dataset, reference_date)
    failures = [r for r in checks if not r.passed and r.severity == "error"]
    if failures:
        names = ", ".join(r.check_name for r in failures)
        raise ValueError(f"{task}: error-level checks failed: {names}")
    return checks


def snapshot_date(
    db: "DuckDBWarehouse", table: str, reference_date: date
) -> date | None:
    """Return the most recent reference_date in table on or before reference_date."""
    return db.execute(
        f"SELECT MAX(reference_date) FROM {table} WHERE reference_date <= ?",
        [reference_date],
    ).fetchone()[0]


def check_snapshot_exists(
    db: "DuckDBWarehouse",
    table: str,
    reference_date: date,
    *,
    name: str,
) -> ValidateResult:
    """Check that at least one snapshot exists on or before reference_date."""
    snap = snapshot_date(db, table, reference_date)
    passed = snap is not None
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity="error",
        value=str(snap) if snap else None,
        threshold=f"<= {reference_date}",
        message=(
            None if passed else f"no snapshot in {table} on or before {reference_date}"
        ),
    )


def check_row_count(
    db: "DuckDBWarehouse",
    table: str,
    *,
    name: str,
    reference_date: date | None = None,
) -> ValidateResult:
    """Check that the table (or its latest snapshot) has at least one row."""
    if reference_date is not None:
        snap = snapshot_date(db, table, reference_date)
        n = (
            db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE reference_date = ?", [snap]
            ).fetchone()[0]
            if snap is not None
            else 0
        )
    else:
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = n > 0
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity="error",
        value=str(n),
        threshold="> 0",
        message=None if passed else f"{table} has 0 rows",
    )


def check_column_no_nulls(
    db: "DuckDBWarehouse",
    table: str,
    column: str,
    *,
    name: str,
    reference_date: date | None = None,
    severity: Literal["error", "warning", "info"] = "error",
) -> ValidateResult:
    """Check that a column contains no null values."""
    if reference_date is not None:
        snap = snapshot_date(db, table, reference_date)
        n_null = (
            db.execute(
                f'SELECT COUNT(*) FROM {table} WHERE reference_date = ? AND "{column}" IS NULL',
                [snap],
            ).fetchone()[0]
            if snap is not None
            else 0
        )
    else:
        n_null = db.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{column}" IS NULL'
        ).fetchone()[0]
    passed = n_null == 0
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity=severity,
        value=str(n_null),
        threshold="0",
        message=None if passed else f"{n_null} null values in {table}.{column}",
    )


def check_date_freshness(
    db: "DuckDBWarehouse",
    table: str,
    date_col: str,
    reference_date: date,
    *,
    name: str,
    max_lag_days: int = 7,
) -> ValidateResult:
    """Check that the maximum date value is within max_lag_days of reference_date."""
    cutoff = reference_date - timedelta(days=max_lag_days)
    max_dt = db.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()[0]
    if max_dt is None:
        return ValidateResult(
            check_name=name,
            passed=False,
            severity="warning",
            value=None,
            threshold=f">= {cutoff}",
            message=f"could not determine max {date_col} in {table}",
        )
    passed = max_dt >= cutoff
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity="warning",
        value=str(max_dt),
        threshold=f">= {cutoff}",
        message=(
            None
            if passed
            else f"latest {date_col} is {max_dt}, more than {max_lag_days} days before {reference_date}"
        ),
    )


def check_pk_unique(
    db: "DuckDBWarehouse",
    table: str,
    columns: list[str],
    *,
    name: str,
    reference_date: date | None = None,
) -> ValidateResult:
    """Check that the given columns form a unique key within the table (or its latest snapshot)."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    if reference_date is not None:
        snap = snapshot_date(db, table, reference_date)
        n_dupes = (
            db.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {col_list}, COUNT(*) AS n
                    FROM {table}
                    WHERE reference_date = ?
                    GROUP BY {col_list}
                    HAVING COUNT(*) > 1
                )
                """,
                [snap],
            ).fetchone()[0]
            if snap is not None
            else 0
        )
    else:
        n_dupes = db.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {col_list}, COUNT(*) AS n
                FROM {table}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            )
            """).fetchone()[0]
    passed = n_dupes == 0
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity="error",
        value=str(n_dupes),
        threshold="0",
        message=(
            None
            if passed
            else f"{n_dupes} duplicate ({', '.join(columns)}) combinations in {table}"
        ),
    )


def check_known_values(
    db: "DuckDBWarehouse",
    table: str,
    column: str,
    known: set[str],
    *,
    name: str,
    reference_date: date | None = None,
) -> ValidateResult:
    """Warn if the column contains values outside the known set."""
    if reference_date is not None:
        snap = snapshot_date(db, table, reference_date)
        rows = (
            db.execute(
                f'SELECT DISTINCT "{column}" FROM {table} WHERE reference_date = ?',
                [snap],
            ).fetchall()
            if snap is not None
            else []
        )
    else:
        rows = db.execute(f'SELECT DISTINCT "{column}" FROM {table}').fetchall()
    found = {r[0] for r in rows if r[0] is not None}
    unknown = found - known
    passed = len(unknown) == 0
    return ValidateResult(
        check_name=name,
        passed=passed,
        severity="warning",
        value=", ".join(sorted(str(v) for v in unknown)) if unknown else None,
        threshold=f"subset of {{{', '.join(sorted(known))}}}",
        message=(
            None
            if passed
            else f"unknown values in {table}.{column}: {', '.join(sorted(str(v) for v in unknown))}"
        ),
    )
