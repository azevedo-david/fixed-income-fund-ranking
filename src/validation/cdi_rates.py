"""Validation checks and runner for staging.cdi_rates."""

from __future__ import annotations

from datetime import date

import pandas as pd

from ._base import validate_timeseries
from ._utils import Check
from ..storage import DuckDBWarehouse


def validate_cdi_rates(db: DuckDBWarehouse) -> list[Check]:
    return validate_timeseries(
        db, "staging.cdi_rates", "validate_cdi_rates", _build_checks
    )


def _build_checks(df: pd.DataFrame) -> list[Check]:
    return [
        _check_row_count(df),
        _check_no_null_rate(df),
        _check_date_coverage(df),
    ]


def _check_row_count(df: pd.DataFrame) -> Check:
    n = len(df)
    passed = n > 0
    return Check(
        name="row_count_positive",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="> 0",
        message=None if passed else "staging.cdi_rates is empty",
    )


def _check_no_null_rate(df: pd.DataFrame) -> Check:
    n_null = int(df["rate"].isna().sum())
    passed = n_null == 0
    return Check(
        name="no_null_rate",
        passed=passed,
        severity="error",
        value=str(n_null),
        threshold="0",
        message=None if passed else f"{n_null} rows have null rate",
    )


def _check_date_coverage(df: pd.DataFrame) -> Check:
    today = date.today()
    max_date = df["date"].max() if not df.empty else None
    if max_date is None:
        return Check(
            name="date_coverage",
            passed=False,
            severity="warning",
            value=None,
            threshold=f"<= 7 days before {today}",
            message="could not determine date coverage",
        )
    gap = (pd.Timestamp(today) - pd.Timestamp(max_date)).days
    passed = gap <= 7
    return Check(
        name="date_coverage",
        passed=passed,
        severity="warning",
        value=str(max_date),
        threshold=f"<= 7 days before {today}",
        message=(
            None
            if passed
            else f"latest CDI rate is {max_date}, {gap} days before today"
        ),
    )
