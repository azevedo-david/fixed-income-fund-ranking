"""Validation checks and runner for staging.anbima."""

from __future__ import annotations

import pandas as pd

from ._base import check_freshness, validate_snapshot
from ._utils import Check
from ..storage import DuckDBWarehouse


def validate_anbima(db: DuckDBWarehouse) -> list[Check]:
    return validate_snapshot(db, "staging.anbima", "validate_anbima", _build_checks)


def _build_checks(df: pd.DataFrame) -> list[Check]:
    return [
        _check_row_count(df),
        _check_no_null_cnpj(df),
        check_freshness(df),
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
        message=None if passed else "staging.anbima is empty",
    )


def _check_no_null_cnpj(df: pd.DataFrame) -> Check:
    n_null = int(df["fund_cnpj"].isna().sum())
    passed = n_null == 0
    return Check(
        name="no_null_fund_cnpj",
        passed=passed,
        severity="error",
        value=str(n_null),
        threshold="0",
        message=None if passed else f"{n_null} rows have null fund_cnpj",
    )
