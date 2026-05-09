"""Validation checks and runner for staging.fees."""

from __future__ import annotations

import pandas as pd

from ._base import check_freshness, validate_snapshot
from ._utils import Check
from ..storage import DuckDBWarehouse


def validate_fees(db: DuckDBWarehouse) -> list[Check]:
    return validate_snapshot(db, "staging.fees", "validate_fees", _build_checks)


def _build_checks(df: pd.DataFrame) -> list[Check]:
    return [
        _check_row_count(df),
        _check_no_null_cnpj(df),
        _check_adm_fee_coverage(df),
        _check_adm_fee_range(df),
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
        message=None if passed else "staging.fees is empty",
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


def _check_adm_fee_coverage(df: pd.DataFrame) -> Check:
    pct_null = df["adm_fee"].isna().mean() * 100
    passed = pct_null < 20.0
    return Check(
        name="adm_fee_coverage",
        passed=passed,
        severity="warning",
        value=f"{pct_null:.1f}%",
        threshold="< 20%",
        message=None if passed else f"{pct_null:.1f}% of funds missing adm_fee",
    )


def _check_adm_fee_range(df: pd.DataFrame) -> Check:
    out_of_range = int(((df["adm_fee"] < 0) | (df["adm_fee"] > 5)).sum())
    passed = out_of_range == 0
    return Check(
        name="adm_fee_range",
        passed=passed,
        severity="warning",
        value=str(out_of_range),
        threshold="0",
        message=(
            None if passed else f"{out_of_range} funds with adm_fee outside [0, 5%]"
        ),
    )
