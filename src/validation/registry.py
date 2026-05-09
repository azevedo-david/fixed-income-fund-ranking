"""Validation checks and runner for staging.registry."""

from __future__ import annotations

import logging
import re

import pandas as pd

from ._base import check_freshness, validate_snapshot
from ._utils import Check
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_CNPJ_RE = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")
_ACTIVE_STATUS = "Em Funcionamento Normal"
_KNOWN_STATUSES = {
    "Em Funcionamento Normal",
    "Em Liquidação",
    "Cancelado",
    "Em Fase Pré-Operacional",
    "Em Processo de Transformação",
}


def validate_registry(db: DuckDBWarehouse) -> list[Check]:
    return validate_snapshot(db, "staging.registry", "validate_registry", _build_checks)


def _build_checks(df: pd.DataFrame) -> list[Check]:
    return [
        _check_row_count(df),
        _check_no_null_cnpj(df),
        _check_cnpj_format(df),
        _check_active_status_present(df),
        _check_unknown_statuses(df),
        _check_inception_date_coverage(df),
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
        message=None if passed else "staging.registry is empty",
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


def _check_cnpj_format(df: pd.DataFrame) -> Check:
    n_bad = int(
        df["fund_cnpj"].dropna().map(lambda v: not bool(_CNPJ_RE.match(v))).sum()
    )
    passed = n_bad == 0
    return Check(
        name="cnpj_format",
        passed=passed,
        severity="error",
        value=str(n_bad),
        threshold="0",
        message=(
            None
            if passed
            else f"{n_bad} fund_cnpj values do not match XX.XXX.XXX/XXXX-XX"
        ),
    )


def _check_active_status_present(df: pd.DataFrame) -> Check:
    active = int((df["status"] == _ACTIVE_STATUS).sum())
    passed = active > 0
    return Check(
        name="active_status_present",
        passed=passed,
        severity="error",
        value=str(active),
        threshold="> 0",
        message=None if passed else f"no funds with status '{_ACTIVE_STATUS}' found",
    )


def _check_unknown_statuses(df: pd.DataFrame) -> Check:
    found = set(df["status"].dropna().unique())
    unknown = found - _KNOWN_STATUSES
    passed = len(unknown) == 0
    if not passed:
        logger.warning(
            "validate_registry: unknown statuses: %s", ", ".join(sorted(unknown))
        )
    return Check(
        name="unknown_statuses",
        passed=passed,
        severity="warning",
        value=", ".join(sorted(unknown)) if unknown else None,
        threshold="all statuses in known set",
        message=(
            None if passed else f"unknown status values: {', '.join(sorted(unknown))}"
        ),
    )


def _check_inception_date_coverage(df: pd.DataFrame) -> Check:
    pct_null = df["inception_date"].isna().mean() * 100
    passed = pct_null < 5.0
    return Check(
        name="inception_date_coverage",
        passed=passed,
        severity="warning",
        value=f"{pct_null:.1f}%",
        threshold="< 5%",
        message=None if passed else f"{pct_null:.1f}% of rows missing inception_date",
    )
