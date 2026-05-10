"""Validation checks for staging.* tables (post-staging gate)."""

from __future__ import annotations

from datetime import date

from ._base import (
    ValidateResult,
    check_and_gate,
    check_column_no_nulls,
    check_date_freshness,
    check_pk_unique,
    check_row_count,
    check_snapshot_exists,
    snapshot_date,
)
from ..storage import DuckDBWarehouse

_TASK = "validate_staging"
_DATASET = "staging"


def validate_staging(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    """Run all staging-layer checks, write to log, and raise on error-level failures."""
    return check_and_gate(
        db, _build_checks(db, reference_date), _TASK, _DATASET, reference_date
    )


def _build_checks(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        *_checks_registry(db, reference_date),
        *_checks_daily_quotes(db, reference_date),
        *_checks_fees(db, reference_date),
        *_checks_cdi_rates(db, reference_date),
        *_checks_anbima(db, reference_date),
    ]


def _checks_registry(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "staging.registry",
            reference_date,
            name="registry_snapshot_exists",
        ),
        check_row_count(
            db,
            "staging.registry",
            name="registry_row_count",
            reference_date=reference_date,
        ),
        check_date_freshness(
            db,
            "staging.registry",
            "reference_date",
            reference_date,
            name="registry_snapshot_freshness",
            max_lag_days=14,
        ),
        check_column_no_nulls(
            db,
            "staging.registry",
            "fund_cnpj",
            name="registry_fund_cnpj_not_null",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "staging.registry",
            ["fund_cnpj", "subclass_id"],
            name="registry_pk_unique",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "staging.registry",
            "inception_date",
            name="registry_inception_date_not_null",
            reference_date=reference_date,
            severity="warning",
        ),
    ]


def _checks_daily_quotes(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_row_count(db, "staging.daily_quotes", name="daily_quotes_row_count"),
        check_date_freshness(
            db,
            "staging.daily_quotes",
            "date",
            reference_date,
            name="daily_quotes_date_freshness",
        ),
        check_column_no_nulls(
            db,
            "staging.daily_quotes",
            "fund_cnpj",
            name="daily_quotes_fund_cnpj_not_null",
        ),
        _daily_quotes_nav_null_rate(db),
    ]


def _checks_fees(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "staging.fees",
            reference_date,
            name="fees_snapshot_exists",
        ),
        check_row_count(
            db,
            "staging.fees",
            name="fees_row_count",
            reference_date=reference_date,
        ),
        check_date_freshness(
            db,
            "staging.fees",
            "reference_date",
            reference_date,
            name="fees_snapshot_freshness",
            max_lag_days=14,
        ),
        check_column_no_nulls(
            db,
            "staging.fees",
            "fund_cnpj",
            name="fees_fund_cnpj_not_null",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "staging.fees",
            ["fund_cnpj"],
            name="fees_fund_cnpj_unique",
            reference_date=reference_date,
        ),
        _fees_adm_fee_not_negative(db, reference_date),
    ]


def _checks_cdi_rates(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_row_count(db, "staging.cdi_rates", name="cdi_rates_row_count"),
        check_date_freshness(
            db,
            "staging.cdi_rates",
            "date",
            reference_date,
            name="cdi_rates_date_freshness",
        ),
        _cdi_rates_rate_not_negative(db),
        _cdi_rates_no_large_gaps(db),
    ]


def _checks_anbima(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "staging.anbima",
            reference_date,
            name="anbima_staging_snapshot_exists",
        ),
        check_row_count(
            db,
            "staging.anbima",
            name="anbima_staging_row_count",
            reference_date=reference_date,
        ),
        check_date_freshness(
            db,
            "staging.anbima",
            "reference_date",
            reference_date,
            name="anbima_staging_snapshot_freshness",
            max_lag_days=14,
        ),
        check_column_no_nulls(
            db,
            "staging.anbima",
            "fund_cnpj",
            name="anbima_fund_cnpj_not_null",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "staging.anbima",
            ["fund_cnpj", "subclass_id"],
            name="anbima_pk_unique",
            reference_date=reference_date,
        ),
    ]


def _daily_quotes_nav_null_rate(db: DuckDBWarehouse) -> ValidateResult:
    """Post-ffill null rate > 50% means most funds have no valid starting NAV — returns will be entirely NaN."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN nav IS NULL THEN 1.0 ELSE 0.0 END) FROM staging.daily_quotes"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="daily_quotes_nav_null_rate",
        passed=passed,
        severity="error",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of nav is null after ffill — returns series will be NaN for most funds"
        ),
    )


def _fees_adm_fee_not_negative(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    snap = snapshot_date(db, "staging.fees", reference_date)
    n = (
        db.execute(
            "SELECT COUNT(*) FROM staging.fees WHERE reference_date = ? AND adm_fee < 0",
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n == 0
    return ValidateResult(
        check_name="fees_adm_fee_not_negative",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} adm_fee values are negative — bps-to-% conversion may have misfired"
        ),
    )


def _cdi_rates_rate_not_negative(db: DuckDBWarehouse) -> ValidateResult:
    n = db.execute("SELECT COUNT(*) FROM staging.cdi_rates WHERE rate < 0").fetchone()[
        0
    ]
    passed = n == 0
    return ValidateResult(
        check_name="cdi_rates_rate_not_negative",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} CDI rates are negative — alpha sign will be inverted for all affected periods"
        ),
    )


def _cdi_rates_no_large_gaps(db: DuckDBWarehouse) -> ValidateResult:
    """Gap > 10 calendar days between consecutive CDI dates indicates missing data, not a normal weekend/holiday."""
    max_gap = db.execute("""
        SELECT MAX(DATEDIFF('day', prev_date, date)) AS max_gap
        FROM (
            SELECT date, LAG(date) OVER (ORDER BY date) AS prev_date
            FROM staging.cdi_rates
        ) t
        WHERE prev_date IS NOT NULL
        """).fetchone()[0]
    if max_gap is None:
        return ValidateResult(
            check_name="cdi_rates_no_large_gaps",
            passed=False,
            severity="warning",
            value=None,
            threshold="<= 10 days",
            message="could not compute CDI date gaps — table may be empty",
        )
    passed = max_gap <= 10
    return ValidateResult(
        check_name="cdi_rates_no_large_gaps",
        passed=passed,
        severity="warning",
        value=str(max_gap),
        threshold="<= 10 days",
        message=(
            None
            if passed
            else f"max consecutive gap in CDI dates is {max_gap} days — missing data understates window CDI returns"
        ),
    )
