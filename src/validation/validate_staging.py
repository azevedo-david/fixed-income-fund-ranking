"""Validation checks for staging.* tables (post-staging gate)."""

from __future__ import annotations

from datetime import date, timedelta

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


def validate_staging(
    db: DuckDBWarehouse,
    reference_date: date,
    quotes_start: date | None = None,
) -> list[ValidateResult]:
    """Run all staging-layer checks, write to log, and raise on error-level failures."""
    return check_and_gate(
        db,
        _build_checks(db, reference_date, quotes_start),
        _TASK,
        _DATASET,
        reference_date,
    )


def _build_checks(
    db: DuckDBWarehouse,
    reference_date: date,
    quotes_start: date | None = None,
) -> list[ValidateResult]:
    return [
        *_checks_registry(db, reference_date),
        *_checks_daily_quotes(db, reference_date, quotes_start),
        *_checks_fees(db, reference_date),
        *_checks_cdi_rates(db, reference_date, quotes_start),
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
        check_column_no_nulls(
            db,
            "staging.registry",
            "fund_name",
            name="registry_fund_name_not_null",
            reference_date=reference_date,
            severity="warning",
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
    db: DuckDBWarehouse,
    reference_date: date,
    quotes_start: date | None = None,
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
        check_pk_unique(
            db,
            "staging.daily_quotes",
            ["fund_cnpj", "subclass_id", "date"],
            name="daily_quotes_pk_unique",
        ),
        _daily_quotes_window_coverage(db, quotes_start),
        _daily_quotes_nav_null_rate(db),
        _daily_quotes_aum_null_rate(db),
        _daily_quotes_shareholders_null_rate(db),
        _daily_quotes_nav_outliers(db),
        _daily_quotes_aum_outliers(db),
        _daily_quotes_shareholders_outliers(db),
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
    db: DuckDBWarehouse, reference_date: date, quotes_start: date | None = None
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
        _cdi_rates_window_coverage(db, quotes_start),
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
        _anbima_redemption_days_null_rate(db, reference_date),
    ]


def _daily_quotes_window_coverage(
    db: DuckDBWarehouse, quotes_start: date | None
) -> ValidateResult:
    """Warn if staging.daily_quotes doesn't cover the full trailing-return window — long-window alphas will be NaN for all funds."""
    min_date = db.execute("SELECT MIN(date) FROM staging.daily_quotes").fetchone()[0]
    if min_date is None or quotes_start is None:
        return ValidateResult(
            check_name="daily_quotes_window_coverage",
            passed=False,
            severity="warning",
            value=None,
            threshold=f"<= {quotes_start}",
            message="staging.daily_quotes is empty — cannot verify window coverage",
        )
    passed = min_date <= quotes_start
    return ValidateResult(
        check_name="daily_quotes_window_coverage",
        passed=passed,
        severity="warning",
        value=str(min_date),
        threshold=f"<= {quotes_start}",
        message=(
            None
            if passed
            else f"earliest quote date {min_date} is after required window start {quotes_start} — long-window trailing returns will be NaN for all funds"
        ),
    )


def _cdi_rates_window_coverage(
    db: DuckDBWarehouse, quotes_start: date | None
) -> ValidateResult:
    """Warn if staging.cdi_rates doesn't cover the full trailing-return window — CDI benchmark will be truncated for long windows."""
    min_date = db.execute("SELECT MIN(date) FROM staging.cdi_rates").fetchone()[0]
    if min_date is None or quotes_start is None:
        return ValidateResult(
            check_name="cdi_rates_window_coverage",
            passed=False,
            severity="warning",
            value=None,
            threshold=f"<= {quotes_start}",
            message="staging.cdi_rates is empty — cannot verify window coverage",
        )
    passed = min_date <= quotes_start
    return ValidateResult(
        check_name="cdi_rates_window_coverage",
        passed=passed,
        severity="warning",
        value=str(min_date),
        threshold=f"<= {quotes_start}",
        message=(
            None
            if passed
            else f"earliest CDI date {min_date} is after required window start {quotes_start} — alpha will be computed against a truncated benchmark"
        ),
    )


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


def _daily_quotes_aum_null_rate(db: DuckDBWarehouse) -> ValidateResult:
    """AuM null rate > 50% means median_aum will be null for most funds, silently excluding them from universe."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN aum IS NULL THEN 1.0 ELSE 0.0 END) FROM staging.daily_quotes"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="daily_quotes_aum_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of aum is null — most funds will have null median_aum and be excluded from universe"
        ),
    )


def _daily_quotes_shareholders_null_rate(db: DuckDBWarehouse) -> ValidateResult:
    """Shareholders null rate > 50% means most funds fail the min_cotistas filter and are silently excluded from universe."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN shareholders IS NULL THEN 1.0 ELSE 0.0 END) FROM staging.daily_quotes"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="daily_quotes_shareholders_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of shareholders is null — most funds will have null median_holders and be excluded from universe"
        ),
    )


def _daily_quotes_nav_outliers(db: DuckDBWarehouse) -> ValidateResult:
    """NAV > 1,000,000,000 is implausible even for the oldest funds (p99 ≈ 8,400)."""
    n_extreme = db.execute(
        "SELECT COUNT(*) FROM staging.daily_quotes WHERE nav IS NOT NULL AND nav > 1000000000"
    ).fetchone()[0]
    n = n_extreme
    passed = n == 0
    return ValidateResult(
        check_name="daily_quotes_nav_outliers",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n_extreme} rows with nav > 1,000,000 — likely data entry errors"
        ),
    )


def _daily_quotes_aum_outliers(db: DuckDBWarehouse) -> ValidateResult:
    """AuM > 1T BRL is implausible (largest Brazilian fund is well below that)."""
    n_extreme = db.execute(
        "SELECT COUNT(*) FROM staging.daily_quotes WHERE aum IS NOT NULL AND aum > 1000000000000"
    ).fetchone()[0]
    n = n_extreme
    passed = n == 0
    return ValidateResult(
        check_name="daily_quotes_aum_outliers",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n_extreme} rows with aum > 1T BRL — will distort median_aum and AuM filter"
        ),
    )


def _daily_quotes_shareholders_outliers(db: DuckDBWarehouse) -> ValidateResult:
    """Shareholder count cannot be negative."""
    n = db.execute(
        "SELECT COUNT(*) FROM staging.daily_quotes WHERE shareholders IS NOT NULL AND shareholders < 0"
    ).fetchone()[0]
    passed = n == 0
    return ValidateResult(
        check_name="daily_quotes_shareholders_outliers",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} rows with negative shareholders — will corrupt median_holders and cotistas filter"
        ),
    )


def _anbima_redemption_days_null_rate(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """High redemption_days null rate means many funds will be penalised with fallback in ranking."""
    snap = snapshot_date(db, "staging.anbima", reference_date)
    if snap is None:
        return ValidateResult(
            check_name="anbima_redemption_days_null_rate",
            passed=False,
            severity="warning",
            value=None,
            threshold="< 50%",
            message="no ANBIMA snapshot — cannot check redemption_days null rate",
        )
    total, n_null = db.execute(
        "SELECT COUNT(*), SUM(CASE WHEN redemption_days IS NULL THEN 1 ELSE 0 END) "
        "FROM staging.anbima WHERE reference_date = ?",
        [snap],
    ).fetchone()
    null_rate = (n_null / total) if total > 0 else 0.0
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="anbima_redemption_days_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of redemption_days is null — those funds get a 100-day penalty in ranking"
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
