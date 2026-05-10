"""Validation checks for marts.* tables (post-compute gate)."""

from __future__ import annotations

from datetime import date

from ._base import (
    ValidateResult,
    check_and_gate,
    check_column_no_nulls,
    check_pk_unique,
    check_row_count,
    check_snapshot_exists,
    snapshot_date,
)
from ..config import Settings
from ..storage import DuckDBWarehouse

_TASK = "validate_marts"
_DATASET = "marts"


def validate_marts(
    db: DuckDBWarehouse, reference_date: date, settings: Settings | None = None
) -> list[ValidateResult]:
    """Run all marts-layer checks, write to log, and raise on error-level failures."""
    return check_and_gate(
        db,
        _build_checks(db, reference_date, settings),
        _TASK,
        _DATASET,
        reference_date,
    )


def _build_checks(
    db: DuckDBWarehouse, reference_date: date, settings: Settings | None
) -> list[ValidateResult]:
    return [
        *_checks_universe(db, reference_date),
        *_checks_metrics(db, reference_date),
        *_checks_rankings(db, reference_date, settings),
    ]


def _checks_universe(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db, "marts.universe", reference_date, name="universe_snapshot_exists"
        ),
        check_row_count(
            db,
            "marts.universe",
            name="universe_row_count",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "marts.universe",
            "fund_cnpj",
            name="universe_fund_cnpj_not_null",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "marts.universe",
            ["fund_cnpj", "subclass_id"],
            name="universe_pk_unique",
            reference_date=reference_date,
        ),
        _universe_anbima_null_rate(db, reference_date, "target_taxation"),
        _universe_anbima_null_rate(db, reference_date, "redemption_days"),
        _universe_anbima_null_rate(db, reference_date, "min_investment"),
    ]


def _universe_anbima_null_rate(
    db: DuckDBWarehouse, reference_date: date, column: str
) -> ValidateResult:
    """Warn when more than 5% of universe funds are missing an ANBIMA enrichment column."""
    snap = snapshot_date(db, "marts.universe", reference_date)
    null_rate = (
        db.execute(
            f"SELECT AVG(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END)"
            " FROM marts.universe WHERE reference_date = ?",
            [snap],
        ).fetchone()[0]
        or 0.0
        if snap is not None
        else 0.0
    )
    passed = null_rate <= 0.05
    return ValidateResult(
        check_name=f"universe_{column}_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="<= 5%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of {column} is null — funds without ANBIMA enrichment use fallback values"
        ),
    )


def _checks_metrics(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db, "marts.metrics", reference_date, name="metrics_snapshot_exists"
        ),
        check_row_count(
            db, "marts.metrics", name="metrics_row_count", reference_date=reference_date
        ),
        _metrics_fund_count_reasonable(db, reference_date),
        check_column_no_nulls(
            db,
            "marts.metrics",
            "fund_cnpj",
            name="metrics_fund_cnpj_not_null",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "marts.metrics",
            ["fund_cnpj", "subclass_id"],
            name="metrics_pk_unique",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "marts.metrics",
            "ir_rate",
            name="metrics_ir_rate_not_null",
            reference_date=reference_date,
        ),
        _metrics_ir_rate_in_range(db, reference_date),
        _metrics_investor_level_valid(db, reference_date),
        _metrics_no_negative_volatility(db, reference_date),
        _metrics_max_drawdown_sign(db, reference_date),
        _metrics_pct_months_in_range(db, reference_date),
        _metrics_return_12m_bounds(db, reference_date),
        _metrics_alpha_12m_net_extreme(db, reference_date),
        _metrics_volatility_bounds(db, reference_date),
        _metrics_sharpe_not_extreme(db, reference_date),
    ]


def _checks_rankings(
    db: DuckDBWarehouse, reference_date: date, settings: Settings | None
) -> list[ValidateResult]:
    checks = [
        check_snapshot_exists(
            db, "marts.rankings", reference_date, name="rankings_snapshot_exists"
        ),
        check_row_count(
            db,
            "marts.rankings",
            name="rankings_row_count",
            reference_date=reference_date,
        ),
        check_pk_unique(
            db,
            "marts.rankings",
            ["fund_cnpj", "subclass_id", "purpose", "profile", "investor_type"],
            name="rankings_pk_unique",
            reference_date=reference_date,
        ),
        _rankings_score_in_range(db, reference_date),
    ]
    if settings is not None:
        checks.append(_rankings_combo_coverage(db, reference_date, settings))
    return checks


def _rankings_score_in_range(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Score outside [0, 1] means weight vector misconfigured or accessibility blend overflowed."""
    snap = snapshot_date(db, "marts.rankings", reference_date)
    n = (
        db.execute(
            "SELECT COUNT(*) FROM marts.rankings "
            "WHERE reference_date = ? AND (score < 0 OR score > 1)",
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n == 0
    return ValidateResult(
        check_name="rankings_score_in_range",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} rankings rows have score outside [0, 1] — weight config may be invalid"
        ),
    )


def _rankings_combo_coverage(
    db: DuckDBWarehouse, reference_date: date, settings: Settings
) -> ValidateResult:
    """Every (purpose, profile, investor_type) in settings.rankings should have at least one fund."""
    snap = snapshot_date(db, "marts.rankings", reference_date)
    present = (
        set(
            db.execute(
                "SELECT DISTINCT purpose, profile, investor_type "
                "FROM marts.rankings WHERE reference_date = ?",
                [snap],
            ).fetchall()
        )
        if snap is not None
        else set()
    )
    expected = {(c.purpose, c.profile, c.investor_type) for c in settings.rankings}
    missing = expected - present
    passed = not missing
    return ValidateResult(
        check_name="rankings_combo_coverage",
        passed=passed,
        severity="warning",
        value=f"{len(present)}/{len(expected)}",
        threshold=f"all {len(expected)} combos present",
        message=(
            None
            if passed
            else f"{len(missing)} combo(s) missing from rankings: {sorted(missing)}"
        ),
    )


def _outlier_count(db: DuckDBWarehouse, reference_date: date, condition: str) -> int:
    """Count rows in the metrics snapshot that satisfy condition."""
    snap = snapshot_date(db, "marts.metrics", reference_date)
    if snap is None:
        return 0
    return db.execute(
        f"SELECT COUNT(*) FROM marts.metrics WHERE reference_date = ? AND ({condition})",
        [snap],
    ).fetchone()[0]


def _metrics_fund_count_reasonable(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Fewer than 50 distinct funds means most were filtered out — likely an upstream data problem."""
    snap = snapshot_date(db, "marts.metrics", reference_date)
    n = (
        db.execute(
            "SELECT COUNT(DISTINCT fund_cnpj) FROM marts.metrics WHERE reference_date = ?",
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n >= 50
    return ValidateResult(
        check_name="metrics_fund_count_reasonable",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold=">= 50 distinct funds",
        message=(
            None
            if passed
            else f"only {n} distinct funds in metrics — most may have been filtered by upstream checks"
        ),
    )


def _metrics_ir_rate_in_range(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """IR rate outside [0, 1] means the tax config has invalid values or an exempt-keyword override misfired."""
    n = _outlier_count(db, reference_date, "ir_rate < 0 OR ir_rate > 1")
    passed = n == 0
    return ValidateResult(
        check_name="metrics_ir_rate_in_range",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} funds have ir_rate outside [0, 1] — tax config may have invalid values"
        ),
    )


def _metrics_investor_level_valid(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """investor_level must be 0 (geral), 1 (qualificado), or 2 (profissional)."""
    n = _outlier_count(
        db, reference_date, "investor_level IS NULL OR investor_level NOT IN (0, 1, 2)"
    )
    passed = n == 0
    return ValidateResult(
        check_name="metrics_investor_level_valid",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} funds have invalid investor_level — will be excluded from all ranking buckets"
        ),
    )


def _metrics_no_negative_volatility(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Negative volatility is mathematically impossible — signals a computation error."""
    n = _outlier_count(db, reference_date, "volatility < 0")
    passed = n == 0
    return ValidateResult(
        check_name="metrics_no_negative_volatility",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} funds have negative volatility — annualised std dev cannot be negative"
        ),
    )


def _metrics_max_drawdown_sign(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """max_drawdown is peak-to-trough, so it must be <= 0; a positive value means the sign was inverted."""
    n = _outlier_count(db, reference_date, "max_drawdown > 0")
    passed = n == 0
    return ValidateResult(
        check_name="metrics_max_drawdown_sign",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} funds have positive max_drawdown — peak-to-trough drawdown must be <= 0"
        ),
    )


def _metrics_pct_months_in_range(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """pct_months_above_cdi is a fraction and must be in [0, 1]."""
    n = _outlier_count(
        db,
        reference_date,
        "pct_months_above_cdi < 0 OR pct_months_above_cdi > 1",
    )
    passed = n == 0
    return ValidateResult(
        check_name="metrics_pct_months_in_range",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} funds have pct_months_above_cdi outside [0, 1] — fraction cannot exceed bounds"
        ),
    )


def _metrics_return_12m_bounds(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """12m gross return outside (-90%, +100%) is implausible for a Renda Fixa fund."""
    n = _outlier_count(
        db,
        reference_date,
        "return_12m IS NOT NULL AND (return_12m < -0.9 OR return_12m > 1.0)",
    )
    passed = n == 0
    return ValidateResult(
        check_name="metrics_return_12m_bounds",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0 outside (-90%, +100%)",
        message=(
            None
            if passed
            else f"{n} funds have 12m gross return outside (-90%, +100%) — likely a NAV data error"
        ),
    )


def _metrics_alpha_12m_net_extreme(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Net alpha > 30pp vs CDI in 12 months is extraordinary for fixed income — likely a data error."""
    n = _outlier_count(
        db,
        reference_date,
        "alpha_12m_net IS NOT NULL AND ABS(alpha_12m_net) > 0.3",
    )
    passed = n == 0
    return ValidateResult(
        check_name="metrics_alpha_12m_net_extreme",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0 with |alpha| > 30pp",
        message=(
            None
            if passed
            else f"{n} funds have |alpha_12m_net| > 30pp — review NAV or CDI data for these funds"
        ),
    )


def _metrics_volatility_bounds(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Annualised volatility > 500% is atypical for fixed income and suggests a computation or data error."""
    n = _outlier_count(db, reference_date, "volatility IS NOT NULL AND volatility > 5")
    passed = n == 0
    return ValidateResult(
        check_name="metrics_volatility_bounds",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0 with vol > 500%",
        message=(
            None
            if passed
            else f"{n} funds have annualised volatility > 500% — atypical for Renda Fixa"
        ),
    )


def _metrics_sharpe_not_extreme(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Sharpe ratio > 100 in absolute value signals near-zero volatility in the denominator."""
    n = _outlier_count(
        db,
        reference_date,
        "sharpe_excess IS NOT NULL AND ABS(sharpe_excess) > 100",
    )
    passed = n == 0
    return ValidateResult(
        check_name="metrics_sharpe_not_extreme",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0 with |Sharpe| > 100",
        message=(
            None
            if passed
            else f"{n} funds have |sharpe_excess| > 100 — near-zero volatility inflating the ratio"
        ),
    )
