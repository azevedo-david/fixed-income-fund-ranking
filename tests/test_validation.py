"""Tests for the validation layer: _base utilities, staging checks, and marts checks."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pytest

from src.validation._base import (
    check_and_gate,
    check_column_no_nulls,
    check_date_freshness,
    check_known_values,
    check_pk_unique,
    check_row_count,
    check_snapshot_exists,
    snapshot_date,
)
from src.validation.validate_marts import (
    _metrics_fund_count_reasonable,
    _metrics_max_drawdown_sign,
    _metrics_pct_months_in_range,
    _metrics_return_12m_bounds,
    _universe_anbima_null_rate,
)
from src.validation.validate_staging import (
    _anbima_redemption_days_null_rate,
    _cdi_rates_no_large_gaps,
    _daily_quotes_aum_null_rate,
    _daily_quotes_aum_outliers,
    _daily_quotes_nav_null_rate,
    _daily_quotes_nav_outliers,
    _daily_quotes_shareholders_outliers,
)

REF = date(2025, 5, 10)


class _DB:
    """In-memory DuckDB with logs schema, mimicking DuckDBWarehouse.execute()."""

    def __init__(self):
        self._con = duckdb.connect(":memory:")
        self._con.execute("CREATE SCHEMA logs")
        self._con.execute("""
            CREATE TABLE logs.validation_log (
                reference_date DATE, task VARCHAR, dataset VARCHAR,
                check_name VARCHAR, severity VARCHAR, passed BOOLEAN,
                value VARCHAR, threshold VARCHAR, message VARCHAR,
                logged_at TIMESTAMPTZ DEFAULT current_timestamp
            )
            """)

    def execute(self, q, p=None):
        return self._con.execute(q, p or [])

    def ddl(self, sql: str) -> "_DB":
        self._con.execute(sql)
        return self


@pytest.fixture
def db():
    return _DB()


# ---------------------------------------------------------------------------
# Helpers — create minimal tables used across multiple test groups
# ---------------------------------------------------------------------------


def _make_snapshot_table(db: _DB, table: str, schema: str) -> None:
    """Create a simple snapshot table in the given schema."""
    db.ddl(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    db.ddl(f"CREATE TABLE {table} ({schema_cols(schema)}, reference_date DATE)")


def schema_cols(schema: str) -> str:
    return "fund_cnpj VARCHAR"


# ---------------------------------------------------------------------------
# _base: snapshot_date
# ---------------------------------------------------------------------------


def test_snapshot_date_returns_latest(db):
    db.ddl("CREATE SCHEMA s1")
    db.ddl("CREATE TABLE s1.t (reference_date DATE)")
    db.execute("INSERT INTO s1.t VALUES ('2025-04-01'), ('2025-05-01')")
    assert snapshot_date(db, "s1.t", REF) == date(2025, 5, 1)


def test_snapshot_date_respects_reference_date(db):
    db.ddl("CREATE SCHEMA s2")
    db.ddl("CREATE TABLE s2.t (reference_date DATE)")
    db.execute("INSERT INTO s2.t VALUES ('2025-03-01'), ('2025-06-01')")
    # Only 2025-03-01 is on or before REF (2025-05-10)
    assert snapshot_date(db, "s2.t", REF) == date(2025, 3, 1)


def test_snapshot_date_returns_none_for_empty(db):
    db.ddl("CREATE SCHEMA s3")
    db.ddl("CREATE TABLE s3.t (reference_date DATE)")
    assert snapshot_date(db, "s3.t", REF) is None


# ---------------------------------------------------------------------------
# _base: check_snapshot_exists
# ---------------------------------------------------------------------------


def test_check_snapshot_exists_pass(db):
    db.ddl("CREATE SCHEMA se1")
    db.ddl("CREATE TABLE se1.t (reference_date DATE)")
    db.execute("INSERT INTO se1.t VALUES ('2025-05-01')")
    r = check_snapshot_exists(db, "se1.t", REF, name="x")
    assert r.passed


def test_check_snapshot_exists_fail_empty(db):
    db.ddl("CREATE SCHEMA se2")
    db.ddl("CREATE TABLE se2.t (reference_date DATE)")
    r = check_snapshot_exists(db, "se2.t", REF, name="x")
    assert not r.passed
    assert r.severity == "error"


# ---------------------------------------------------------------------------
# _base: check_row_count
# ---------------------------------------------------------------------------


def test_check_row_count_pass(db):
    db.ddl("CREATE SCHEMA rc1")
    db.ddl("CREATE TABLE rc1.t (v INTEGER)")
    db.execute("INSERT INTO rc1.t VALUES (1), (2)")
    r = check_row_count(db, "rc1.t", name="x")
    assert r.passed


def test_check_row_count_fail(db):
    db.ddl("CREATE SCHEMA rc2")
    db.ddl("CREATE TABLE rc2.t (v INTEGER)")
    r = check_row_count(db, "rc2.t", name="x")
    assert not r.passed
    assert r.severity == "error"


def test_check_row_count_with_snapshot_filter(db):
    db.ddl("CREATE SCHEMA rc3")
    db.ddl("CREATE TABLE rc3.t (v INTEGER, reference_date DATE)")
    db.execute("INSERT INTO rc3.t VALUES (1, '2025-04-01')")
    # Snapshot at 2025-04-01 has 1 row — passes
    r = check_row_count(db, "rc3.t", name="x", reference_date=REF)
    assert r.passed
    # But an earlier ref_date has no snapshot at all — fails
    r2 = check_row_count(db, "rc3.t", name="x", reference_date=date(2025, 1, 1))
    assert not r2.passed


# ---------------------------------------------------------------------------
# _base: check_column_no_nulls
# ---------------------------------------------------------------------------


def test_check_column_no_nulls_pass(db):
    db.ddl("CREATE SCHEMA cn1")
    db.ddl("CREATE TABLE cn1.t (fund_cnpj VARCHAR)")
    db.execute("INSERT INTO cn1.t VALUES ('A'), ('B')")
    r = check_column_no_nulls(db, "cn1.t", "fund_cnpj", name="x")
    assert r.passed


def test_check_column_no_nulls_fail_error(db):
    db.ddl("CREATE SCHEMA cn2")
    db.ddl("CREATE TABLE cn2.t (fund_cnpj VARCHAR)")
    db.execute("INSERT INTO cn2.t VALUES ('A'), (NULL)")
    r = check_column_no_nulls(db, "cn2.t", "fund_cnpj", name="x")
    assert not r.passed
    assert r.severity == "error"
    assert r.value == "1"


def test_check_column_no_nulls_warning_severity(db):
    db.ddl("CREATE SCHEMA cn3")
    db.ddl("CREATE TABLE cn3.t (fund_cnpj VARCHAR)")
    db.execute("INSERT INTO cn3.t VALUES (NULL)")
    r = check_column_no_nulls(db, "cn3.t", "fund_cnpj", name="x", severity="warning")
    assert not r.passed
    assert r.severity == "warning"


# ---------------------------------------------------------------------------
# _base: check_date_freshness
# ---------------------------------------------------------------------------


def test_check_date_freshness_pass(db):
    db.ddl("CREATE SCHEMA df1")
    db.ddl("CREATE TABLE df1.t (dt DATE)")
    db.execute(f"INSERT INTO df1.t VALUES ('{REF}')")
    r = check_date_freshness(db, "df1.t", "dt", REF, name="x", max_lag_days=7)
    assert r.passed


def test_check_date_freshness_fail_stale(db):
    db.ddl("CREATE SCHEMA df2")
    db.ddl("CREATE TABLE df2.t (dt DATE)")
    stale = REF - timedelta(days=30)
    db.execute(f"INSERT INTO df2.t VALUES ('{stale}')")
    r = check_date_freshness(db, "df2.t", "dt", REF, name="x", max_lag_days=7)
    assert not r.passed
    assert r.severity == "warning"


def test_check_date_freshness_fail_empty(db):
    db.ddl("CREATE SCHEMA df3")
    db.ddl("CREATE TABLE df3.t (dt DATE)")
    r = check_date_freshness(db, "df3.t", "dt", REF, name="x")
    assert not r.passed


# ---------------------------------------------------------------------------
# _base: check_pk_unique
# ---------------------------------------------------------------------------


def test_check_pk_unique_pass(db):
    db.ddl("CREATE SCHEMA pk1")
    db.ddl("CREATE TABLE pk1.t (a VARCHAR, b VARCHAR)")
    db.execute("INSERT INTO pk1.t VALUES ('X','1'), ('X','2'), ('Y','1')")
    r = check_pk_unique(db, "pk1.t", ["a", "b"], name="x")
    assert r.passed


def test_check_pk_unique_fail(db):
    db.ddl("CREATE SCHEMA pk2")
    db.ddl("CREATE TABLE pk2.t (a VARCHAR, b VARCHAR)")
    db.execute("INSERT INTO pk2.t VALUES ('X','1'), ('X','1'), ('Y','2')")
    r = check_pk_unique(db, "pk2.t", ["a", "b"], name="x")
    assert not r.passed
    assert r.severity == "error"
    assert r.value == "1"  # one duplicate group


def test_check_pk_unique_with_snapshot(db):
    db.ddl("CREATE SCHEMA pk3")
    db.ddl("CREATE TABLE pk3.t (a VARCHAR, reference_date DATE)")
    # Duplicate 'A' in the snapshot
    db.execute("INSERT INTO pk3.t VALUES ('A','2025-05-01'),('A','2025-05-01')")
    r = check_pk_unique(db, "pk3.t", ["a"], name="x", reference_date=REF)
    assert not r.passed


# ---------------------------------------------------------------------------
# _base: check_known_values
# ---------------------------------------------------------------------------


def test_check_known_values_pass(db):
    db.ddl("CREATE SCHEMA kv1")
    db.ddl("CREATE TABLE kv1.t (status VARCHAR)")
    db.execute("INSERT INTO kv1.t VALUES ('S'), ('N')")
    r = check_known_values(db, "kv1.t", "status", {"S", "N"}, name="x")
    assert r.passed


def test_check_known_values_fail_unknown(db):
    db.ddl("CREATE SCHEMA kv2")
    db.ddl("CREATE TABLE kv2.t (status VARCHAR)")
    db.execute("INSERT INTO kv2.t VALUES ('S'), ('N'), ('X')")
    r = check_known_values(db, "kv2.t", "status", {"S", "N"}, name="x")
    assert not r.passed
    assert r.severity == "warning"
    assert "X" in r.value


def test_check_known_values_ignores_nulls(db):
    db.ddl("CREATE SCHEMA kv3")
    db.ddl("CREATE TABLE kv3.t (status VARCHAR)")
    db.execute("INSERT INTO kv3.t VALUES ('S'), (NULL)")
    r = check_known_values(db, "kv3.t", "status", {"S", "N"}, name="x")
    assert r.passed  # NULL is excluded from the distinct check


# ---------------------------------------------------------------------------
# _base: check_and_gate
# ---------------------------------------------------------------------------


def test_check_and_gate_raises_on_error(db):
    from src.validation._base import ValidateResult

    checks = [
        ValidateResult("ok_check", True, "error"),
        ValidateResult("bad_check", False, "error"),
    ]
    with pytest.raises(ValueError, match="bad_check"):
        check_and_gate(db, checks, "task", "dataset", REF)


def test_check_and_gate_no_raise_on_warning(db):
    from src.validation._base import ValidateResult

    checks = [ValidateResult("warn_check", False, "warning")]
    result = check_and_gate(db, checks, "task", "dataset", REF)
    assert len(result) == 1  # returned without raising


def test_check_and_gate_writes_to_log(db):
    from src.validation._base import ValidateResult

    checks = [ValidateResult("my_check", True, "info", value="42")]
    check_and_gate(db, checks, "task", "dataset", REF)
    n = db.execute(
        "SELECT COUNT(*) FROM logs.validation_log WHERE check_name = 'my_check'"
    ).fetchone()[0]
    assert n == 1


# ---------------------------------------------------------------------------
# staging: daily_quotes nav null rate
# ---------------------------------------------------------------------------


@pytest.fixture
def db_daily_quotes(db):
    db.ddl("CREATE SCHEMA staging")
    db.ddl(
        "CREATE TABLE staging.daily_quotes "
        "(fund_cnpj VARCHAR, subclass_id VARCHAR, date DATE, "
        "nav DOUBLE, aum DOUBLE, shareholders DOUBLE)"
    )
    return db


def test_daily_quotes_nav_null_rate_pass(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES "
        "('A', NULL, '2025-01-01', 1.0, 1000.0, 5)"
    )
    r = _daily_quotes_nav_null_rate(db_daily_quotes)
    assert r.passed


def test_daily_quotes_nav_null_rate_fail(db_daily_quotes):
    # Insert 10 rows: 6 with null nav (60% null rate)
    for i in range(10):
        nav = "NULL" if i < 6 else "1.0"
        db_daily_quotes.execute(
            f"INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-{i+1:02d}', {nav}, 1000.0, 5)"
        )
    r = _daily_quotes_nav_null_rate(db_daily_quotes)
    assert not r.passed
    assert r.severity == "error"


# ---------------------------------------------------------------------------
# staging: daily_quotes nav outliers
# ---------------------------------------------------------------------------


def test_daily_quotes_nav_outliers_pass(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 100.0, 1e9, 10)"
    )
    r = _daily_quotes_nav_outliers(db_daily_quotes)
    assert r.passed


def test_daily_quotes_nav_outliers_extreme(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 2e9, 1e9, 10)"
    )
    r = _daily_quotes_nav_outliers(db_daily_quotes)
    assert not r.passed
    assert r.severity == "warning"


# ---------------------------------------------------------------------------
# staging: daily_quotes aum outliers
# ---------------------------------------------------------------------------


def test_daily_quotes_aum_outliers_pass(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 1.0, 5e9, 10)"
    )
    r = _daily_quotes_aum_outliers(db_daily_quotes)
    assert r.passed


def test_daily_quotes_aum_outliers_extreme(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 1.0, 2e12, 10)"
    )
    r = _daily_quotes_aum_outliers(db_daily_quotes)
    assert not r.passed
    assert "1 rows with aum > 1T" in r.message


def test_daily_quotes_shareholders_outliers_pass(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 1.0, 1e9, 100)"
    )
    r = _daily_quotes_shareholders_outliers(db_daily_quotes)
    assert r.passed


def test_daily_quotes_shareholders_outliers_negative(db_daily_quotes):
    db_daily_quotes.execute(
        "INSERT INTO staging.daily_quotes VALUES ('A', NULL, '2025-01-01', 1.0, 1e9, -1)"
    )
    r = _daily_quotes_shareholders_outliers(db_daily_quotes)
    assert not r.passed


# ---------------------------------------------------------------------------
# staging: anbima redemption_days null rate
# ---------------------------------------------------------------------------


@pytest.fixture
def db_anbima_staging(db):
    db.ddl("CREATE SCHEMA IF NOT EXISTS staging")
    db.ddl(
        "CREATE TABLE staging.anbima "
        "(fund_cnpj VARCHAR, subclass_id VARCHAR, redemption_days INTEGER, reference_date DATE)"
    )
    return db


def test_anbima_redemption_days_null_rate_pass(db_anbima_staging):
    for i in range(10):
        db_anbima_staging.execute(
            f"INSERT INTO staging.anbima VALUES ('{i:014d}', NULL, 30, '{REF}')"
        )
    r = _anbima_redemption_days_null_rate(db_anbima_staging, REF)
    assert r.passed


def test_anbima_redemption_days_null_rate_fail(db_anbima_staging):
    # 7 out of 10 rows null — 70% null rate
    for i in range(10):
        days = "NULL" if i < 7 else "30"
        db_anbima_staging.execute(
            f"INSERT INTO staging.anbima VALUES ('{i:014d}', NULL, {days}, '{REF}')"
        )
    r = _anbima_redemption_days_null_rate(db_anbima_staging, REF)
    assert not r.passed
    assert r.severity == "warning"


def test_anbima_redemption_days_null_rate_no_snapshot(db_anbima_staging):
    r = _anbima_redemption_days_null_rate(db_anbima_staging, REF)
    assert not r.passed
    assert "no ANBIMA snapshot" in r.message


# ---------------------------------------------------------------------------
# staging: cdi_rates no large gaps
# ---------------------------------------------------------------------------


@pytest.fixture
def db_cdi(db):
    db.ddl("CREATE SCHEMA IF NOT EXISTS staging")
    db.ddl("CREATE TABLE staging.cdi_rates (date DATE, rate DOUBLE)")
    return db


def test_cdi_rates_no_large_gaps_pass(db_cdi):
    # Business days — max gap is 3 days over a weekend
    from datetime import timedelta

    base = date(2025, 1, 2)  # Thursday
    for i in range(10):
        d = base + timedelta(days=i)
        if d.weekday() < 5:
            db_cdi.execute(f"INSERT INTO staging.cdi_rates VALUES ('{d}', 0.0001)")
    r = _cdi_rates_no_large_gaps(db_cdi)
    assert r.passed


def test_cdi_rates_no_large_gaps_fail(db_cdi):
    db_cdi.execute("INSERT INTO staging.cdi_rates VALUES ('2025-01-02', 0.0001)")
    db_cdi.execute(
        "INSERT INTO staging.cdi_rates VALUES ('2025-01-20', 0.0001)"
    )  # 18-day gap
    r = _cdi_rates_no_large_gaps(db_cdi)
    assert not r.passed
    assert r.severity == "warning"


def test_cdi_rates_no_large_gaps_single_row(db_cdi):
    # With only one row there is no LAG pair — the function treats NULL max_gap
    # as unable to compute and returns passed=False with a descriptive message.
    db_cdi.execute("INSERT INTO staging.cdi_rates VALUES ('2025-01-02', 0.0001)")
    r = _cdi_rates_no_large_gaps(db_cdi)
    assert not r.passed
    assert r.message is not None


# ---------------------------------------------------------------------------
# marts: universe anbima null rate
# ---------------------------------------------------------------------------


@pytest.fixture
def db_universe(db):
    db.ddl("CREATE SCHEMA IF NOT EXISTS marts")
    db.ddl(
        "CREATE TABLE marts.universe "
        "(fund_cnpj VARCHAR, target_taxation VARCHAR, redemption_days INTEGER, "
        "min_investment DOUBLE, reference_date DATE)"
    )
    return db


def test_universe_anbima_null_rate_pass(db_universe):
    # 3% null rate — below 5% threshold
    for i in range(100):
        tax = "NULL" if i < 3 else "'Longo Prazo'"
        db_universe.execute(
            f"INSERT INTO marts.universe VALUES ('{i:014d}', {tax}, 30, 1000.0, '{REF}')"
        )
    r = _universe_anbima_null_rate(db_universe, REF, "target_taxation")
    assert r.passed


def test_universe_anbima_null_rate_fail(db_universe):
    # 20% null rate — above 5% threshold
    for i in range(100):
        tax = "NULL" if i < 20 else "'Longo Prazo'"
        db_universe.execute(
            f"INSERT INTO marts.universe VALUES ('{i:014d}', {tax}, 30, 1000.0, '{REF}')"
        )
    r = _universe_anbima_null_rate(db_universe, REF, "target_taxation")
    assert not r.passed
    assert r.severity == "warning"
    assert r.check_name == "universe_target_taxation_null_rate"


def test_universe_anbima_null_rate_no_snapshot(db_universe):
    # Table exists but has no data — should pass (0% null rate)
    r = _universe_anbima_null_rate(db_universe, REF, "target_taxation")
    assert r.passed


# ---------------------------------------------------------------------------
# marts: metrics business logic
# ---------------------------------------------------------------------------


@pytest.fixture
def db_metrics(db):
    db.ddl("CREATE SCHEMA IF NOT EXISTS marts")
    db.ddl(
        "CREATE TABLE marts.metrics ("
        "fund_cnpj VARCHAR, subclass_id VARCHAR, ir_rate DOUBLE, "
        "investor_level INTEGER, volatility DOUBLE, max_drawdown DOUBLE, "
        "pct_months_above_cdi DOUBLE, return_12m DOUBLE, alpha_12m_net DOUBLE, "
        "sharpe_excess DOUBLE, reference_date DATE)"
    )
    return db


def _insert_metric(
    db,
    cnpj,
    *,
    ref=REF,
    ir=0.15,
    level=0,
    vol=0.03,
    mdd=-0.01,
    pct=0.6,
    r12=0.12,
    alpha=0.01,
    sharpe=2.0,
):
    db.execute(
        f"INSERT INTO marts.metrics VALUES "
        f"('{cnpj}', NULL, {ir}, {level}, {vol}, {mdd}, {pct}, {r12}, {alpha}, {sharpe}, '{ref}')"
    )


def test_metrics_fund_count_reasonable_pass(db_metrics):
    for i in range(60):
        _insert_metric(db_metrics, f"{i:014d}")
    r = _metrics_fund_count_reasonable(db_metrics, REF)
    assert r.passed


def test_metrics_fund_count_reasonable_fail(db_metrics):
    for i in range(10):
        _insert_metric(db_metrics, f"{i:014d}")
    r = _metrics_fund_count_reasonable(db_metrics, REF)
    assert not r.passed
    assert r.severity == "warning"


def test_metrics_max_drawdown_sign_pass(db_metrics):
    _insert_metric(db_metrics, "A", mdd=-0.05)
    _insert_metric(db_metrics, "B", mdd=0.0)
    r = _metrics_max_drawdown_sign(db_metrics, REF)
    assert r.passed


def test_metrics_max_drawdown_sign_fail(db_metrics):
    _insert_metric(db_metrics, "A", mdd=0.05)  # positive — sign inverted
    r = _metrics_max_drawdown_sign(db_metrics, REF)
    assert not r.passed
    assert r.severity == "error"
    assert r.value == "1"


def test_metrics_pct_months_in_range_pass(db_metrics):
    _insert_metric(db_metrics, "A", pct=0.0)
    _insert_metric(db_metrics, "B", pct=1.0)
    _insert_metric(db_metrics, "C", pct=0.55)
    r = _metrics_pct_months_in_range(db_metrics, REF)
    assert r.passed


def test_metrics_pct_months_in_range_fail_above_one(db_metrics):
    _insert_metric(db_metrics, "A", pct=1.1)
    r = _metrics_pct_months_in_range(db_metrics, REF)
    assert not r.passed
    assert r.severity == "error"


def test_metrics_pct_months_in_range_fail_negative(db_metrics):
    _insert_metric(db_metrics, "A", pct=-0.1)
    r = _metrics_pct_months_in_range(db_metrics, REF)
    assert not r.passed


def test_metrics_return_12m_bounds_pass(db_metrics):
    _insert_metric(db_metrics, "A", r12=0.12)
    _insert_metric(db_metrics, "B", r12=-0.10)
    r = _metrics_return_12m_bounds(db_metrics, REF)
    assert r.passed


def test_metrics_return_12m_bounds_fail_extreme_positive(db_metrics):
    _insert_metric(db_metrics, "A", r12=1.5)  # +150% — impossible for fixed income
    r = _metrics_return_12m_bounds(db_metrics, REF)
    assert not r.passed
    assert r.severity == "warning"


def test_metrics_return_12m_bounds_fail_extreme_negative(db_metrics):
    _insert_metric(db_metrics, "A", r12=-0.95)  # -95%, below the -90% lower bound
    r = _metrics_return_12m_bounds(db_metrics, REF)
    assert not r.passed


def test_metrics_return_12m_bounds_ignores_null(db_metrics):
    db_metrics.execute(
        f"INSERT INTO marts.metrics VALUES "
        f"('A', NULL, 0.15, 0, 0.03, -0.01, 0.6, NULL, 0.01, 2.0, '{REF}')"
    )
    r = _metrics_return_12m_bounds(db_metrics, REF)
    assert r.passed  # NULL return_12m should not trigger the check
