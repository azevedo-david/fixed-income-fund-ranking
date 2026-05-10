"""Build eligible fund universe from staging tables."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from typing import Generator

import pandas as pd

from ..config import Settings
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_UNIVERSE_COLS = [
    "fund_cnpj",
    "subclass_id",
    "fund_name",
    "inception_date",
    "anbima_category",
    "target_investor",
    "share_class",
    "fund_structure",
    "adm_fee",
    "has_perf_fee",
    "median_aum",
    "median_holders",
    "target_taxation",
    "redemption_days",
    "min_investment",
    "reference_date",
]


@contextmanager
def _temp_table(
    db: DuckDBWarehouse, name: str, df: pd.DataFrame
) -> Generator[None, None, None]:
    """Context manager for registering/unregistering temp tables with exception safety."""
    db._con.register(name, df)
    try:
        yield
    finally:
        db._con.unregister(name)


def _load_snapshot(db: DuckDBWarehouse, table: str, calc_date: date) -> pd.DataFrame:
    """Load the most recent snapshot at or before calc_date; falls back to earliest after.

    The fallback enables backfill runs where the registry was downloaded after the
    reference date — inception_date filtering in build_universe handles as-of correctness.
    """
    row = db.execute(
        f"SELECT COALESCE("
        f"  (SELECT MAX(reference_date) FROM {table} WHERE reference_date <= ?),"
        f"  (SELECT MIN(reference_date) FROM {table} WHERE reference_date >= ?)"
        f") AS snap_date",
        [calc_date, calc_date],
    ).fetchone()
    snap_date = row[0] if row else None

    if snap_date is None:
        logger.warning("%s: no snapshot available for %s", table, calc_date)
        return pd.DataFrame()

    if snap_date > calc_date:
        logger.warning(
            "%s: no snapshot at or before %s — using nearest future snapshot (%s)",
            table,
            calc_date,
            snap_date,
        )
    else:
        logger.debug(
            "%s: using snapshot %s for reference_date %s", table, snap_date, calc_date
        )

    return db.execute(
        f"SELECT * FROM {table} WHERE reference_date = ?", [snap_date]
    ).df()


def build_universe(
    db: DuckDBWarehouse, reference_date: date, settings: Settings
) -> pd.DataFrame:
    """Build eligible fund universe keyed by (fund_cnpj, subclass_id)."""
    registry = _load_snapshot(db, "staging.registry", reference_date)

    if registry.empty:
        logger.warning("universe: empty registry snapshot for %s", reference_date)
        return pd.DataFrame(columns=_UNIVERSE_COLS)

    eligible = registry[
        (registry["status"] == "Em Funcionamento Normal")
        & (registry["anbima_category"].str.startswith("Renda Fixa", na=False))
        & (registry["is_exclusive"].fillna("N") != "S")
        & (registry["fund_structure"].fillna("") != "Fechado")
        & (registry["is_pension"].fillna("N") != "S")
        & (
            pd.to_datetime(registry["inception_date"], errors="coerce")
            <= pd.Timestamp(reference_date)
        )
    ].copy()
    logger.debug("universe: %d funds pass filter criteria", len(eligible))

    fee_cols = db.execute(
        "SELECT fund_cnpj, adm_fee, has_perf_fee "
        "FROM staging.fees "
        "WHERE reference_date = (SELECT MAX(reference_date) FROM staging.fees) "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_cnpj ORDER BY reference_date DESC) = 1"
    ).df()
    eligible = eligible.merge(fee_cols, on="fund_cnpj", how="left")

    window_start = (
        pd.Timestamp(reference_date)
        - pd.Timedelta(days=settings.universe.aum_lookback_days)
    ).date()

    with _temp_table(db, "_eligible", eligible):
        result = db.execute(
            """
            WITH aum_metrics AS (
                SELECT fund_cnpj, subclass_id,
                       MEDIAN(aum) AS median_aum,
                       MEDIAN(shareholders) AS median_holders
                FROM staging.daily_quotes
                WHERE date >= ? AND date <= ?
                GROUP BY fund_cnpj, subclass_id
            ),
            anbima_enriched AS (
                SELECT fund_cnpj, subclass_id,
                       target_taxation, redemption_days,
                       min_initial_investment AS min_investment
                FROM staging.anbima
            )
            SELECT e.fund_cnpj, e.subclass_id, e.fund_name, e.inception_date,
                   e.anbima_category, e.target_investor, e.share_class,
                   e.fund_structure, e.adm_fee, e.has_perf_fee,
                   COALESCE(am_exact.median_aum, am_null.median_aum) AS median_aum,
                   COALESCE(am_exact.median_holders, am_null.median_holders) AS median_holders,
                   COALESCE(ae_exact.target_taxation, ae_null.target_taxation) AS target_taxation,
                   COALESCE(ae_exact.redemption_days, ae_null.redemption_days) AS redemption_days,
                   COALESCE(ae_exact.min_investment, ae_null.min_investment) AS min_investment
            FROM _eligible e
            LEFT JOIN aum_metrics am_exact
                ON e.fund_cnpj = am_exact.fund_cnpj AND e.subclass_id = am_exact.subclass_id
            LEFT JOIN aum_metrics am_null
                ON e.fund_cnpj = am_null.fund_cnpj AND am_null.subclass_id IS NULL
            LEFT JOIN anbima_enriched ae_exact
                ON e.fund_cnpj = ae_exact.fund_cnpj AND e.subclass_id = ae_exact.subclass_id
            LEFT JOIN anbima_enriched ae_null
                ON e.fund_cnpj = ae_null.fund_cnpj AND ae_null.subclass_id IS NULL
            WHERE COALESCE(am_exact.median_aum, am_null.median_aum) IS NOT NULL
            """,
            [window_start, reference_date],
        ).df()

    eligible = result
    logger.debug("universe: %d combos after joins", len(eligible))

    eligible = eligible[
        (eligible["median_holders"].fillna(0) > settings.universe.min_cotistas)
        & (eligible["median_aum"].fillna(0) > settings.universe.min_aum)
    ].reset_index(drop=True)

    if eligible.empty:
        logger.warning(
            "universe: no funds pass thresholds (min_aum=%.0f, min_cotistas=%.0f) for %s",
            settings.universe.min_aum,
            settings.universe.min_cotistas,
            reference_date,
        )
        return pd.DataFrame(columns=_UNIVERSE_COLS)

    logger.debug("universe: %d combos pass aum/holders threshold", len(eligible))

    null_cnpj = eligible["fund_cnpj"].isna().sum()
    if null_cnpj > 0:
        logger.error("universe: %d rows with NULL fund_cnpj!", null_cnpj)
        eligible = eligible[eligible["fund_cnpj"].notna()].reset_index(drop=True)

    eligible["reference_date"] = reference_date
    logger.info("universe: %d eligible funds for %s", len(eligible), reference_date)
    return eligible[_UNIVERSE_COLS]
