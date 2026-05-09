"""Build eligible fund universe from staging tables."""

from __future__ import annotations

import logging
from datetime import date

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


def _load_snapshot(db: DuckDBWarehouse, table: str, calc_date: date) -> pd.DataFrame:
    """Load the most recent snapshot from a staging table at or before calc_date."""
    return db.execute(
        f"SELECT * FROM {table} "
        f"WHERE reference_date = ("
        f"  SELECT MAX(reference_date) FROM {table} WHERE reference_date <= ?"
        f")",
        [calc_date],
    ).df()


def build_universe(
    db: DuckDBWarehouse, reference_date: date, settings: Settings
) -> pd.DataFrame:
    """Build eligible fund universe keyed by (fund_cnpj, subclass_id)."""
    registry = _load_snapshot(db, "staging.registry", reference_date)
    fees = _load_snapshot(db, "staging.fees", reference_date)
    anbima = _load_snapshot(db, "staging.anbima", reference_date)

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

    fee_cols = fees[["fund_cnpj", "adm_fee", "has_perf_fee"]].drop_duplicates(
        "fund_cnpj", keep="last"
    )
    eligible = eligible.merge(fee_cols, on="fund_cnpj", how="left")

    window_start = (
        pd.Timestamp(reference_date)
        - pd.Timedelta(days=settings.universe.aum_lookback_days)
    ).date()
    agg = db.execute(
        """
        SELECT fund_cnpj, subclass_id,
               MEDIAN(aum)          AS median_aum,
               MEDIAN(shareholders) AS median_holders
        FROM staging.daily_quotes
        WHERE date >= ? AND date <= ?
        GROUP BY fund_cnpj, subclass_id
        """,
        [window_start, reference_date],
    ).df()
    logger.debug("universe: %d fund-subclass combos from daily_quotes", len(agg))

    eligible = eligible.merge(agg, on=["fund_cnpj", "subclass_id"], how="inner")
    logger.debug("universe: %d combos after aum merge", len(eligible))

    eligible = eligible[
        (eligible["median_holders"].fillna(0) > settings.universe.min_cotistas)
        & (eligible["median_aum"].fillna(0) > settings.universe.min_aum)
    ].reset_index(drop=True)
    logger.debug("universe: %d combos pass aum/holders threshold", len(eligible))

    anbima_keep = anbima[
        [
            "fund_cnpj",
            "subclass_id",
            "target_taxation",
            "redemption_days",
            "min_initial_investment",
        ]
    ].rename(columns={"min_initial_investment": "min_investment"})
    eligible = eligible.merge(anbima_keep, on=["fund_cnpj", "subclass_id"], how="left")

    null_cnpj = eligible["fund_cnpj"].isna().sum()
    if null_cnpj > 0:
        logger.error("universe: %d rows with NULL fund_cnpj!", null_cnpj)
        eligible = eligible[eligible["fund_cnpj"].notna()].reset_index(drop=True)

    eligible["reference_date"] = reference_date
    logger.info("universe: %d eligible funds for %s", len(eligible), reference_date)
    return eligible.reindex(columns=_UNIVERSE_COLS)
