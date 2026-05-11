"""Build eligible fund universe from staging tables."""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
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

    fees = _load_snapshot(db, "staging.fees", reference_date)
    eligible = eligible.merge(
        (
            fees[["fund_cnpj", "adm_fee", "has_perf_fee"]]
            if not fees.empty
            else pd.DataFrame(columns=["fund_cnpj", "adm_fee", "has_perf_fee"])
        ),
        on="fund_cnpj",
        how="left",
    )

    window_start = (
        pd.Timestamp(reference_date)
        - pd.Timedelta(days=settings.universe.aum_lookback_days)
    ).date()

    quote_metrics = db.execute(
        """
        SELECT fund_cnpj, subclass_id,
               MEDIAN(aum) FILTER (WHERE date >= ? AND date <= ?) AS median_aum,
               MEDIAN(shareholders) FILTER (WHERE date >= ? AND date <= ?) AS median_holders,
               MIN(date) AS first_quote_date,
               MAX(date) AS last_quote_date,
               COUNT(*) AS quote_count
        FROM staging.daily_quotes
        WHERE date <= ?
        GROUP BY fund_cnpj, subclass_id
        """,
        [
            window_start,
            reference_date,
            window_start,
            reference_date,
            reference_date,
        ],
    ).df()

    eligible = eligible.merge(
        quote_metrics, on=["fund_cnpj", "subclass_id"], how="left"
    )
    eligible = eligible[eligible["median_aum"].notna()].reset_index(drop=True)
    logger.debug("universe: %d combos after AuM join", len(eligible))

    ref_ts = pd.Timestamp(reference_date)
    last_quote = pd.to_datetime(eligible["last_quote_date"])
    first_quote = pd.to_datetime(eligible["first_quote_date"])
    span_days_series = (last_quote - first_quote).dt.days
    span_bdays = pd.Series(
        np.busday_count(
            first_quote.values.astype("datetime64[D]"),
            (last_quote + pd.Timedelta(days=1)).values.astype("datetime64[D]"),
        ),
        index=eligible.index,
    )
    obs_ratio = eligible["quote_count"] / span_bdays.replace(0, np.nan)
    fresh_mask = (
        ref_ts - last_quote
    ).dt.days <= settings.universe.max_quote_staleness_days
    span_mask = span_days_series >= settings.universe.min_span_days
    dense_mask = obs_ratio >= settings.universe.min_obs_ratio
    dropped_stale = int((~fresh_mask).sum())
    dropped_short = int((fresh_mask & ~span_mask).sum())
    dropped_sparse = int((fresh_mask & span_mask & ~dense_mask).sum())
    eligible = eligible[fresh_mask & span_mask & dense_mask].reset_index(drop=True)
    logger.debug(
        "universe: dropped %d stale (>%dd), %d short (<%dd span), %d sparse (obs/span<%.2f)",
        dropped_stale,
        settings.universe.max_quote_staleness_days,
        dropped_short,
        settings.universe.min_span_days,
        dropped_sparse,
        settings.universe.min_obs_ratio,
    )
    eligible = eligible.drop(
        columns=["first_quote_date", "last_quote_date", "quote_count"]
    )

    anbima = _load_snapshot(db, "staging.anbima", reference_date)
    anbima_cols = [
        "fund_cnpj",
        "subclass_id",
        "target_taxation",
        "redemption_days",
        "min_initial_investment",
    ]
    eligible = eligible.merge(
        anbima[anbima_cols] if not anbima.empty else pd.DataFrame(columns=anbima_cols),
        on=["fund_cnpj", "subclass_id"],
        how="left",
    ).rename(columns={"min_initial_investment": "min_investment"})
    logger.debug("universe: %d combos after ANBIMA join", len(eligible))

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
        raise ValueError(f"universe: {null_cnpj} rows with NULL fund_cnpj after joins")

    eligible["reference_date"] = reference_date
    logger.info("universe: %d eligible funds for %s", len(eligible), reference_date)
    return eligible[_UNIVERSE_COLS]
