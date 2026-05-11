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

_FEES_COLS = ["fund_cnpj", "adm_fee", "has_perf_fee"]
_ANBIMA_COLS = [
    "fund_cnpj",
    "subclass_id",
    "target_taxation",
    "redemption_days",
    "min_initial_investment",
]
_QUOTE_HELPER_COLS = ["first_quote_date", "last_quote_date", "quote_count"]


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


def _safe_select(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Project df to cols; return an empty DataFrame with cols if df is shape-less."""
    if df.empty and not set(cols).issubset(df.columns):
        return pd.DataFrame(columns=cols)
    return df[cols]


def _filter_eligible_registry(
    registry: pd.DataFrame, reference_date: date
) -> pd.DataFrame:
    """Registry-level eligibility filters: status, category, structure, inception."""
    if registry.empty:
        return registry
    keep = (
        (registry["status"] == "Em Funcionamento Normal")
        & (registry["anbima_category"].str.startswith("Renda Fixa", na=False))
        & (registry["is_exclusive"].fillna("N") != "S")
        & (registry["fund_structure"].fillna("") != "Fechado")
        & (registry["is_pension"].fillna("N") != "S")
        & (
            pd.to_datetime(registry["inception_date"], errors="coerce")
            <= pd.Timestamp(reference_date)
        )
    )
    eligible = registry[keep].copy()
    logger.debug("universe: %d funds pass registry filters", len(eligible))
    return eligible


def _join_fees(
    db: DuckDBWarehouse, eligible: pd.DataFrame, reference_date: date
) -> pd.DataFrame:
    """Add adm_fee, has_perf_fee from staging.fees."""
    fees = _load_snapshot(db, "staging.fees", reference_date)
    return eligible.merge(_safe_select(fees, _FEES_COLS), on="fund_cnpj", how="left")


def _load_quote_metrics(
    db: DuckDBWarehouse, settings: Settings, reference_date: date
) -> pd.DataFrame:
    """Per-fund AuM/holders medians over the lookback window plus span metadata."""
    window_start = (
        pd.Timestamp(reference_date)
        - pd.Timedelta(days=settings.universe.aum_lookback_days)
    ).date()
    return db.execute(
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
        [window_start, reference_date, window_start, reference_date, reference_date],
    ).df()


def _join_quote_metrics(
    db: DuckDBWarehouse,
    eligible: pd.DataFrame,
    settings: Settings,
    reference_date: date,
) -> pd.DataFrame:
    """Attach AuM/holders/span metadata and drop funds with no AuM in the lookback."""
    quote_metrics = _load_quote_metrics(db, settings, reference_date)
    out = eligible.merge(quote_metrics, on=["fund_cnpj", "subclass_id"], how="left")
    out = out[out["median_aum"].notna()].reset_index(drop=True)
    logger.debug("universe: %d combos with AuM in lookback window", len(out))
    return out


def _apply_quote_filters(
    eligible: pd.DataFrame, settings: Settings, reference_date: date
) -> pd.DataFrame:
    """Drop stale, short-history, and sparse funds. Removes quote-helper columns."""
    if eligible.empty:
        return eligible.drop(columns=_QUOTE_HELPER_COLS, errors="ignore")

    ref_ts = pd.Timestamp(reference_date)
    last_quote = pd.to_datetime(eligible["last_quote_date"])
    first_quote = pd.to_datetime(eligible["first_quote_date"])
    span_cdays = (last_quote - first_quote).dt.days
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
    span_mask = span_cdays >= settings.universe.min_span_days
    dense_mask = obs_ratio >= settings.universe.min_obs_ratio

    logger.debug(
        "universe: dropped %d stale (>%dd), %d short (<%dd span), %d sparse (obs/bday<%.2f)",
        int((~fresh_mask).sum()),
        settings.universe.max_quote_staleness_days,
        int((fresh_mask & ~span_mask).sum()),
        settings.universe.min_span_days,
        int((fresh_mask & span_mask & ~dense_mask).sum()),
        settings.universe.min_obs_ratio,
    )
    return (
        eligible[fresh_mask & span_mask & dense_mask]
        .drop(columns=_QUOTE_HELPER_COLS)
        .reset_index(drop=True)
    )


def _join_anbima(
    db: DuckDBWarehouse, eligible: pd.DataFrame, reference_date: date
) -> pd.DataFrame:
    """Add target_taxation, redemption_days, min_investment from ANBIMA."""
    anbima = _load_snapshot(db, "staging.anbima", reference_date)
    return eligible.merge(
        _safe_select(anbima, _ANBIMA_COLS),
        on=["fund_cnpj", "subclass_id"],
        how="left",
    ).rename(columns={"min_initial_investment": "min_investment"})


def _apply_aum_holders_filter(
    eligible: pd.DataFrame, settings: Settings
) -> pd.DataFrame:
    """Drop funds below min_aum or min_cotistas thresholds."""
    out = eligible[
        (eligible["median_holders"].fillna(0) > settings.universe.min_cotistas)
        & (eligible["median_aum"].fillna(0) > settings.universe.min_aum)
    ].reset_index(drop=True)
    logger.debug("universe: %d combos pass aum/holders threshold", len(out))
    return out


def build_universe(
    db: DuckDBWarehouse, reference_date: date, settings: Settings
) -> pd.DataFrame:
    """Build eligible fund universe keyed by (fund_cnpj, subclass_id)."""
    registry = _load_snapshot(db, "staging.registry", reference_date)
    eligible = _filter_eligible_registry(registry, reference_date)
    eligible = _join_fees(db, eligible, reference_date)
    eligible = _join_quote_metrics(db, eligible, settings, reference_date)
    eligible = _apply_quote_filters(eligible, settings, reference_date)
    eligible = _join_anbima(db, eligible, reference_date)
    eligible = _apply_aum_holders_filter(eligible, settings)

    null_cnpj = int(eligible["fund_cnpj"].isna().sum()) if not eligible.empty else 0
    if null_cnpj > 0:
        raise ValueError(f"universe: {null_cnpj} rows with NULL fund_cnpj after joins")

    eligible = eligible.copy()
    eligible["reference_date"] = reference_date
    missing = [c for c in _UNIVERSE_COLS if c not in eligible.columns]
    for c in missing:
        eligible[c] = pd.Series(dtype="object")
    logger.info("universe: %d eligible funds for %s", len(eligible), reference_date)
    return eligible[_UNIVERSE_COLS]
