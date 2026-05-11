"""Build fund metrics from staging tables."""

from __future__ import annotations

import logging
from dataclasses import replace as _replace
from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from ..config import Settings
from ..storage import DuckDBWarehouse
from .compute.returns import (
    GROUP_KEY,
    annualized_return,
    cdi_window_returns,
    monthly_returns,
    pct_months_above_cdi,
    trailing_returns,
)
from .compute.risk import max_drawdown, volatility_and_sharpe
from .compute.tax import apply_ir, net_series

logger = logging.getLogger(__name__)


def span_days(ri: pd.DataFrame) -> pd.DataFrame:
    """Calendar days between first and last observed quote per fund.

    Output: flat DataFrame with GROUP_KEY + span_days columns.
    """
    return (
        ri.groupby(GROUP_KEY, dropna=False)["date"]
        .agg(["min", "max"])
        .assign(span_days=lambda d: (d["max"] - d["min"]).dt.days.astype("int64"))
        .drop(columns=["min", "max"])
        .reset_index()
    )


def map_investor_level(publico_alvo: pd.Series) -> pd.Series:
    """0 = geral, 1 = qualificado, 2 = profissional."""
    s = publico_alvo.fillna("").str.lower()
    return pd.Series(
        np.where(
            s.str.contains("profissional"),
            2,
            np.where(s.str.contains("qualificado"), 1, 0),
        ),
        index=publico_alvo.index,
        name="investor_level",
    )


def _cdi_annualised(cdi_daily: pd.Series, reference_date: pd.Timestamp) -> float:
    s = cdi_daily[cdi_daily.index <= reference_date]
    if s.empty:
        return 0.0
    span = (reference_date - s.index.min()).days
    if span <= 0:
        return 0.0
    return float((1.0 + s).prod() ** (252.0 / span) - 1.0)


def compute_fund_metrics(
    ri: pd.DataFrame,
    cdi_daily: pd.Series,
    windows: dict[str, int],
    reference_date: date,
) -> pd.DataFrame:
    """Compute trailing returns, alphas, and risk metrics per fund.

    Output: flat DataFrame with GROUP_KEY + all metric columns.
    """
    trailing = trailing_returns(ri, windows, reference_date)
    cdi_w = cdi_window_returns(
        cdi_daily, reference_date=pd.Timestamp(reference_date), windows=windows
    )
    for label, value in cdi_w.items():
        trailing[f"alpha_{label}"] = trailing[f"return_{label}"] - value

    parts = [
        trailing,
        annualized_return(ri),
        pct_months_above_cdi(monthly_returns(ri, cdi_daily)),
        volatility_and_sharpe(ri, cdi_daily),
        max_drawdown(ri),
        span_days(ri),
    ]
    result = parts[0]
    for part in parts[1:]:
        result = result.merge(part, on=GROUP_KEY, how="outer")
    return result


def _apply_tax_layer(
    df: pd.DataFrame,
    cdi_window: dict[str, float],
    cdi_annual: float,
    settings: Settings,
) -> pd.DataFrame:
    """Apply IR tax rates and compute net returns/alphas.

    Rate resolution: exact match on target_taxation → exempt keyword → default_rate.
    """
    df = df.copy()

    rates_dict = settings.tax.rates_by_taxation
    exempt_kws = settings.tax.exempt_keywords
    default_rate = settings.tax.default_rate

    ir_rate = df["target_taxation"].map(rates_dict)
    unresolved = ir_rate.isna()
    if unresolved.any() and exempt_kws:
        pattern = "|".join(exempt_kws)
        is_exempt = (
            df.loc[unresolved, "fund_name"]
            .fillna("")
            .str.upper()
            .str.contains(pattern, regex=True)
        )
        ir_rate.loc[unresolved] = np.where(is_exempt, 0.0, default_rate)
    df["ir_rate"] = ir_rate.fillna(default_rate)

    cdi_ir = settings.tax.cdi_ir_rate
    labels = list(settings.windows.keys()) + ["annualized"]
    for label in labels:
        gross_col = f"return_{label}" if label != "annualized" else "annualized_return"
        df[f"return_{label}_net"] = net_series(df[gross_col], df["ir_rate"])
        cdi_gross = cdi_window.get(label, cdi_annual)
        df[f"alpha_{label}_net"] = df[f"return_{label}_net"] - apply_ir(
            cdi_gross, cdi_ir
        )
    return df


def build_metrics(
    db: DuckDBWarehouse,
    universe_df: pd.DataFrame,
    reference_date: date,
    settings: Settings,
) -> pd.DataFrame:
    """Build metrics DataFrame from staging.daily_quotes and staging.cdi_rates."""
    settings = _replace(settings, reference_date=reference_date)

    cdi_start = (
        pd.Timestamp(reference_date)
        - relativedelta(months=settings.max_window_months + 2)
    ).date()

    db._con.register(
        "_metrics_universe",
        universe_df[["fund_cnpj", "subclass_id"]].drop_duplicates(),
    )
    try:
        quotes_df = db.execute(
            """
            SELECT dq.fund_cnpj AS cnpj, dq.subclass_id, dq.date, dq.nav
            FROM staging.daily_quotes dq
            INNER JOIN _metrics_universe u
                ON dq.fund_cnpj = u.fund_cnpj
               AND dq.subclass_id IS NOT DISTINCT FROM u.subclass_id
            WHERE dq.date >= ? AND dq.date <= ?
            """,
            [settings.quotes_start, reference_date],
        ).df()
    finally:
        db._con.unregister("_metrics_universe")

    if quotes_df.empty:
        logger.warning("metrics: no quotes found for universe on %s", reference_date)
        return pd.DataFrame(columns=["fund_cnpj", "subclass_id", "reference_date"])

    cdi_df = db.execute(
        "SELECT date, rate FROM staging.cdi_rates WHERE date >= ? AND date <= ? ORDER BY date",
        [cdi_start, reference_date],
    ).df()
    if cdi_df.empty:
        logger.warning("metrics: no CDI data found for %s", reference_date)
        return pd.DataFrame(columns=["fund_cnpj", "subclass_id", "reference_date"])

    cdi = pd.Series(
        cdi_df["rate"].values,
        index=pd.to_datetime(cdi_df["date"]).rename("date"),
        name="cdi_daily",
    )

    clean_quotes = (
        quotes_df[quotes_df["nav"].notna()].sort_values(GROUP_KEY + ["date"]).copy()
    )
    clean_quotes["subclass_id"] = clean_quotes["subclass_id"].astype(object)

    db._con.register("_quotes", clean_quotes[GROUP_KEY + ["date", "nav"]])
    try:
        aligned = db.execute("""
            WITH with_returns AS (
                SELECT cnpj, subclass_id, date, nav,
                       nav / NULLIF(LAG(nav) OVER (
                           PARTITION BY cnpj, subclass_id ORDER BY date
                       ), 0) - 1 AS return_daily
                FROM _quotes
            )
            SELECT cnpj, subclass_id, date, return_daily, nav
            FROM with_returns
            WHERE return_daily IS NOT NULL
            ORDER BY cnpj, subclass_id, date
            """).df()
    finally:
        db._con.unregister("_quotes")

    aligned["subclass_id"] = aligned["subclass_id"].astype(object)
    ri = aligned

    metrics = compute_fund_metrics(ri, cdi, settings.windows, reference_date)
    metrics["subclass_id"] = metrics["subclass_id"].astype(object)

    ref = pd.Timestamp(reference_date)
    cdi_window = cdi_window_returns(cdi, ref, settings.windows)
    cdi_annual = _cdi_annualised(cdi, ref)

    meta_cols = [
        "fund_cnpj",
        "subclass_id",
        "fund_name",
        "target_investor",
        "target_taxation",
        "redemption_days",
        "min_investment",
    ]
    if not all(c in universe_df.columns for c in meta_cols):
        missing = [c for c in meta_cols if c not in universe_df.columns]
        raise KeyError(f"metrics: missing universe columns: {missing}")

    meta = (
        universe_df[meta_cols]
        .copy()
        .assign(cnpj=lambda x: x["fund_cnpj"])
        .drop(columns=["fund_cnpj"])
        .drop_duplicates(subset=["cnpj", "subclass_id"])
    )
    meta["subclass_id"] = meta["subclass_id"].astype(object)
    before_merge = len(metrics)
    df = metrics.merge(meta, on=GROUP_KEY, how="left")
    if len(df) > before_merge:
        logger.warning(
            "metrics: merge expanded rows (cartesian product). Before: %d, After: %d",
            before_merge,
            len(df),
        )

    df = _apply_tax_layer(df, cdi_window, cdi_annual, settings)
    df["investor_level"] = map_investor_level(df["target_investor"])
    df = df.rename(columns={"cnpj": "fund_cnpj"})
    df["reference_date"] = reference_date
    logger.info("metrics: %d rows computed for %s", len(df), reference_date)
    return df
