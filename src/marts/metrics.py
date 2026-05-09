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
    daily_returns,
    monthly_returns,
    pct_months_above_cdi,
    trailing_returns,
)
from .compute.risk import max_drawdown, volatility_and_sharpe
from .compute.tax import apply_ir, net_series, resolve_ir_rate

logger = logging.getLogger(__name__)

_SUB_SENTINEL = "__NS__"


def _index_with_sentinel(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    out = df.copy()
    out.index = pd.MultiIndex.from_arrays(
        [
            idx.get_level_values("cnpj"),
            idx.get_level_values("subclass_id").fillna(_SUB_SENTINEL),
            idx.get_level_values("date"),
        ],
        names=["cnpj", "subclass_id", "date"],
    )
    return out


def _restore_subclasse_nan(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    sub = (
        idx.get_level_values("subclass_id")
        .to_series()
        .replace({_SUB_SENTINEL: np.nan})
        .values
    )
    out = df.copy()
    out.index = pd.MultiIndex.from_arrays(
        [idx.get_level_values("cnpj"), sub], names=GROUP_KEY
    )
    return out


def span_days(ri: pd.DataFrame) -> pd.Series:
    """Calendar days between first and last observed quote per fund."""
    dates = (
        ri.reset_index().groupby(GROUP_KEY, dropna=False)["date"].agg(["min", "max"])
    )
    return (dates["max"] - dates["min"]).dt.days.rename("span_days")


def filter_min_span(ri: pd.DataFrame, min_span_days: int) -> pd.DataFrame:
    """Drop funds with fewer than min_span_days of history."""
    span = span_days(ri)
    keep = set(map(tuple, span[span >= min_span_days].index.tolist()))
    return ri[ri.index.droplevel("date").isin(keep)]


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


def _build_indexed_returns(daily: pd.DataFrame, cdi_daily: pd.Series) -> pd.DataFrame:
    ri = daily.set_index(GROUP_KEY + ["date"])[["return_daily", "nav"]].sort_index()
    ri = _index_with_sentinel(ri)
    fund_dates = pd.Index(
        sorted(ri.index.get_level_values("date").unique()), name="date"
    )
    cdi_aligned = cdi_daily.reindex(fund_dates).ffill()
    n_missing = int(cdi_aligned.isna().sum())
    if n_missing:
        logger.warning("CDI alignment: %d dates still NaN after ffill", n_missing)
    ri = ri.join(cdi_aligned, on="date")
    ri["excess_daily"] = ri["return_daily"] - ri["cdi_daily"]
    return ri


def _cdi_annualised(cdi_daily: pd.Series, reference_date: pd.Timestamp) -> float:
    s = cdi_daily[cdi_daily.index <= reference_date]
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
    """Compute trailing returns, alphas, and risk metrics per fund."""
    trailing = trailing_returns(ri, windows)
    cdi_w = cdi_window_returns(
        cdi_daily, reference_date=pd.Timestamp(reference_date), windows=windows
    )
    for label, value in cdi_w.items():
        trailing[f"alpha_{label}"] = trailing[f"return_{label}"] - value

    return _restore_subclasse_nan(
        pd.concat(
            [
                trailing,
                annualized_return(ri),
                pct_months_above_cdi(monthly_returns(ri, cdi_daily)),
                volatility_and_sharpe(ri),
                max_drawdown(ri),
                span_days(ri),
            ],
            axis=1,
        )
    )


def _apply_tax_layer(
    df: pd.DataFrame,
    cdi_window: dict[str, float],
    cdi_annual: float,
    settings: Settings,
) -> pd.DataFrame:
    df = df.copy()
    df["ir_rate"] = df.apply(
        lambda r: resolve_ir_rate(
            r.get("target_taxation"),
            r.get("fund_name"),
            settings.tax.rates_by_tributacao,
            settings.tax.exempt_keywords,
            settings.tax.default_rate,
        ),
        axis=1,
    )
    cdi_ir = settings.tax.cdi_ir_rate
    labels = list(settings.windows.keys()) + ["annualized"]
    for label in labels:
        gross_col = f"return_{label}" if label != "annualized" else "annualized_return"
        df[f"return_{label}_net"] = net_series(df[gross_col], df["ir_rate"])
        cdi_gross = cdi_window[label] if label in cdi_window else cdi_annual
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

    cdi_df = db.execute(
        "SELECT date, rate FROM staging.cdi_rates WHERE date >= ? AND date <= ? ORDER BY date",
        [cdi_start, reference_date],
    ).df()
    cdi = pd.Series(
        cdi_df["rate"].values,
        index=pd.to_datetime(cdi_df["date"]),
        name="cdi_daily",
    )

    daily = daily_returns(quotes_df)
    ri = _build_indexed_returns(daily, cdi)
    ri = filter_min_span(ri, settings.universe.min_span_days)
    logger.info(
        "metrics: %d funds after span filter for %s",
        ri.index.droplevel("date").unique().shape[0],
        reference_date,
    )

    metrics = compute_fund_metrics(ri, cdi, settings.windows, reference_date)

    ref = pd.Timestamp(reference_date)
    cdi_window = cdi_window_returns(cdi, ref, settings.windows)
    cdi_annual = _cdi_annualised(cdi, ref)

    df = metrics.reset_index()
    df["subclass_id"] = df["subclass_id"].fillna(_SUB_SENTINEL)

    meta_cols = [
        "fund_cnpj",
        "subclass_id",
        "fund_name",
        "target_investor",
        "target_taxation",
        "redemption_days",
        "min_investment",
    ]
    meta = (
        universe_df[meta_cols]
        .copy()
        .assign(cnpj=lambda x: x["fund_cnpj"])
        .fillna({"subclass_id": _SUB_SENTINEL})
        .drop_duplicates(subset=["cnpj", "subclass_id"])
    )
    df = df.merge(
        meta.drop(columns=["fund_cnpj"]), on=["cnpj", "subclass_id"], how="left"
    )
    df["subclass_id"] = df["subclass_id"].replace(_SUB_SENTINEL, np.nan)

    df = _apply_tax_layer(df, cdi_window, cdi_annual, settings)
    df["investor_level"] = map_investor_level(df["target_investor"])

    df = df.rename(columns={"cnpj": "fund_cnpj"})
    df["reference_date"] = reference_date
    logger.info("metrics: %d rows computed for %s", len(df), reference_date)
    return df
