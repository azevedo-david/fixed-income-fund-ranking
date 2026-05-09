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


def span_days(ri: pd.DataFrame) -> pd.Series:
    """Calendar days between first and last observed quote per fund."""
    dates = (
        ri.reset_index().groupby(GROUP_KEY, dropna=False)["date"].agg(["min", "max"])
    )
    return (dates["max"] - dates["min"]).dt.days.rename("span_days").astype("int64")


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

    return pd.concat(
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


def _apply_tax_layer(
    df: pd.DataFrame,
    cdi_window: dict[str, float],
    cdi_annual: float,
    settings: Settings,
) -> pd.DataFrame:
    """Apply IR tax rates and compute net returns/alphas.

    Tax rate is resolved via:
    1. Exact match on target_taxation in rates_by_taxation dict
    2. If not found, check if fund_name contains exempt keywords → 0% tax
    3. Otherwise use default_rate
    """
    df = df.copy()

    rates_dict = settings.tax.rates_by_taxation
    exempt_kws = settings.tax.exempt_keywords
    default_rate = settings.tax.default_rate

    def get_ir_rate(row: pd.Series) -> float:
        return resolve_ir_rate(
            row.get("target_taxation"),
            row.get("fund_name"),
            rates_dict,
            exempt_kws,
            default_rate,
        )

    df["ir_rate"] = df.apply(get_ir_rate, axis=1)

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

    db._con.register("_daily", daily)
    db._con.register("_cdi_ts", cdi.reset_index().rename(columns={"index": "date"}))
    try:
        aligned = db.execute("""
            WITH fund_date_range AS (
                SELECT fund_cnpj, subclass_id,
                       MIN(date) AS first_date,
                       MAX(date) AS last_date
                FROM _daily
                GROUP BY fund_cnpj, subclass_id
            ),
            cdi_per_fund AS (
                SELECT f.fund_cnpj, f.subclass_id, c.date, c.cdi_daily
                FROM fund_date_range f
                CROSS JOIN _cdi_ts c
                WHERE c.date >= f.first_date AND c.date <= f.last_date
            ),
            with_quotes AS (
                SELECT cpf.fund_cnpj, cpf.subclass_id, cpf.date, cpf.cdi_daily,
                       d.return_daily, d.nav
                FROM cdi_per_fund cpf
                LEFT JOIN _daily d
                    ON cpf.fund_cnpj = d.fund_cnpj
                   AND cpf.subclass_id IS NOT DISTINCT FROM d.subclass_id
                   AND cpf.date = d.date
            ),
            ffilled AS (
                SELECT fund_cnpj, subclass_id, date, cdi_daily,
                       FIRST_VALUE(return_daily) OVER (
                           PARTITION BY fund_cnpj, subclass_id
                           ORDER BY date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) FILTER (WHERE return_daily IS NOT NULL) AS return_daily,
                       FIRST_VALUE(nav) OVER (
                           PARTITION BY fund_cnpj, subclass_id
                           ORDER BY date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) FILTER (WHERE nav IS NOT NULL) AS nav
                FROM with_quotes
            )
            SELECT fund_cnpj, subclass_id, date, return_daily, nav, cdi_daily
            FROM ffilled
            ORDER BY fund_cnpj, subclass_id, date
            """).df()
    finally:
        db._con.unregister("_daily")
        db._con.unregister("_cdi_ts")

    ri = aligned.set_index(GROUP_KEY + ["date"])[["return_daily", "nav", "cdi_daily"]]
    ri["excess_daily"] = ri["return_daily"] - ri["cdi_daily"]

    funds_before_span = ri.index.droplevel("date").unique().shape[0]
    ri = filter_min_span(ri, settings.universe.min_span_days)
    funds_after_span = ri.index.droplevel("date").unique().shape[0]

    if ri.empty:
        logger.warning(
            "metrics: all funds filtered by min_span_days=%d for %s",
            settings.universe.min_span_days,
            reference_date,
        )
        return pd.DataFrame(columns=["fund_cnpj", "subclass_id", "reference_date"])

    logger.info(
        "metrics: %d funds after span filter for %s (removed %d)",
        funds_after_span,
        reference_date,
        funds_before_span - funds_after_span,
    )

    metrics = compute_fund_metrics(ri, cdi, settings.windows, reference_date)

    ref = pd.Timestamp(reference_date)
    cdi_window = cdi_window_returns(cdi, ref, settings.windows)
    cdi_annual = _cdi_annualised(cdi, ref)

    df = metrics.reset_index()

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
        .drop_duplicates(subset=["cnpj", "subclass_id"])
    )
    before_merge = len(df)
    df = df.merge(
        meta.drop(columns=["fund_cnpj"]), on=["cnpj", "subclass_id"], how="left"
    )
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
