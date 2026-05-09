"""Pure metrics computation functions — no ingestion dependencies.

Extracted from compute/metrics.py so the marts layer can import them
without pulling in the deprecated load_inf_diario / fetch_cdi_daily.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .returns import (
    GROUP_KEY,
    annualized_return,
    cdi_window_returns,
    monthly_returns,
    pct_months_above_cdi,
    trailing_returns,
)
from .risk import max_drawdown, volatility_and_sharpe
from ...config import Settings

logger = logging.getLogger(__name__)

_SUB_SENTINEL = "__NS__"


def span_days(ri: pd.DataFrame) -> pd.Series:
    """Calendar days between the first and last observed quote per fund."""
    dates = (
        ri.reset_index().groupby(GROUP_KEY, dropna=False)["date"].agg(["min", "max"])
    )
    return (dates["max"] - dates["min"]).dt.days.rename("span_days")


def filter_min_span(ri: pd.DataFrame, min_span_days: int) -> pd.DataFrame:
    """Drop funds whose history spans fewer than min_span_days calendar days."""
    span = span_days(ri)
    keep = set(map(tuple, span[span >= min_span_days].index.tolist()))
    mask = ri.index.droplevel("date").isin(keep)
    return ri[mask]


def resolve_ir_rate(
    tributacao: str | None,
    nome: str | None,
    rates_by_tributacao: dict[str, float],
    isento_keywords: list[str],
    default_rate: float,
) -> float:
    """Map target taxation + name to IR rate; direct lookup → exempt keyword → default."""
    if tributacao in rates_by_tributacao:
        return rates_by_tributacao[tributacao]
    nome_up = str(nome or "").upper()
    if any(kw in nome_up for kw in isento_keywords):
        return 0.0
    return default_rate


def apply_ir(gross_return: float, rate: float) -> float:
    """IR on accumulated period return: r_net = r_gross - max(r_gross × rate, 0)."""
    if pd.isna(gross_return):
        return np.nan
    a = 1.0 + gross_return
    return a - max((a - 1.0) * rate, 0.0) - 1.0


def _net_series(gross: pd.Series, rates: pd.Series) -> pd.Series:
    a = 1.0 + gross
    tax = np.maximum((a - 1.0) * rates, 0.0)
    return (a - tax) - 1.0


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


def _index_with_sentinel(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    new_idx = pd.MultiIndex.from_arrays(
        [
            idx.get_level_values("cnpj"),
            idx.get_level_values("subclass_id").fillna(_SUB_SENTINEL),
            idx.get_level_values("date"),
        ],
        names=["cnpj", "subclass_id", "date"],
    )
    out = df.copy()
    out.index = new_idx
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
        [idx.get_level_values("cnpj"), sub],
        names=GROUP_KEY,
    )
    return out


def _build_indexed_returns(
    daily: pd.DataFrame,
    cdi_daily: pd.Series,
) -> pd.DataFrame:
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


def _compute_per_fund_metrics(
    ri: pd.DataFrame,
    cdi_daily: pd.Series,
    settings: Settings,
) -> pd.DataFrame:
    windows = settings.windows
    trailing = trailing_returns(ri, windows)
    cdi_w = cdi_window_returns(
        cdi_daily,
        reference_date=pd.Timestamp(settings.reference_date),
        windows=windows,
    )
    for label, value in cdi_w.items():
        trailing[f"alpha_{label}"] = trailing[f"return_{label}"] - value

    metrics = pd.concat(
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
    return _restore_subclasse_nan(metrics)


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
            settings.tax.rates_by_taxation,
            settings.tax.exempt_keywords,
            settings.tax.default_rate,
        ),
        axis=1,
    )

    cdi_ir = settings.tax.cdi_ir_rate
    labels = list(settings.windows.keys()) + ["annualized"]
    for label in labels:
        gross_col = f"return_{label}" if label != "annualized" else "annualized_return"
        df[f"return_{label}_net"] = _net_series(df[gross_col], df["ir_rate"])
        cdi_gross = cdi_window[label] if label in cdi_window else cdi_annual
        cdi_net = apply_ir(cdi_gross, cdi_ir)
        df[f"alpha_{label}_net"] = df[f"return_{label}_net"] - cdi_net

    return df


def _cdi_annualised(cdi_daily: pd.Series, reference_date: pd.Timestamp) -> float:
    """CAGR of the CDI series up to and including reference_date."""
    s = cdi_daily[cdi_daily.index <= reference_date]
    span = (reference_date - s.index.min()).days
    if span <= 0:
        return 0.0
    return float((1.0 + s).prod() ** (252.0 / span) - 1.0)
