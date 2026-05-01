"""Build the full metrics DataFrame ready for ranking (``metrics_df``).

Pipeline:
    1. Fetch cleaned daily returns (CVM) and CDI daily series (BCB).
    2. Index returns on (CNPJ, ID_SUBCLASSE, date), join CDI, compute
       ``excess_daily``.
    3. Span filter: drop funds with less than ``settings.universe.min_span_days``
       between their first and last quote.
    4. Compute every metric:
         - Trailing returns and alphas (1m..24m)
         - %-of-months above CDI
         - Volatility, Sharpe (raw + excess), max drawdown
         - Annualised return (CAGR)
         - Span (days)
    5. Apply IR per fund (target taxation + isento keyword override) to all
       accumulated returns and to CDI, producing net returns/alphas.
    6. Map ``Publico_Alvo`` → ``investor_level`` (0/1/2).

The output is keyed by (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE), one row per fund.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ..config import Settings
from ..ingestion.bcb import fetch_cdi_daily
from ..ingestion.cvm import load_inf_diario
from .returns import (
    annualized_return,
    cdi_window_returns,
    daily_returns,
    monthly_returns,
    pct_months_above_cdi,
    trailing_returns,
)
from .risk import max_drawdown, volatility_and_sharpe

logger = logging.getLogger(__name__)

GROUP_KEY = ["cnpj", "subclass_id"]

# Sentinel string used while computing on the (cnpj, subclass_id, date)
# MultiIndex: NaN ≠ NaN in MultiIndex equality, which corrupts joins/concats.
# Restored to NaN once we drop back to a 2-level (cnpj, subclass_id) index.
_SUB_SENTINEL = "__NS__"


# ---------------------------------------------------------------------------
# Span (days between first and last observation)
# ---------------------------------------------------------------------------

def span_days(ri: pd.DataFrame) -> pd.Series:
    """Calendar days between the first and last observed quote per fund."""
    dates = (
        ri.reset_index()
          .groupby(GROUP_KEY, dropna=False)["date"]
          .agg(["min", "max"])
    )
    return (dates["max"] - dates["min"]).dt.days.rename("span_days")


def filter_min_span(ri: pd.DataFrame, min_span_days: int) -> pd.DataFrame:
    """Drop fund-rows from ``ri`` whose span is below ``min_span_days``."""
    span = span_days(ri)
    keep = set(map(tuple, span[span >= min_span_days].index.tolist()))
    mask = ri.index.droplevel("date").isin(keep)
    return ri[mask]


# ---------------------------------------------------------------------------
# IR + investor level helpers
# ---------------------------------------------------------------------------

def resolve_ir_rate(
    tributacao: str | None,
    nome: str | None,
    rates_by_tributacao: dict[str, float],
    isento_keywords: list[str],
    default_rate: float,
) -> float:
    """Map a fund's target taxation + name to its accumulated-return IR rate.

    Order of precedence:
      1. Direct lookup in ``rates_by_tributacao``.
      2. Name match against any keyword in ``isento_keywords`` → 0.0.
      3. ``default_rate``.
    """
    if tributacao in rates_by_tributacao:
        return rates_by_tributacao[tributacao]
    nome_up = str(nome or "").upper()
    if any(kw in nome_up for kw in isento_keywords):
        return 0.0
    return default_rate


def apply_ir(gross_return: float, rate: float) -> float:
    """IR on accumulated period return: ``r_net = r_gross - max(r_gross × rate, 0)``."""
    if pd.isna(gross_return):
        return np.nan
    a = 1.0 + gross_return
    return a - max((a - 1.0) * rate, 0.0) - 1.0


def _net_series(gross: pd.Series, rates: pd.Series) -> pd.Series:
    """Vectorised ``apply_ir`` over a Series."""
    a = 1.0 + gross
    tax = np.maximum((a - 1.0) * rates, 0.0)
    return (a - tax) - 1.0


def map_investor_level(publico_alvo: pd.Series) -> pd.Series:
    """0 = geral (default), 1 = qualificado, 2 = profissional."""
    s = publico_alvo.fillna("").str.lower()
    return pd.Series(
        np.where(s.str.contains("profissional"), 2,
        np.where(s.str.contains("qualificado"), 1, 0)),
        index=publico_alvo.index,
        name="investor_level",
    )


# ---------------------------------------------------------------------------
# Sub-sentinel ↔ NaN index plumbing
# ---------------------------------------------------------------------------

def _index_with_sentinel(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN ``subclass_id`` in the row index with ``_SUB_SENTINEL``."""
    idx = df.index
    new_idx = pd.MultiIndex.from_arrays([
        idx.get_level_values("cnpj"),
        idx.get_level_values("subclass_id").fillna(_SUB_SENTINEL),
        idx.get_level_values("date"),
    ], names=["cnpj", "subclass_id", "date"])
    out = df.copy()
    out.index = new_idx
    return out


def _restore_subclasse_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Reverse ``_index_with_sentinel`` for a 2-level (cnpj, subclass_id) index."""
    idx = df.index
    sub = (
        idx.get_level_values("subclass_id")
           .to_series()
           .replace({_SUB_SENTINEL: np.nan})
           .values
    )
    out = df.copy()
    out.index = pd.MultiIndex.from_arrays([
        idx.get_level_values("cnpj"), sub,
    ], names=GROUP_KEY)
    return out


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _build_indexed_returns(
    daily: pd.DataFrame,
    cdi_daily: pd.Series,
) -> pd.DataFrame:
    """Index daily returns on (CNPJ, ID_SUBCLASSE, date) and join CDI."""
    ri = daily.set_index(GROUP_KEY + ["date"])[["return_daily", "nav"]].sort_index()
    ri = _index_with_sentinel(ri)

    fund_dates = pd.Index(
        sorted(ri.index.get_level_values("date").unique()), name="date",
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
    """Run every per-fund metric and stitch into a single DataFrame."""
    windows = settings.windows

    trailing = trailing_returns(ri, windows)
    cdi_w = cdi_window_returns(
        cdi_daily,
        reference_date=pd.Timestamp(settings.reference_date),
        windows=windows,
    )
    # Alpha = fund_return - cdi_return (CDI is a scalar per window, broadcast).
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
    """Per-fund net returns and net alphas for every window + the annualised one.

    CDI uses a fixed long-term IR rate (``settings.tax.cdi_ir_rate``); funds use
    the rate resolved from their target taxation / name.
    """
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
    """CAGR of the CDI series up to and including ``reference_date``."""
    s = cdi_daily[cdi_daily.index <= reference_date]
    span = (reference_date - s.index.min()).days
    if span <= 0:
        return 0.0
    return float((1.0 + s).prod() ** (252.0 / span) - 1.0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_metrics(
    universe: pd.DataFrame,
    settings: Settings,
    force: bool = False,
) -> pd.DataFrame:
    """End-to-end metrics build from the universe to ``metrics_df``.

    Args:
        universe: Output of ``compute.universe.build_universe`` — one row per
            (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE) with metadata, fees, AuM, ANBIMA.
        settings: Project settings.
        force: Re-fetch all upstream data (inf_diario + CDI) bypassing caches.

    Output columns: trailing returns/alphas (gross + net) for every window,
    annualised return/alpha, pct_months_above_cdi, volatility, sharpe_*,
    max_drawdown, span_days, ir_rate, investor_level, plus universe metadata.
    """
    universe_keys = set(zip(
        universe["cnpj"],
        universe["subclass_id"].where(universe["subclass_id"].notna(), None),
    ))
    # 1. Daily quotes for the full window
    logger.info("metrics: loading daily quotes for %d funds (%s → %s)",
                len(universe_keys), settings.quotes_start, settings.quotes_end)
    inf = load_inf_diario(
        start=settings.quotes_start,
        end=settings.quotes_end,
        universe_keys=universe_keys,
        force=force,
    )
    logger.info("metrics: %d rows loaded", len(inf))
    inf = inf.rename(columns={
        "CNPJ_FUNDO_CLASSE": "cnpj",
        "ID_SUBCLASSE":      "subclass_id",
        "DT_COMPTC":         "date",
        "VL_QUOTA":          "nav",
    })
    daily = daily_returns(inf)

    # 2. CDI: cover a couple of months of buffer for trailing-window margins
    cdi = fetch_cdi_daily(
        start=pd.Timestamp(settings.quotes_start) - pd.DateOffset(months=2),
        end=pd.Timestamp(settings.quotes_end),
        timeout=settings.bcb.timeout_seconds,
        force=force,
    )

    # 3. Index, align, span filter
    ri = _build_indexed_returns(daily, cdi)
    ri = filter_min_span(ri, settings.universe.min_span_days)
    n_kept = ri.index.droplevel("date").unique().shape[0]
    n_dropped = len(universe_keys) - n_kept
    logger.info("metrics: %d funds ready (dropped %d with < %d days of history)",
                n_kept, n_dropped, settings.universe.min_span_days)

    # 4. Per-fund metrics
    metrics = _compute_per_fund_metrics(ri, cdi, settings)

    # 5. CDI windows + annualised (used by the tax layer)
    ref = pd.Timestamp(settings.reference_date)
    cdi_window = cdi_window_returns(cdi, ref, settings.windows)
    cdi_annual = _cdi_annualised(cdi, ref)

    # 6. Merge universe metadata (one row per fund, sentinel-based join)
    df = metrics.reset_index()
    meta_cols = [
        "cnpj", "subclass_id",
        "fund_name", "target_investor",
        "target_taxation", "redemption_days", "min_investment",
    ]
    meta = (
        universe[meta_cols]
        .copy()
        .fillna({"subclass_id": _SUB_SENTINEL})
        .drop_duplicates(subset=GROUP_KEY)
    )
    df["subclass_id"] = df["subclass_id"].fillna(_SUB_SENTINEL)
    df = df.merge(meta, on=GROUP_KEY, how="left")
    df["subclass_id"] = df["subclass_id"].replace(_SUB_SENTINEL, np.nan)

    # 7. Tax layer + investor level
    df = _apply_tax_layer(df, cdi_window, cdi_annual, settings)
    df["investor_level"] = map_investor_level(df["target_investor"])

    logger.info("metrics: all metrics computed for %d funds", len(df))
    return df
