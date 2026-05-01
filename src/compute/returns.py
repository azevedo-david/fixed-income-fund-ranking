"""Return calculations on cleaned inf_diario data.

Layers:
    1. ``daily_returns`` — nav pct_change per (CNPJ, ID_SUBCLASSE).
    2. ``trailing_returns`` — point-to-point V_end/V_start - 1 over each window.
    3. ``monthly_returns`` — calendar-month compounded returns.
    4. ``annualized_return`` — CAGR from first to last quote per fund.

All daily returns are net-of-tax as reported by CVM. Net-of-IR returns at
the period level are computed downstream in ``compute.metrics``.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

GROUP_KEY = ["cnpj", "subclass_id"]


def daily_returns(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Compute daily returns per (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE).

    Input: DataFrame from ``ingestion.cvm.load_inf_diario``.
    Returns columns: CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, date, nav, return_daily.
    """
    df = (
        df_clean[df_clean["nav"].notna()]
        .sort_values(GROUP_KEY + ["date"])
        .copy()
    )

    df["return_daily"] = (
        df.groupby(GROUP_KEY, dropna=False)["nav"].pct_change(fill_method=None)
    )

    n_inf = int(np.isinf(df["return_daily"]).sum())
    if n_inf:
        logger.warning("daily_returns: dropping %d Inf return rows (nav crossed zero)", n_inf)
    mask = df["return_daily"].notna() & ~np.isinf(df["return_daily"])
    return df.loc[mask, ["cnpj", "subclass_id", "date", "nav", "return_daily"]].reset_index(drop=True)


def _trailing_one_window(quotas: pd.DataFrame, months: int) -> pd.Series:
    """Vectorised V[last] / V[start] - 1 per fund for a single window.

    ``quotas`` must have a 3-level MultiIndex (CNPJ, ID_SUBCLASSE, date)
    and a single ``nav`` column. ``start`` is the latest observation
    on or before (last - N months); funds with no observation that old fall
    back to their first available date.
    """
    last_dates = quotas.groupby(level=GROUP_KEY, dropna=False).apply(
        lambda s: s.index.get_level_values("date").max()
    )
    cutoffs = last_dates - pd.DateOffset(months=months)

    out = {}
    for key, sub in quotas.groupby(level=GROUP_KEY, dropna=False):
        dates = sub.index.get_level_values("date")
        cutoff = cutoffs.loc[key]
        before = dates[dates <= cutoff]
        if len(before) == 0:
            out[key] = np.nan
            continue
        v_end = sub["nav"].iloc[-1]
        v_start = sub.loc[(key[0], key[1], before.max()), "nav"]
        if pd.isna(v_end) or pd.isna(v_start) or v_start == 0:
            out[key] = np.nan
        else:
            out[key] = v_end / v_start - 1.0
    s = pd.Series(out, name=f"return_{months}m")
    s.index.names = GROUP_KEY
    return s


def trailing_returns(
    ri: pd.DataFrame,
    windows: dict[str, int],
) -> pd.DataFrame:
    """Trailing point-to-point returns for each fund × window.

    ``ri`` must be indexed on (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, date) and
    contain a ``nav`` column. Returns a DataFrame indexed on
    (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE) with one column per window: ``return_1m``,
    ``return_3m``, ...
    """
    quotas = ri[["nav"]].sort_index()
    parts = {f"return_{label}": _trailing_one_window(quotas, m) for label, m in windows.items()}
    return pd.concat(parts.values(), axis=1)


def cdi_window_returns(
    cdi_daily: pd.Series,
    reference_date: pd.Timestamp,
    windows: dict[str, int],
) -> dict[str, float]:
    """Compounded CDI return over each trailing window ending at reference_date.

    Output is a scalar per window — same benchmark for every fund.
    """
    out: dict[str, float] = {}
    for label, m in windows.items():
        cutoff = reference_date - pd.DateOffset(months=m)
        s = cdi_daily[(cdi_daily.index > cutoff) & (cdi_daily.index <= reference_date)]
        out[label] = float((1.0 + s).prod() - 1.0)
    return out


def monthly_returns(
    ri: pd.DataFrame,
    cdi_daily: pd.Series,
) -> pd.DataFrame:
    """Calendar-month compounded returns per fund and CDI benchmark.

    Returns a DataFrame with columns (CNPJ, ID_SUBCLASSE, month, r_fund, r_cdi).
    Months are normalised to ``datetime64[M]``.
    """
    cdi_monthly = (
        cdi_daily.to_frame("r_cdi")
        .assign(month=lambda d: d.index.values.astype("datetime64[M]"))
        .groupby("month")["r_cdi"]
        .apply(lambda s: (1 + s).prod() - 1)
    )

    df = ri.reset_index()
    df["month"] = df["date"].values.astype("datetime64[M]")

    out = (
        df.groupby(GROUP_KEY + ["month"], dropna=False)
          .agg(r_fund=("return_daily", lambda s: (1 + s).prod() - 1))
          .reset_index()
          .merge(cdi_monthly.rename("r_cdi").reset_index(), on="month", how="left")
    )
    return out


def pct_months_above_cdi(monthly: pd.DataFrame) -> pd.Series:
    """Fraction of months where the fund's compounded return beat CDI."""
    monthly = monthly.copy()
    monthly["above"] = monthly["r_fund"] > monthly["r_cdi"]
    return (
        monthly.groupby(GROUP_KEY, dropna=False)["above"]
               .mean()
               .rename("pct_months_above_cdi")
    )


def annualized_return(ri: pd.DataFrame) -> pd.Series:
    """CAGR via nav: ``(V_last / V_first) ** (365 / span_days) - 1`` per fund.

    ``ri`` must be indexed on (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, date) and
    expose a ``nav`` column.
    """
    def _cagr(sub: pd.DataFrame) -> float:
        q = sub["nav"].dropna()
        if len(q) < 2 or q.iloc[0] == 0:
            return np.nan
        dates = sub.index.get_level_values("date")
        span = (dates.max() - dates.min()).days
        if span == 0:
            return np.nan
        return float((q.iloc[-1] / q.iloc[0]) ** (365.0 / span) - 1.0)

    return (
        ri.groupby(level=GROUP_KEY, dropna=False, group_keys=False)
          .apply(_cagr)
          .rename("annualized_return")
    )