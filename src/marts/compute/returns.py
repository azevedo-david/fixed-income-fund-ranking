"""Daily, trailing, monthly, and annualised return calculations."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

GROUP_KEY = ["cnpj", "subclass_id"]


def daily_returns(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Compute daily nav pct_change per (cnpj, subclass_id).

    Output columns: cnpj, subclass_id, date, nav, return_daily.
    """
    df = df_clean[df_clean["nav"].notna()].sort_values(GROUP_KEY + ["date"]).copy()
    df["return_daily"] = df.groupby(GROUP_KEY, dropna=False)["nav"].pct_change(
        fill_method=None
    )
    n_inf = int(np.isinf(df["return_daily"]).sum())
    if n_inf:
        logger.warning(
            "daily_returns: dropping %d Inf return rows (nav crossed zero)", n_inf
        )
    mask = df["return_daily"].notna() & ~np.isinf(df["return_daily"])
    return df.loc[mask, GROUP_KEY + ["date", "nav", "return_daily"]].reset_index(
        drop=True
    )


def _trailing_one_window(quotas: pd.DataFrame, months: int) -> pd.DataFrame:
    """V_last / V_start − 1 per fund for a single trailing window.

    Input/output: flat DataFrame with GROUP_KEY columns.
    """
    last_dates = quotas.groupby(GROUP_KEY, dropna=False)["date"].max()
    cutoffs = last_dates - pd.DateOffset(months=months)

    out = {}
    for i, (key, sub) in enumerate(quotas.groupby(GROUP_KEY, dropna=False)):
        dates = sub["date"]
        cutoff = cutoffs.iloc[i]
        before = dates[dates <= cutoff]
        if len(before) == 0:
            out[key] = np.nan
            continue
        v_end = sub["nav"].iloc[-1]
        v_start_vals = sub.loc[sub["date"] == before.max(), "nav"]
        if v_start_vals.empty:
            out[key] = np.nan
            continue
        v_start = v_start_vals.iloc[0]
        if pd.isna(v_end) or pd.isna(v_start) or v_start == 0:
            out[key] = np.nan
        else:
            out[key] = v_end / v_start - 1.0

    s = pd.Series(out, name=f"return_{months}m")
    s.index.names = GROUP_KEY
    return s.reset_index()


def trailing_returns(ri: pd.DataFrame, windows: dict[str, int]) -> pd.DataFrame:
    """Trailing point-to-point returns for each fund × window.

    Output: flat DataFrame with GROUP_KEY + return_{label} columns.
    """
    quotas = ri[GROUP_KEY + ["nav", "date"]].sort_values(GROUP_KEY + ["date"])
    result = None
    for _label, m in windows.items():
        part = _trailing_one_window(quotas, m)
        result = (
            part if result is None else result.merge(part, on=GROUP_KEY, how="outer")
        )
    return result


def cdi_window_returns(
    cdi_daily: pd.Series,
    reference_date: pd.Timestamp,
    windows: dict[str, int],
) -> dict[str, float]:
    """Compounded CDI return over each trailing window ending at reference_date."""
    out: dict[str, float] = {}
    for label, m in windows.items():
        cutoff = reference_date - pd.DateOffset(months=m)
        s = cdi_daily[(cdi_daily.index > cutoff) & (cdi_daily.index <= reference_date)]
        out[label] = float((1.0 + s).prod() - 1.0)
    return out


def monthly_returns(ri: pd.DataFrame, cdi_daily: pd.Series) -> pd.DataFrame:
    """Calendar-month compounded returns per fund and CDI benchmark.

    Output columns: cnpj, subclass_id, month, r_fund, r_cdi.
    """
    cdi_monthly = (
        cdi_daily.to_frame("r_cdi")
        .assign(month=lambda d: d.index.values.astype("datetime64[M]"))
        .groupby("month")["r_cdi"]
        .apply(lambda s: (1 + s).prod() - 1)
    )
    df = ri.copy()
    df["month"] = df["date"].values.astype("datetime64[M]")
    return (
        df.groupby(GROUP_KEY + ["month"], dropna=False)
        .agg(r_fund=("return_daily", lambda s: (1 + s).prod() - 1))
        .reset_index()
        .merge(cdi_monthly.rename("r_cdi").reset_index(), on="month", how="left")
    )


def pct_months_above_cdi(monthly: pd.DataFrame) -> pd.DataFrame:
    """Fraction of months where the fund's compounded return beat CDI.

    Output: flat DataFrame with GROUP_KEY + pct_months_above_cdi columns.
    """
    monthly = monthly.copy()
    monthly["above"] = monthly["r_fund"] > monthly["r_cdi"]
    return (
        monthly.groupby(GROUP_KEY, dropna=False)["above"]
        .mean()
        .rename("pct_months_above_cdi")
        .reset_index()
    )


def annualized_return(ri: pd.DataFrame) -> pd.DataFrame:
    """CAGR: (V_last / V_first) ** (365 / span_days) - 1 per fund.

    Output: flat DataFrame with GROUP_KEY + annualized_return columns.
    """

    def _cagr(sub: pd.DataFrame) -> float:
        sub = sub.sort_values("date")
        q = sub["nav"].dropna()
        if len(q) < 2 or q.iloc[0] == 0:
            return np.nan
        span = (sub["date"].max() - sub["date"].min()).days
        if span == 0:
            return np.nan
        return float((q.iloc[-1] / q.iloc[0]) ** (365.0 / span) - 1.0)

    return (
        ri.groupby(GROUP_KEY, dropna=False)
        .apply(_cagr, include_groups=False)
        .rename("annualized_return")
        .reset_index()
    )
