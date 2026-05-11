"""Daily, trailing, monthly, and annualised return calculations."""

from __future__ import annotations

import logging
from datetime import date

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


def _nav_as_of(quotas: pd.DataFrame, cutoff: pd.Timestamp) -> pd.Series:
    """Last NAV at or before cutoff per fund. quotas must be sorted by GROUP_KEY+date."""
    return (
        quotas[quotas["date"] <= cutoff].groupby(GROUP_KEY, dropna=False)["nav"].last()
    )


def trailing_returns(
    ri: pd.DataFrame, windows: dict[str, int], reference_date: date
) -> pd.DataFrame:
    """Trailing point-to-point returns anchored at reference_date.

    For each fund × window: v_end / v_start − 1, where v_end is the last
    NAV at or before reference_date and v_start is the last NAV at or
    before reference_date − months. NaN if either anchor has no quote.
    """
    quotas = ri[GROUP_KEY + ["nav", "date"]].sort_values(GROUP_KEY + ["date"])
    ref_ts = pd.Timestamp(reference_date)
    v_end = _nav_as_of(quotas, ref_ts).rename("v_end")

    result = v_end.to_frame()
    for label, months in windows.items():
        cutoff = ref_ts - pd.DateOffset(months=months)
        v_start = _nav_as_of(quotas, cutoff).rename("v_start")
        result = result.join(v_start, how="left")
        result[f"return_{label}"] = np.where(
            result["v_start"].gt(0),
            result["v_end"] / result["v_start"] - 1.0,
            np.nan,
        )
        result = result.drop(columns=["v_start"])

    return result.drop(columns=["v_end"]).reset_index()


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
