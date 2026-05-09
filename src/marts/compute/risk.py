"""Risk metrics on flat daily-returns DataFrames."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .returns import GROUP_KEY

PERIODS_PER_YEAR = 252


def volatility_and_sharpe(ri: pd.DataFrame) -> pd.DataFrame:
    """Annualised volatility and Sharpe (raw + excess-of-CDI).

    Output: flat DataFrame with GROUP_KEY + volatility, sharpe_excess, sharpe_raw.
    """
    g = ri.groupby(GROUP_KEY, dropna=False)
    mean_r = g["return_daily"].mean()
    mean_excess = g["excess_daily"].mean()
    std_r = g["return_daily"].std()

    sqrtN = np.sqrt(PERIODS_PER_YEAR)
    return pd.DataFrame(
        {
            "volatility": std_r * sqrtN,
            "sharpe_excess": (mean_excess / std_r) * sqrtN,
            "sharpe_raw": (mean_r / std_r) * sqrtN,
        }
    ).reset_index()


def max_drawdown(ri: pd.DataFrame) -> pd.DataFrame:
    """Worst peak-to-trough drawdown of the cumulative-return curve per fund.

    Output: flat DataFrame with GROUP_KEY + max_drawdown columns.
    """

    def _mdd(sub: pd.DataFrame) -> float:
        s = sub["return_daily"].dropna()
        if len(s) < 2:
            return np.nan
        nav = (1.0 + s).cumprod()
        return float(nav.div(nav.cummax()).sub(1.0).min())

    return (
        ri.groupby(GROUP_KEY, dropna=False)
        .apply(_mdd, include_groups=False)
        .rename("max_drawdown")
        .reset_index()
    )
