"""Risk metrics on indexed daily returns.

All functions take a DataFrame ``ri`` indexed on
(CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, DT_COMPTC) with at least ``return_daily``,
``excess_daily`` and ``VL_QUOTA`` columns, and return a Series/DataFrame
indexed on (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

GROUP_KEY = ["cnpj", "subclass_id"]
PERIODS_PER_YEAR = 252


def volatility_and_sharpe(ri: pd.DataFrame) -> pd.DataFrame:
    """Annualised volatility and Sharpe (raw + excess-of-CDI).

    Sharpe annualisation: ``(mean × N) / (std × √N) = (mean / std) × √N``.
    """
    g = ri.groupby(level=GROUP_KEY, dropna=False)
    mean_r = g["return_daily"].mean()
    mean_excess = g["excess_daily"].mean()
    std_r = g["return_daily"].std()

    sqrtN = np.sqrt(PERIODS_PER_YEAR)
    return pd.DataFrame({
        "volatility":    std_r * sqrtN,
        "sharpe_excess": (mean_excess / std_r) * sqrtN,
        "sharpe_raw":    (mean_r / std_r) * sqrtN,
    })


def max_drawdown(ri: pd.DataFrame) -> pd.Series:
    """Worst peak-to-trough drawdown of the cumulative-return curve per fund."""
    def _mdd(sub: pd.DataFrame) -> float:
        s = sub["return_daily"].dropna()
        if len(s) < 2:
            return np.nan
        nav = (1.0 + s).cumprod()
        return float(nav.div(nav.cummax()).sub(1.0).min())

    return (
        ri.groupby(level=GROUP_KEY, dropna=False, group_keys=False)
          .apply(_mdd)
          .rename("max_drawdown")
    )
