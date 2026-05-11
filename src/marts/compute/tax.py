"""IR tax computation for fixed-income fund returns."""

from __future__ import annotations

import numpy as np
import pandas as pd


def resolve_ir_rate(
    target_taxation: str | None,
    fund_name: str | None,
    rates_by_taxation: dict[str, float],
    exempt_keywords: list[str],
    default_rate: float,
) -> float:
    """Map target taxation + fund name to IR rate; direct lookup → exempt keyword → default."""
    if target_taxation in rates_by_taxation:
        return rates_by_taxation[target_taxation]
    fund_name_up = str(fund_name or "").upper()
    if any(kw in fund_name_up for kw in exempt_keywords):
        return 0.0
    return default_rate


def apply_ir(gross_return: float, rate: float) -> float:
    """IR on accumulated period return: r_net = r_gross - max(r_gross × rate, 0)."""
    if pd.isna(gross_return):
        return np.nan
    a = 1.0 + gross_return
    return a - max((a - 1.0) * rate, 0.0) - 1.0


def net_series(gross: pd.Series, rates: pd.Series) -> pd.Series:
    """Vectorised apply_ir over a Series."""
    a = 1.0 + gross
    tax = np.maximum((a - 1.0) * rates, 0.0)
    return (a - tax) - 1.0
