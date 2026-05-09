"""IR tax computation for fixed-income fund returns."""

from __future__ import annotations

import numpy as np
import pandas as pd


def resolve_ir_rate(
    tributacao: str | None,
    nome: str | None,
    rates_by_tributacao: dict[str, float],
    isento_keywords: list[str],
    default_rate: float,
) -> float:
    """Map target taxation + fund name to IR rate; direct lookup → exempt keyword → default."""
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


def net_series(gross: pd.Series, rates: pd.Series) -> pd.Series:
    """Vectorised apply_ir over a Series."""
    a = 1.0 + gross
    tax = np.maximum((a - 1.0) * rates, 0.0)
    return (a - tax) - 1.0
