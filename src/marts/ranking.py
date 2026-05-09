"""Build rankings from marts.metrics."""

from __future__ import annotations

import logging

import pandas as pd

from .compute.ranking import rank_all
from ..config import Settings

logger = logging.getLogger(__name__)


def build_rankings(metrics_df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Rank funds for every combo in settings.rankings; return flat DataFrame."""
    df = metrics_df.rename(columns={"fund_cnpj": "cnpj"})

    results = rank_all(df, settings)

    rows = []
    for combo, ranked in results:
        if ranked.empty:
            logger.warning(
                "rankings: no results for %s/%s/%s",
                combo.purpose,
                combo.profile,
                combo.investor_type,
            )
            continue
        r = ranked.reset_index()
        r["purpose"] = combo.purpose
        r["profile"] = combo.profile
        r["investor_type"] = combo.investor_type
        rows.append(r)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True).rename(columns={"cnpj": "fund_cnpj"})
    logger.info("rankings: %d rows across %d combos", len(out), len(rows))
    return out
