"""Shared helpers for publish and report tasks."""

from __future__ import annotations

from datetime import date

import pandas as pd

from ..config import RankingCombo, Settings
from ..storage import DuckDBWarehouse


def load_rankings(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
) -> list[tuple[RankingCombo, pd.DataFrame]]:
    """Reconstruct list[tuple[RankingCombo, DataFrame]] from marts.rankings.

    Each DataFrame is sorted by rank and indexed on (cnpj, subclass_id) to
    match the contract expected by compute/report.py and compute/publish.py.
    """
    df = db.execute(
        "SELECT * FROM marts.rankings WHERE reference_date = ?", [reference_date]
    ).df()

    results = []
    for combo in settings.rankings:
        subset = (
            df[
                (df["purpose"] == combo.purpose)
                & (df["profile"] == combo.profile)
                & (df["investor_type"] == combo.investor_type)
            ]
            .sort_values("rank")
            .copy()
        )
        subset = subset.set_index(["fund_cnpj", "subclass_id"])
        subset.index.names = ["cnpj", "subclass_id"]
        results.append((combo, subset))

    return results


def universe_size(db: DuckDBWarehouse, reference_date: date) -> int:
    return db.execute(
        "SELECT COUNT(*) FROM marts.universe WHERE reference_date = ?", [reference_date]
    ).fetchone()[0]
