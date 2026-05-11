"""CVM daily quotes: raw.inf_diario → staging-ready DataFrame."""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_COLS = [
    "fund_cnpj",
    "subclass_id",
    "date",
    "nav",
    "aum",
    "inflows",
    "outflows",
    "shareholders",
]


def fetch_raw_daily_quotes_month(db: DuckDBWarehouse, ym: str) -> pd.DataFrame | None:
    """Fetch and clean one YYYYMM month of inf_diario for staging.

    Seeds (last known NAV per fund before this month) are read from whatever
    is already in staging.daily_quotes, so cross-month ffill is correct even
    when months are processed sequentially from scratch.
    """
    year, month = int(ym[:4]), int(ym[4:])
    from_date = date(year, month, 1)
    to_date = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

    raw = db.execute(
        "SELECT * FROM raw.inf_diario WHERE DT_COMPTC >= ? AND DT_COMPTC < ?",
        [from_date, to_date],
    ).df()

    if raw.empty:
        return None

    seeds = db.execute(
        """
        SELECT fund_cnpj, subclass_id, date, nav
        FROM staging.daily_quotes
        WHERE date < ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY fund_cnpj, subclass_id ORDER BY date DESC
        ) = 1
        """,
        [from_date],
    ).df()

    return _clean(raw, seeds if not seeds.empty else None)


def _clean(df: pd.DataFrame, seeds: pd.DataFrame | None = None) -> pd.DataFrame:
    out = df.copy()

    row_key = ["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"]
    n_before = len(out)
    out = (
        out.sort_values(row_key + ["TP_FUNDO_CLASSE"], na_position="first")
        .drop_duplicates(subset=row_key, keep="first")
        .sort_values(row_key)
        .reset_index(drop=True)
    )
    if n_before - len(out):
        logger.debug("daily_quotes: removed %d duplicate rows", n_before - len(out))

    if "NR_COTST" in out.columns:
        mask_dead = (out["VL_QUOTA"] == 0) & (out["NR_COTST"] == 0)
        if mask_dead.any():
            logger.debug("daily_quotes: dropped %d ghost rows", int(mask_dead.sum()))
        out = out[~mask_dead].reset_index(drop=True)

    mask_zero = out["VL_QUOTA"] == 0
    if "NR_COTST" in out.columns and "VL_PATRIM_LIQ" in out.columns:
        active = (out["NR_COTST"].fillna(0) > 0) | (out["VL_PATRIM_LIQ"].fillna(0) > 0)
        mask_bad_zero = mask_zero & active
    else:
        mask_bad_zero = mask_zero

    if mask_bad_zero.any():
        logger.debug(
            "daily_quotes: masked %d zero-nav rows (active fund)",
            int(mask_bad_zero.sum()),
        )
        out.loc[mask_bad_zero, "VL_QUOTA"] = np.nan

    out = out.rename(
        columns={
            "CNPJ_FUNDO_CLASSE": "fund_cnpj",
            "ID_SUBCLASSE": "subclass_id",
            "DT_COMPTC": "date",
            "VL_QUOTA": "nav",
            "VL_PATRIM_LIQ": "aum",
            "CAPTC_DIA": "inflows",
            "RESG_DIA": "outflows",
            "NR_COTST": "shareholders",
        }
    )[_COLS]

    if seeds is not None and not seeds.empty:
        seed_rows = seeds[["fund_cnpj", "subclass_id", "date", "nav"]].reindex(
            columns=_COLS
        )
        seed_rows["_seed"] = True
        out["_seed"] = False
        combined = pd.concat([seed_rows, out], ignore_index=True).sort_values(
            ["fund_cnpj", "subclass_id", "date"]
        )
        combined["nav"] = combined.groupby(["fund_cnpj", "subclass_id"], dropna=False)[
            "nav"
        ].ffill()
        return (
            combined[~combined["_seed"]].drop(columns=["_seed"]).reset_index(drop=True)
        )

    out["nav"] = out.groupby(["fund_cnpj", "subclass_id"], dropna=False)["nav"].ffill()
    return out
