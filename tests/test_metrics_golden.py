"""Characterisation test for build_metrics at a pinned reference_date.

Captures current output as a golden parquet; subsequent runs assert equality.
Regenerate with `REGEN_GOLDEN=1 pytest tests/test_metrics_golden.py`.
Skipped automatically when the local DuckDB warehouse is unavailable.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import DEFAULT_CONFIG_PATH, Settings
from src.marts.metrics import build_metrics
from src.marts.universe import build_universe
from src.storage import DuckDBWarehouse

REFERENCE_DATE = date(2025, 5, 7)
DB_PATH = Path("data/fund_ranking.duckdb")
GOLDEN_PATH = (
    Path(__file__).parent / "fixtures" / f"metrics_{REFERENCE_DATE.isoformat()}.parquet"
)
SORT_KEYS = ["fund_cnpj", "subclass_id"]


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["subclass_id"] = (
        df["subclass_id"].astype(object).where(df["subclass_id"].notna())
    )
    return df.sort_values(SORT_KEYS, na_position="last").reset_index(drop=True)[
        sorted(df.columns)
    ]


@pytest.mark.skipif(not DB_PATH.exists(), reason="local DuckDB warehouse not available")
def test_build_metrics_matches_golden(tmp_path: Path) -> None:
    settings = Settings.from_yaml(DEFAULT_CONFIG_PATH)
    with DuckDBWarehouse(str(DB_PATH)) as db:
        universe = build_universe(db, REFERENCE_DATE, settings)
        actual = build_metrics(db, universe, REFERENCE_DATE, settings)

    if os.environ.get("REGEN_GOLDEN") == "1":
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        _normalise(actual).to_parquet(GOLDEN_PATH, index=False)
        pytest.skip(f"regenerated golden at {GOLDEN_PATH} ({len(actual)} rows)")

    if not GOLDEN_PATH.exists():
        pytest.fail(
            f"golden fixture missing at {GOLDEN_PATH}; "
            "run with REGEN_GOLDEN=1 to create it"
        )

    expected = pd.read_parquet(GOLDEN_PATH)
    pd.testing.assert_frame_equal(
        _normalise(actual),
        _normalise(expected),
        check_dtype=False,
        check_exact=False,
        rtol=1e-9,
        atol=1e-12,
    )
