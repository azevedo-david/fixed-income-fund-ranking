"""Write ranking.json and metrics.parquet from marts tables."""

from __future__ import annotations

import logging
from dataclasses import replace as _replace
from datetime import date

from ..compute.publish import build_payload, local_json_sink, publish
from ..config import Settings
from ..storage import DuckDBWarehouse
from ._utils import load_rankings, universe_size

logger = logging.getLogger(__name__)


def write_json(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
) -> None:
    """Write ranking.json to settings.output.ranking_json."""
    settings = _replace(settings, reference_date=reference_date)
    rankings = load_rankings(db, reference_date, settings)
    n_funds = universe_size(db, reference_date)
    payload = build_payload(rankings, n_funds, settings)
    publish(payload, sinks=[local_json_sink(settings.output.ranking_json)])
    logger.info("publish: ranking.json written for %s", reference_date)


def write_parquet(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
) -> None:
    """Write metrics.parquet to settings.output.metrics_parquet."""
    metrics_df = db.execute(
        "SELECT * FROM marts.metrics WHERE reference_date = ?", [reference_date]
    ).df()
    path = settings.output.metrics_parquet
    path.parent.mkdir(parents=True, exist_ok=True)
    metrics_df.to_parquet(path, index=False)
    logger.info(
        "publish: metrics.parquet written for %s (%d rows)",
        reference_date,
        len(metrics_df),
    )
