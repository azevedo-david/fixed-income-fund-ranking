"""Orchestration: run universe → metrics → rankings for a given reference_date."""

from __future__ import annotations

import logging
from datetime import date

from ..config import Settings
from ..storage import DuckDBWarehouse
from .metrics import build_metrics
from .ranking import build_rankings
from .universe import build_universe

logger = logging.getLogger(__name__)


def mart_universe(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
    force: bool = False,
) -> int:
    if not force:
        existing = db.execute(
            "SELECT COUNT(*) FROM marts.universe WHERE reference_date = ?",
            [reference_date],
        ).fetchone()[0]
        if existing > 0:
            logger.info("mart_universe: %s already done, skipping", reference_date)
            return existing

    df = build_universe(db, reference_date, settings)
    rows = db.upsert_derived("marts", "universe", df, reference_date=reference_date)
    logger.info("mart_universe: %d rows for %s", rows, reference_date)
    return rows


def mart_metrics(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
    force: bool = False,
) -> int:
    if not force:
        existing = db.execute(
            "SELECT COUNT(*) FROM marts.metrics WHERE reference_date = ?",
            [reference_date],
        ).fetchone()[0]
        if existing > 0:
            logger.info("mart_metrics: %s already done, skipping", reference_date)
            return existing

    universe_df = db.execute(
        "SELECT * FROM marts.universe WHERE reference_date = ?", [reference_date]
    ).df()
    if universe_df.empty:
        logger.warning("mart_metrics: no universe for %s", reference_date)
        return 0

    df = build_metrics(db, universe_df, reference_date, settings)
    rows = db.upsert_derived("marts", "metrics", df, reference_date=reference_date)
    logger.info("mart_metrics: %d rows for %s", rows, reference_date)
    return rows


def mart_rankings(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
    force: bool = False,
) -> int:
    if not force:
        existing = db.execute(
            "SELECT COUNT(*) FROM marts.rankings WHERE reference_date = ?",
            [reference_date],
        ).fetchone()[0]
        if existing > 0:
            logger.info("mart_rankings: %s already done, skipping", reference_date)
            return existing

    metrics_df = db.execute(
        "SELECT * FROM marts.metrics WHERE reference_date = ?", [reference_date]
    ).df()
    if metrics_df.empty:
        logger.warning("mart_rankings: no metrics for %s", reference_date)
        return 0

    df = build_rankings(metrics_df, settings)
    if df.empty:
        logger.warning("mart_rankings: no rankings produced for %s", reference_date)
        return 0

    df["reference_date"] = reference_date
    rows = db.upsert_derived("marts", "rankings", df, reference_date=reference_date)
    logger.info("mart_rankings: %d rows for %s", rows, reference_date)
    return rows


def mart_all(
    db: DuckDBWarehouse,
    reference_date: date | None = None,
    settings: Settings | None = None,
    force: bool = False,
) -> None:
    """Run the full marts pipeline for a given reference_date."""
    if settings is None:
        settings = Settings.from_yaml()
    if reference_date is None:
        reference_date = settings.reference_date

    mart_universe(db, reference_date, settings, force=force)
    mart_metrics(db, reference_date, settings, force=force)
    mart_rankings(db, reference_date, settings, force=force)
