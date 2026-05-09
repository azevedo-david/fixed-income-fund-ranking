"""Generate ranking.md from marts.rankings."""

from __future__ import annotations

import logging
from dataclasses import replace as _replace
from datetime import date

from ..compute.report import generate_report
from ..config import Settings
from ..storage import DuckDBWarehouse
from ._utils import load_rankings, universe_size

logger = logging.getLogger(__name__)


def write_report(
    db: DuckDBWarehouse,
    reference_date: date,
    settings: Settings,
) -> None:
    """Write ranking.md to settings.output.ranking_md."""
    settings = _replace(settings, reference_date=reference_date)
    rankings = load_rankings(db, reference_date, settings)
    n_funds = universe_size(db, reference_date)
    generate_report(rankings, n_funds, settings)
    logger.info("report: ranking.md written for %s (%d funds)", reference_date, n_funds)
