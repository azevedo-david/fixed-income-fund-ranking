"""End-to-end ranking pipeline entry point."""

from __future__ import annotations

import argparse
import dataclasses
import logging
from datetime import date
from pathlib import Path

from src.config import LOGS_DIR, Settings

logger = logging.getLogger(__name__)

_FMT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=_FMT)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOGS_DIR / "pipeline.log", mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter(_FMT))
    logging.getLogger().addHandler(handler)


def main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(description="Fixed Income Fund Ranking pipeline.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all upstream data, bypassing caches.",
    )
    parser.add_argument(
        "--reference-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Override the reference date (default: today, or config.yaml value).",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help="Path to config.yaml (default: project config.yaml).",
    )
    args = parser.parse_args()

    cfg_path = Path(args.config) if args.config else None
    settings = Settings.from_yaml(cfg_path) if cfg_path else Settings.from_yaml()

    if args.reference_date:
        settings = dataclasses.replace(
            settings, reference_date=date.fromisoformat(args.reference_date)
        )

    force = args.force or settings.force_download
    logger.info(
        "--- pipeline start  reference_date=%s  force=%s ---",
        settings.reference_date,
        force,
    )

    from src.storage import DuckDBWarehouse
    from src.marts.mart import mart_all
    from src.publish.report import write_report
    from src.publish.publish import write_json, write_parquet

    with DuckDBWarehouse(str(settings.db_path)) as db:
        mart_all(db, settings.reference_date, settings, force=force)
        write_report(db, settings.reference_date, settings)
        write_json(db, settings.reference_date, settings)
        write_parquet(db, settings.reference_date, settings)

    logger.info(
        "--- pipeline complete  outputs → %s ---", settings.output.ranking_md.parent
    )


if __name__ == "__main__":
    main()
