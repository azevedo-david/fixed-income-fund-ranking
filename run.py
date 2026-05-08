"""End-to-end ranking pipeline entry point."""

from __future__ import annotations

import argparse
import dataclasses
import logging
from datetime import date
from pathlib import Path

from src.config import LOGS_DIR, Settings
from src.compute.metrics import build_metrics
from src.compute.publish import build_payload, local_json_sink, publish
from src.compute.ranking import rank_all
from src.compute.report import generate_report
from src.compute.universe import build_universe

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

    universe = build_universe(settings, force=force)
    metrics_df = build_metrics(universe, settings, force=force)

    metrics_path = settings.output.metrics_parquet
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_df.to_parquet(metrics_path, index=False)
    logger.info(
        "metrics_df saved to %s (%d rows, %d cols)",
        metrics_path,
        len(metrics_df),
        metrics_df.shape[1],
    )

    rankings = rank_all(metrics_df, settings)
    generate_report(rankings, len(metrics_df), settings)

    payload = build_payload(rankings, len(metrics_df), settings)
    publish(payload, sinks=[local_json_sink(settings.output.ranking_json)])

    for seg in payload["segments"]:
        label = f"{seg['purpose']} · {seg['profile']} · {seg['investor_type']}"
        for fund in seg["funds"]:
            logger.debug(
                "  [%s] #%d %s (%s)  ret_ann=%.2f%%",
                label,
                fund["rank"],
                fund["fund_name"],
                fund["cnpj"],
                (fund["return_annualized_net"] or 0) * 100,
            )

    logger.info(
        "--- pipeline complete  outputs → %s ---", settings.output.ranking_md.parent
    )


if __name__ == "__main__":
    main()
