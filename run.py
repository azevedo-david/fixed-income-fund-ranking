"""End-to-end ranking pipeline.

Usage:
    python run.py [--force] [--reference-date YYYY-MM-DD] [--config PATH]

Steps:
    1. Load settings from config.yaml.
    2. Build the eligible fund universe.
    3. Compute metrics via build_metrics → metrics_df.
    4. Save metrics_df to output/metrics.parquet.
    5. Score, rank, and write output/ranking.md and output/ranking.json.

All runs are appended to output/pipeline.log.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
from datetime import date
from pathlib import Path

from src.config import LOGS_DIR, Settings
from src.compute.metrics import build_metrics
from src.compute.publish import build_payload, local_json_sink, publish
from src.compute.report import generate_report
from src.compute.universe import build_universe

# ── Logging: console + persistent log file ────────────────────────────────────
_fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=_fmt)

LOGS_DIR.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(LOGS_DIR / "pipeline.log", mode="a", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_fmt))
logging.getLogger().addHandler(_file_handler)

logger = logging.getLogger(__name__)


def main() -> None:
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
    logger.info("--- pipeline start  reference_date=%s  force=%s ---",
                settings.reference_date, force)

    # ── 1. Universe ────────────────────────────────────────────────────────────
    universe = build_universe(settings, force=force)

    # ── 2. Metrics ─────────────────────────────────────────────────────────────
    metrics_df = build_metrics(universe, settings, force=force)

    # ── 3. Save metrics parquet ────────────────────────────────────────────────
    metrics_path = settings.output.metrics_parquet
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_df.to_parquet(metrics_path, index=False)
    logger.info("metrics_df saved to %s (%d rows, %d cols)",
                metrics_path, len(metrics_df), metrics_df.shape[1])

    # ── 4. Report (markdown) ──────────────────────────────────────────────────
    generate_report(metrics_df, settings)

    # ── 5. Publish (structured JSON + any future sinks) ───────────────────────
    payload = build_payload(metrics_df, settings)
    publish(payload, sinks=[local_json_sink(settings.output.ranking_json)])

    # ── 6. Log top-5 per segment ──────────────────────────────────────────────
    for seg in payload["segments"]:
        label = f"{seg['purpose']} · {seg['profile']} · {seg['investor_type']}"
        for fund in seg["funds"]:
            logger.debug("  [%s] #%d %s (%s)  ret_ann=%.2f%%",
                        label, fund["rank"], fund["fund_name"],
                        fund["cnpj"], (fund["return_annualized_net"] or 0) * 100)

    logger.info("--- pipeline complete  outputs → %s ---", settings.output.ranking_md.parent)


if __name__ == "__main__":
    main()
