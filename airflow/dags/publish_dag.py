"""Airflow DAG: generate output files from marts.* tables.

Triggered automatically by fund_ranking_marts on successful completion.
Can also be triggered manually with a specific reference_date.

Produces:
  output/ranking.md       — human-readable markdown report
  output/ranking.json     — machine-readable structured payload
  output/metrics.parquet  — full metrics table for downstream analysis

Requires Airflow Variable: duckdb_path — absolute path to the .duckdb file.
Optional Airflow Variable: config_yaml_path — absolute path to config.yaml; falls back to repo default.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param


def _db_path() -> str:
    return Variable.get("duckdb_path")


def _settings():
    from src.config import Settings

    path = Variable.get("config_yaml_path", default_var=None)
    return Settings.from_yaml(Path(path)) if path else Settings.from_yaml()


_DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="fund_ranking_publish",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "reference_date": Param(None, type=["null", "string"], format="date"),
    },
    tags=["fund-ranking", "publish"],
)
def fund_ranking_publish() -> None:
    """Write ranking.md, ranking.json, and metrics.parquet from marts tables."""

    @task()
    def report(**context) -> None:
        from datetime import date

        from src.publish.report import write_report
        from src.storage import DuckDBWarehouse

        raw_date = context["params"]["reference_date"]
        settings = _settings()
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            write_report(db, reference_date, settings)

    @task()
    def publish(**context) -> None:
        from datetime import date

        from src.publish.publish import write_json, write_parquet
        from src.storage import DuckDBWarehouse

        raw_date = context["params"]["reference_date"]
        settings = _settings()
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            write_json(db, reference_date, settings)
            write_parquet(db, reference_date, settings)

    report()
    publish()


fund_ranking_publish()
