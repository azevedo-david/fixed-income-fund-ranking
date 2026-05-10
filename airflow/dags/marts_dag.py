"""Airflow DAG: compute marts.* tables from staging.* tables.

Triggered automatically by fund_ranking_stage on successful completion.
Can also be triggered manually with a specific reference_date.

Requires Airflow Variable: duckdb_path — absolute path to the .duckdb file.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _db_path() -> str:
    return Variable.get("duckdb_path")


_DEFAULT_ARGS = {}


@dag(
    dag_id="fund_ranking_marts",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "reference_date": Param(None, type=["null", "string"], format="date"),
        "force": Param(False, type="boolean"),
    },
    tags=["fund-ranking", "marts"],
)
def fund_ranking_marts() -> None:
    """Compute universe, metrics, and rankings for a given reference_date."""

    @task()
    def universe(**context) -> None:
        from datetime import date

        from src.config import Settings
        from src.marts.mart import mart_universe
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        raw_date = context["params"]["reference_date"]
        settings = Settings.from_yaml()
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            mart_universe(db, reference_date, settings, force=force)

    @task()
    def metrics(**context) -> None:
        from datetime import date

        from src.config import Settings
        from src.marts.mart import mart_metrics
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        raw_date = context["params"]["reference_date"]
        settings = Settings.from_yaml()
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            mart_metrics(db, reference_date, settings, force=force)

    @task()
    def rankings(**context) -> None:
        from datetime import date

        from src.config import Settings
        from src.marts.mart import mart_rankings
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        raw_date = context["params"]["reference_date"]
        settings = Settings.from_yaml()
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            mart_rankings(db, reference_date, settings, force=force)

    @task()
    def validate_marts(**context) -> None:
        from datetime import date

        from src.storage import DuckDBWarehouse
        from src.validation import validate_marts as _validate_marts

        raw_date = context["params"]["reference_date"]
        reference_date = date.fromisoformat(raw_date) if raw_date else date.today()

        with DuckDBWarehouse(_db_path()) as db:
            _validate_marts(db, reference_date)

    trigger_publish = TriggerDagRunOperator(
        task_id="trigger_publish",
        trigger_dag_id="fund_ranking_publish",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    universe() >> metrics() >> rankings() >> validate_marts() >> trigger_publish


fund_ranking_marts()
