"""Airflow DAG: transform raw.* tables into staging.* tables.

Triggered automatically by fund_ranking_ingest on successful completion.
Can also be triggered manually (e.g. with force=True to rebuild from scratch).
Tasks are chained sequentially because DuckDB allows only one write
connection per file at a time.

Requires Airflow Variable: duckdb_path — absolute path to the .duckdb file.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _db_path() -> str:
    return Variable.get("duckdb_path")


_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="fund_ranking_stage",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "force": Param(False, type="boolean"),
    },
    tags=["fund-ranking", "staging"],
)
def fund_ranking_stage() -> None:
    """Transform all raw sources into clean staging.* tables."""

    @task()
    def registry(**context) -> None:
        from src.staging.stage import stage_registry
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            stage_registry(db, force=force)

    @task()
    def fees(**context) -> None:
        from src.staging.stage import stage_fees
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            stage_fees(db, force=force)

    @task()
    def daily_quotes(**context) -> None:
        from src.staging.stage import stage_daily_quotes
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            stage_daily_quotes(db, force=force)

    @task()
    def cdi_rates(**context) -> None:
        from src.staging.stage import stage_cdi_rates
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            stage_cdi_rates(db, force=force)

    @task()
    def anbima(**context) -> None:
        from src.staging.stage import stage_anbima
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            stage_anbima(db, force=force)

    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_marts",
        trigger_dag_id="fund_ranking_marts",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    (registry() >> fees() >> daily_quotes() >> cdi_rates() >> anbima() >> trigger_marts)


fund_ranking_stage()
