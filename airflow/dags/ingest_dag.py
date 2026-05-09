"""Airflow DAG: ingest raw CVM/ANBIMA/BCB data into DuckDB raw.* tables.

Runs weekly (Monday 6am). Tasks are chained sequentially because DuckDB
allows only one write connection per file at a time.

Requires Airflow Variable: duckdb_path — absolute path to the .duckdb file.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param


def _db_path() -> str:
    return Variable.get("duckdb_path")


_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="fund_ranking_ingest",
    schedule="0 6 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "force": Param(False, type="boolean"),
        "history_start": Param("2021-01-01", type="string", format="date"),
    },
    tags=["fund-ranking", "ingestion"],
)
def fund_ranking_ingest() -> None:
    """Download all raw sources and write to raw.* DuckDB tables."""

    @task()
    def registro(**context) -> None:
        from src.ingestion.ingest import ingest_registro
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            ingest_registro(db, force=force)

    @task()
    def inf_diario(**context) -> None:
        from datetime import date

        from src.ingestion.ingest import ingest_inf_diario
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        history_start = date.fromisoformat(context["params"]["history_start"])
        with DuckDBWarehouse(_db_path()) as db:
            ingest_inf_diario(db, force=force, history_start=history_start)

    @task()
    def cad_fi_hist(**context) -> None:
        from src.ingestion.ingest import ingest_cad_fi_hist
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            ingest_cad_fi_hist(db, force=force)

    @task()
    def extrato(**context) -> None:
        from src.ingestion.ingest import ingest_extrato
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            ingest_extrato(db, force=force)

    @task()
    def cdi(**context) -> None:
        from datetime import date

        from src.ingestion.ingest import ingest_cdi
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        history_start = date.fromisoformat(context["params"]["history_start"])
        with DuckDBWarehouse(_db_path()) as db:
            ingest_cdi(db, force=force, history_start=history_start)

    @task()
    def anbima(**context) -> None:
        from src.ingestion.ingest import ingest_anbima
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            ingest_anbima(db, force=force)

    @task()
    def cleanup() -> None:
        from src.ingestion.ingest import ingest_cleanup

        ingest_cleanup()

    (
        registro()
        >> inf_diario()
        >> cad_fi_hist()
        >> extrato()
        >> cdi()
        >> anbima()
        >> cleanup()
    )


fund_ranking_ingest()
