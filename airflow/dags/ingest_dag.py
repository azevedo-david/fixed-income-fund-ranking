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


def _reference_date(logical_date: datetime) -> "date":
    return logical_date.date()


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
    params={"force": Param(False, type="boolean")},
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
        from src.ingestion.ingest import ingest_inf_diario
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        ref = _reference_date(context["logical_date"])
        with DuckDBWarehouse(_db_path()) as db:
            ingest_inf_diario(db, reference_date=ref, force=force)

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
        ref = _reference_date(context["logical_date"])
        with DuckDBWarehouse(_db_path()) as db:
            ingest_extrato(db, reference_date=ref, force=force)

    @task()
    def cdi(**context) -> None:
        from src.ingestion.ingest import ingest_cdi
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        ref = _reference_date(context["logical_date"])
        with DuckDBWarehouse(_db_path()) as db:
            ingest_cdi(db, reference_date=ref, force=force)

    @task()
    def anbima(**context) -> None:
        from src.ingestion.ingest import ingest_anbima
        from src.storage import DuckDBWarehouse

        force = context["params"]["force"]
        with DuckDBWarehouse(_db_path()) as db:
            ingest_anbima(db, force=force)

    (registro() >> inf_diario() >> cad_fi_hist() >> extrato() >> cdi() >> anbima())


fund_ranking_ingest()
