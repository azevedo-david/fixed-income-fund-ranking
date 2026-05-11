"""Tests for DuckDBWarehouse write patterns and incremental guards."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.storage import DuckDBWarehouse


@pytest.fixture
def db(tmp_path):
    with DuckDBWarehouse(str(tmp_path / "test.db")) as warehouse:
        yield warehouse


def test_schemas_created_on_init(db):
    schemas = {
        r[0]
        for r in db.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
    }
    assert {"raw", "staging", "marts", "logs"}.issubset(schemas)


def test_raw_tables_created_on_init(db):
    tables = {
        r[0]
        for r in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
    }
    assert {"inf_diario", "cdi_daily", "registro_classe", "extrato_fi"}.issubset(tables)


def test_upsert_timeseries_deduplicates(db):
    df1 = pd.DataFrame(
        {
            "CNPJ_FUNDO_CLASSE": ["11.111.111/0001-11"],
            "ID_SUBCLASSE": [None],
            "DT_COMPTC": [date(2024, 1, 1)],
            "VL_QUOTA": [1.0],
        }
    )
    df2 = df1.copy()
    df2["VL_QUOTA"] = 2.0

    db.upsert_timeseries(
        "raw",
        "inf_diario",
        df1,
        natural_key=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"],
    )
    db.upsert_timeseries(
        "raw",
        "inf_diario",
        df2,
        natural_key=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"],
    )

    rows = db.execute("SELECT VL_QUOTA FROM raw.inf_diario").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2.0


def test_get_max_date_returns_none_for_empty_table(db):
    assert db.get_max_date("raw", "inf_diario", "DT_COMPTC") is None


def test_get_max_date_returns_max(db):
    df = pd.DataFrame(
        {
            "CNPJ_FUNDO_CLASSE": ["11.111.111/0001-11", "11.111.111/0001-11"],
            "ID_SUBCLASSE": [None, None],
            "DT_COMPTC": [date(2024, 1, 1), date(2024, 3, 1)],
        }
    )
    db.upsert_timeseries(
        "raw",
        "inf_diario",
        df,
        natural_key=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"],
    )
    assert db.get_max_date("raw", "inf_diario", "DT_COMPTC") == date(2024, 3, 1)


def test_append_snapshot_keeps_both_versions(db):
    df = pd.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "rate": [0.0004],
        }
    )
    db.append_snapshot("raw", "cdi_daily", df, reference_date=date(2024, 1, 1))
    db.append_snapshot("raw", "cdi_daily", df, reference_date=date(2024, 1, 2))

    count = db.execute("SELECT COUNT(*) FROM raw.cdi_daily").fetchone()[0]
    assert count == 2


def test_upsert_derived_is_idempotent(db):
    ref = date(2024, 1, 31)
    df = pd.DataFrame(
        {
            "CNPJ_FUNDO_CLASSE": ["11.111.111/0001-11"],
            "ID_SUBCLASSE": [None],
            "fund_name": ["Test Fund"],
            "reference_date": [ref],
        }
    )
    db.upsert_derived("marts", "universe", df, reference_date=ref)
    db.upsert_derived("marts", "universe", df, reference_date=ref)

    count = db.execute(
        "SELECT COUNT(*) FROM marts.universe WHERE reference_date = ?", [ref]
    ).fetchone()[0]
    assert count == 1


def test_upsert_derived_preserves_other_dates(db):
    ref1, ref2 = date(2024, 1, 31), date(2024, 2, 29)
    df1 = pd.DataFrame(
        {
            "CNPJ_FUNDO_CLASSE": ["11.111.111/0001-11"],
            "ID_SUBCLASSE": [None],
            "fund_name": ["Fund A"],
            "reference_date": [ref1],
        }
    )
    df2 = df1.copy()
    df2["reference_date"] = ref2

    db.upsert_derived("marts", "universe", df1, reference_date=ref1)
    db.upsert_derived("marts", "universe", df2, reference_date=ref2)

    count = db.execute("SELECT COUNT(*) FROM marts.universe").fetchone()[0]
    assert count == 2
