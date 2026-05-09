"""DuckDB client — module that imports duckdb."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from typing import Generator

import duckdb
import pandas as pd

from .schemas import ALL_DDLS

logger = logging.getLogger(__name__)

_SCHEMAS = ("raw", "staging", "marts", "logs")


class DuckDBWarehouse:
    """DuckDB analytical database with three write patterns: upsert_timeseries, append_snapshot, upsert_derived."""

    def __init__(self, db_path: str) -> None:
        self._con = duckdb.connect(db_path)
        for schema in _SCHEMAS:
            self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for ddl in ALL_DDLS:
            self._con.execute(ddl)

    def __enter__(self) -> DuckDBWarehouse:
        return self

    def __exit__(self, *_: object) -> None:
        self._con.close()

    def upsert_timeseries(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        natural_key: list[str],
    ) -> int:
        """UPSERT on natural_key — one authoritative row per key, revisions overwrite silently."""
        if df.empty:
            return 0
        self._ensure_table(schema, table, df)
        cols = self._col_list(schema, table, df)
        join_pred = " AND ".join(
            f't."{k}" IS NOT DISTINCT FROM s."{k}"' for k in natural_key
        )
        self._con.register("_upsert_src", df)
        try:
            with self._transaction():
                self._con.execute(
                    f"DELETE FROM {schema}.{table} t "
                    f"USING _upsert_src s WHERE {join_pred}"
                )
                self._con.execute(
                    f"INSERT INTO {schema}.{table} ({cols}) SELECT {cols} FROM _upsert_src"
                )
        finally:
            self._con.unregister("_upsert_src")
        logger.debug("upsert_timeseries %s.%s: %d rows", schema, table, len(df))
        return len(df)

    def append_snapshot(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        reference_date: date,
    ) -> int:
        """Insert df into schema.table with a reference_date column added; never modifies existing rows."""
        if df.empty:
            return 0
        df = df.copy()
        df["reference_date"] = reference_date
        self._ensure_table(schema, table, df)
        cols = self._col_list(schema, table, df)
        self._con.register("_snapshot_src", df)
        try:
            self._con.execute(
                f"INSERT INTO {schema}.{table} ({cols}) SELECT {cols} FROM _snapshot_src"
            )
        finally:
            self._con.unregister("_snapshot_src")
        logger.debug("append_snapshot %s.%s: %d rows", schema, table, len(df))
        return len(df)

    def upsert_derived(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        reference_date: date,
    ) -> int:
        """Delete all rows for reference_date then insert df; fully idempotent."""
        if df.empty:
            return 0

        self._ensure_table_with_schema(schema, table, df)
        cols = self._col_list(schema, table, df)

        null_cols = df.isnull().sum()
        if (null_cols > 0).any():
            null_report = ", ".join(
                f"{col}={n}" for col, n in null_cols[null_cols > 0].items()
            )
            logger.debug(
                "upsert_derived %s.%s: NULL values in DataFrame: %s",
                schema,
                table,
                null_report,
            )

        self._con.register("_derived_src", df)
        try:
            with self._transaction():
                self._con.execute(
                    f"DELETE FROM {schema}.{table} WHERE reference_date = ?",
                    [reference_date],
                )
                self._con.execute(
                    f"INSERT INTO {schema}.{table} ({cols}) SELECT {cols} FROM _derived_src"
                )
        except Exception as e:
            logger.error(
                "upsert_derived failed for %s.%s: %s\nDataFrame shape: %s\nColumns: %s",
                schema,
                table,
                str(e),
                df.shape,
                list(df.columns),
            )
            raise
        finally:
            self._con.unregister("_derived_src")
        logger.debug("upsert_derived %s.%s: %d rows", schema, table, len(df))
        return len(df)

    def get_max_date(
        self,
        schema: str,
        table: str,
        date_col: str,
    ) -> date | None:
        """Return the maximum value of date_col in schema.table, or None if the table is empty or does not exist."""
        exists = self._con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        if not exists:
            return None
        row = self._con.execute(
            f"SELECT MAX({date_col}) FROM {schema}.{table}"
        ).fetchone()
        return row[0] if row and row[0] is not None else None

    def execute(self, sql: str, params: list | None = None) -> duckdb.DuckDBPyRelation:
        """Pass-through for arbitrary queries (use sparingly — prefer the typed methods above)."""
        return self._con.execute(sql, params or [])

    @contextmanager
    def _transaction(self) -> Generator[None, None, None]:
        self._con.begin()
        try:
            yield
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise

    def _col_list(self, schema: str, table: str, df: pd.DataFrame) -> str:
        """Quoted column list for INSERT: table columns present in df, in table definition order."""
        rows = self._con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        table_cols = [r[0] for r in rows]
        matched = [c for c in table_cols if c in df.columns]
        return ", ".join(f'"{c}"' for c in matched)

    def _ensure_table(self, schema: str, table: str, df: pd.DataFrame) -> None:
        """Create schema.table from df's column schema if it does not already exist."""
        self._con.register("_schema_src", df)
        self._con.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS "
            f"SELECT * FROM _schema_src WHERE 1=0"
        )
        self._con.unregister("_schema_src")

    def _ensure_table_with_schema(
        self, schema: str, table: str, df: pd.DataFrame
    ) -> None:
        """Create or validate schema.table matches df columns; drop and recreate if mismatch."""
        exists = self._con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()

        if exists:
            existing_cols = set(
                r[0]
                for r in self._con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchall()
            )
            expected_cols = set(df.columns)

            if existing_cols != expected_cols:
                logger.warning(
                    "%s.%s schema mismatch: existing=%s, expected=%s. Dropping and recreating.",
                    schema,
                    table,
                    sorted(existing_cols),
                    sorted(expected_cols),
                )
                self._con.execute(f"DROP TABLE {schema}.{table}")
                self._ensure_table(schema, table, df)
        else:
            self._ensure_table(schema, table, df)
