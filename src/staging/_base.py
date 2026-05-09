"""Abstract base class for all staging modules."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd

from ._utils import Check, log_checks
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)


class BaseStager(ABC):
    dataset: str
    task_stage: str
    task_validate: str

    @abstractmethod
    def _fetch_raw(
        self, db: DuckDBWarehouse, reference_date: date
    ) -> pd.DataFrame | None:
        """Fetch and clean raw data into a staging-ready DataFrame, or None if unavailable."""
        ...

    @abstractmethod
    def _build_checks(self, df: pd.DataFrame) -> list[Check]:
        """Return data quality checks to run against the staged slice."""
        ...

    def stage(self, db: DuckDBWarehouse, reference_date: date) -> int:
        """Fetch raw, attach reference_date, and upsert into the staging table."""
        df = self._fetch_raw(db, reference_date)
        if df is None or df.empty:
            logger.warning(
                "%s: no data available for %s", self.task_stage, reference_date
            )
            return 0
        df = df.copy()
        df["reference_date"] = reference_date
        schema, table = self.dataset.split(".")
        rows = db.upsert_derived(schema, table, df, reference_date=reference_date)
        logger.info("%s: %d rows written", self.task_stage, rows)
        return rows

    def validate(self, db: DuckDBWarehouse, reference_date: date) -> list[Check]:
        """Read staged data, run checks, write to logs.validation_log, raise on errors."""
        df = db.execute(
            f"SELECT * FROM {self.dataset} WHERE reference_date = ?", [reference_date]
        ).df()
        checks = self._build_checks(df)
        log_checks(db, checks, self.dataset, self.task_validate, reference_date)
        failed = [c for c in checks if not c.passed and c.severity == "error"]
        if failed:
            names = ", ".join(c.name for c in failed)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: {names}"
            )
        return checks
