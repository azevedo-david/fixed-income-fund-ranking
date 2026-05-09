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
    def _build_checks(self, df: pd.DataFrame, reference_date: date) -> list[Check]:
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
        if "reference_date" not in df.columns:
            df["reference_date"] = reference_date
        acquired_date = df["reference_date"].iloc[0]
        schema, table = self.dataset.split(".")
        rows = db.upsert_derived(schema, table, df, reference_date=acquired_date)
        logger.info("%s: %d rows written", self.task_stage, rows)
        return rows

    def validate(self, db: DuckDBWarehouse, reference_date: date) -> list[Check]:
        """Read the latest staged snapshot on or before reference_date, run checks, raise on errors.

        Timeseries stagers (no reference_date column) must override this method.
        """
        acquired_date = db.execute(
            f"SELECT MAX(reference_date) FROM {self.dataset}"
        ).fetchone()[0]
        if acquired_date is None:
            checks = [
                Check(
                    name="data_available",
                    passed=False,
                    severity="error",
                    value=None,
                    threshold="any snapshot",
                    message=f"no snapshot found in {self.dataset}",
                )
            ]
            log_checks(db, checks, self.dataset, self.task_validate, reference_date)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: data_available"
            )
        df = db.execute(
            f"SELECT * FROM {self.dataset} WHERE reference_date = ?", [acquired_date]
        ).df()
        checks = self._build_checks(df, reference_date)
        log_checks(db, checks, self.dataset, self.task_validate, reference_date)
        failed = [c for c in checks if not c.passed and c.severity == "error"]
        if failed:
            names = ", ".join(c.name for c in failed)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: {names}"
            )
        return checks

    def _check_freshness(self, df: pd.DataFrame, reference_date: date) -> Check:
        """Warn if the snapshot acquisition date is more than 7 days before reference_date."""
        acquired = df["reference_date"].dropna().max()
        if pd.isna(acquired):
            return Check(
                name="freshness",
                passed=False,
                severity="warning",
                value=None,
                threshold=f"<= 7 days before {reference_date}",
                message="cannot determine snapshot acquisition date",
            )
        gap = (pd.Timestamp(reference_date) - pd.Timestamp(acquired)).days
        passed = gap <= 7
        return Check(
            name="freshness",
            passed=passed,
            severity="warning",
            value=str(acquired.date() if hasattr(acquired, "date") else acquired),
            threshold=f"<= 7 days before {reference_date}",
            message=(
                None if passed else f"snapshot is {gap} days old (acquired: {acquired})"
            ),
        )
