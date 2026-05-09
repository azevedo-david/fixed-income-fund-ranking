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
    raw_dataset: str
    task_stage: str
    task_validate: str

    @abstractmethod
    def _fetch_raw(self, db: DuckDBWarehouse) -> pd.DataFrame | None:
        """Fetch and clean raw data into a staging-ready DataFrame, or None if unavailable."""
        ...

    @abstractmethod
    def _build_checks(self, df: pd.DataFrame) -> list[Check]:
        """Return data quality checks to run against the staged slice."""
        ...

    def stage(self, db: DuckDBWarehouse, force: bool = False) -> int:
        """Snapshot stage: skip when staging already holds the latest raw snapshot."""
        if not force and self._snapshot_up_to_date(db):
            logger.info(
                "%s: staging already at latest raw snapshot, skipping", self.task_stage
            )
            return 0
        df = self._fetch_raw(db)
        if df is None or df.empty:
            logger.warning("%s: no data available", self.task_stage)
            return 0
        acquired_date = df["reference_date"].iloc[0]
        schema, table = self.dataset.split(".")
        rows = db.upsert_derived(schema, table, df, reference_date=acquired_date)
        logger.info("%s: %d rows written", self.task_stage, rows)
        return rows

    def _snapshot_up_to_date(self, db: DuckDBWarehouse) -> bool:
        """True if staging already holds a snapshot >= the latest raw snapshot."""
        raw_max = db.execute(
            f"SELECT MAX(reference_date) FROM {self.raw_dataset}"
        ).fetchone()[0]
        if raw_max is None:
            return True
        staging_max = db.execute(
            f"SELECT MAX(reference_date) FROM {self.dataset}"
        ).fetchone()[0]
        return staging_max is not None and staging_max >= raw_max

    def validate(self, db: DuckDBWarehouse) -> list[Check]:
        """Read the latest staged snapshot, run checks, raise on errors.

        Timeseries stagers (no reference_date column) must override this method.
        """
        today = date.today()
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
            log_checks(db, checks, self.dataset, self.task_validate, today)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: data_available"
            )
        df = db.execute(
            f"SELECT * FROM {self.dataset} WHERE reference_date = ?", [acquired_date]
        ).df()
        checks = self._build_checks(df)
        log_checks(db, checks, self.dataset, self.task_validate, today)
        failed = [c for c in checks if not c.passed and c.severity == "error"]
        if failed:
            names = ", ".join(c.name for c in failed)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: {names}"
            )
        return checks

    def _check_freshness(self, df: pd.DataFrame) -> Check:
        """Warn if the snapshot acquisition date is more than 7 days before today."""
        today = date.today()
        acquired = df["reference_date"].dropna().max()
        if pd.isna(acquired):
            return Check(
                name="freshness",
                passed=False,
                severity="warning",
                value=None,
                threshold=f"<= 7 days before {today}",
                message="cannot determine snapshot acquisition date",
            )
        gap = (pd.Timestamp(today) - pd.Timestamp(acquired)).days
        passed = gap <= 7
        return Check(
            name="freshness",
            passed=passed,
            severity="warning",
            value=str(acquired.date() if hasattr(acquired, "date") else acquired),
            threshold=f"<= 7 days before {today}",
            message=(
                None if passed else f"snapshot is {gap} days old (acquired: {acquired})"
            ),
        )
