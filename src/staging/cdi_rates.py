"""Staging transform and validation for raw.cdi_daily → staging.cdi_rates."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from ._base import BaseStager
from ._utils import Check, log_checks
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_NATURAL_KEY = ["date"]


class CDIRatesStager(BaseStager):
    dataset = "staging.cdi_rates"
    task_stage = "stage_cdi_rates"
    task_validate = "validate_cdi_rates"

    def stage(self, db: DuckDBWarehouse, reference_date: date) -> int:
        df = self._fetch_raw(db, reference_date)
        if df is None or df.empty:
            logger.info("%s: no new data up to %s", self.task_stage, reference_date)
            return 0
        rows = db.upsert_timeseries("staging", "cdi_rates", df, _NATURAL_KEY)
        logger.info("%s: %d rows written", self.task_stage, rows)
        return rows

    def validate(self, db: DuckDBWarehouse, reference_date: date) -> list[Check]:
        df = db.execute("SELECT * FROM staging.cdi_rates").df()
        checks = self._build_checks(df, reference_date)
        log_checks(db, checks, self.dataset, self.task_validate, reference_date)
        failed = [c for c in checks if not c.passed and c.severity == "error"]
        if failed:
            names = ", ".join(c.name for c in failed)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: {names}"
            )
        return checks

    def _fetch_raw(
        self, db: DuckDBWarehouse, reference_date: date
    ) -> pd.DataFrame | None:
        last_date = db.get_max_date("staging", "cdi_rates", "date")
        if last_date is not None:
            raw = db.execute(
                "SELECT date, rate FROM raw.cdi_daily WHERE date > ?",
                [last_date],
            ).df()
        else:
            raw = db.execute("SELECT date, rate FROM raw.cdi_daily").df()
        return raw if not raw.empty else None

    def _build_checks(self, df: pd.DataFrame, reference_date: date) -> list[Check]:
        return [
            self._check_row_count(df),
            self._check_no_null_rate(df),
            self._check_date_coverage(df, reference_date),
        ]

    def _check_row_count(self, df: pd.DataFrame) -> Check:
        n = len(df)
        passed = n > 0
        return Check(
            name="row_count_positive",
            passed=passed,
            severity="error",
            value=str(n),
            threshold="> 0",
            message=None if passed else "staging.cdi_rates is empty",
        )

    def _check_no_null_rate(self, df: pd.DataFrame) -> Check:
        n_null = int(df["rate"].isna().sum())
        passed = n_null == 0
        return Check(
            name="no_null_rate",
            passed=passed,
            severity="error",
            value=str(n_null),
            threshold="0",
            message=None if passed else f"{n_null} rows have null rate",
        )

    def _check_date_coverage(self, df: pd.DataFrame, reference_date: date) -> Check:
        max_date = df["date"].max() if not df.empty else None
        if max_date is None:
            return Check(
                name="date_coverage",
                passed=False,
                severity="warning",
                value=None,
                threshold="within 7 days of reference_date",
                message="could not determine date coverage",
            )
        gap = (pd.Timestamp(reference_date) - pd.Timestamp(max_date)).days
        passed = gap <= 7
        return Check(
            name="date_coverage",
            passed=passed,
            severity="warning",
            value=str(max_date),
            threshold=f"<= 7 days before {reference_date}",
            message=(
                None
                if passed
                else f"latest CDI rate is {max_date}, {gap} days before reference_date"
            ),
        )


def stage_cdi_rates(db: DuckDBWarehouse, reference_date: date) -> int:
    return CDIRatesStager().stage(db, reference_date)


def validate_cdi_rates(db: DuckDBWarehouse, reference_date: date) -> list[Check]:
    return CDIRatesStager().validate(db, reference_date)
