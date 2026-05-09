"""Staging transform and validation for raw.inf_diario → staging.daily_quotes."""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from ._base import BaseStager
from ._utils import Check, log_checks
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_NATURAL_KEY = ["fund_cnpj", "subclass_id", "date"]
_COLS = [
    "fund_cnpj",
    "subclass_id",
    "date",
    "nav",
    "aum",
    "inflows",
    "outflows",
    "shareholders",
]


class DailyQuotesStager(BaseStager):
    dataset = "staging.daily_quotes"
    task_stage = "stage_daily_quotes"
    task_validate = "validate_daily_quotes"

    def stage(self, db: DuckDBWarehouse, reference_date: date) -> int:
        """Incremental upsert re-processing from the start of the last staged month.

        Re-processing the last month (instead of only fetching net-new dates) mirrors
        ingest_inf_diario's strategy: CVM sometimes publishes corrections to past months,
        and upsert_timeseries makes the overwrite idempotent.
        """
        df = self._fetch_raw(db, reference_date)
        if df is None or df.empty:
            logger.info("%s: no new data up to %s", self.task_stage, reference_date)
            return 0
        rows = db.upsert_timeseries("staging", "daily_quotes", df, _NATURAL_KEY)
        logger.info("%s: %d rows written", self.task_stage, rows)
        return rows

    def validate(self, db: DuckDBWarehouse, reference_date: date) -> list[Check]:
        """Validate the full staged timeseries; reference_date is used only for freshness checks."""
        df = db.execute("SELECT * FROM staging.daily_quotes").df()
        checks = self._build_checks(df, reference_date)
        log_checks(db, checks, self.dataset, self.task_validate, reference_date)
        failed = [c for c in checks if not c.passed and c.severity == "error"]
        if failed:
            names = ", ".join(c.name for c in failed)
            raise ValueError(
                f"{self.task_validate}: error-level checks failed: {names}"
            )
        return checks

    # --- transforms ---

    def _fetch_raw(
        self, db: DuckDBWarehouse, reference_date: date
    ) -> pd.DataFrame | None:
        last_date = db.get_max_date("staging", "daily_quotes", "date")

        if last_date is not None:
            # Start of the last staged month so corrections published by CVM for
            # that month are picked up (upsert_timeseries makes the re-write idempotent).
            from_date = last_date.replace(day=1)
            raw = db.execute(
                "SELECT * FROM raw.inf_diario WHERE DT_COMPTC >= ?",
                [from_date],
            ).df()
            # Seed the ffill with the last known NAV per fund from before the
            # re-processing window so bad zeros at the window boundary are filled.
            seeds = db.execute(
                """
                SELECT fund_cnpj, subclass_id, date, nav
                FROM staging.daily_quotes
                WHERE date < ?
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY fund_cnpj, subclass_id ORDER BY date DESC
                ) = 1
                """,
                [from_date],
            ).df()
        else:
            raw = db.execute("SELECT * FROM raw.inf_diario").df()
            seeds = None

        if raw.empty:
            return None

        return self._clean(
            raw, seeds if seeds is not None and not seeds.empty else None
        )

    def _clean(
        self, df: pd.DataFrame, seeds: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        out = df.copy()

        row_key = ["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"]
        n_before = len(out)
        out = (
            out.sort_values(row_key + ["TP_FUNDO_CLASSE"], na_position="first")
            .drop_duplicates(subset=row_key, keep="first")
            .sort_values(row_key)
            .reset_index(drop=True)
        )
        n_dups = n_before - len(out)
        if n_dups:
            logger.debug("%s: removed %d duplicate rows", self.task_stage, n_dups)

        # rows where both quota and shareholders are zero are dead/ghost entries
        if "NR_COTST" in out.columns:
            mask_dead = (out["VL_QUOTA"] == 0) & (out["NR_COTST"] == 0)
            n_dead = int(mask_dead.sum())
            if n_dead:
                logger.debug("%s: dropped %d ghost rows", self.task_stage, n_dead)
            out = out[~mask_dead].reset_index(drop=True)

        # quota=0 on an active fund is a CVM reporting error — mask before ffill
        mask_zero = out["VL_QUOTA"] == 0
        if "NR_COTST" in out.columns and "VL_PATRIM_LIQ" in out.columns:
            active = (out["NR_COTST"].fillna(0) > 0) | (
                out["VL_PATRIM_LIQ"].fillna(0) > 0
            )
            mask_bad_zero = mask_zero & active
        else:
            mask_bad_zero = mask_zero

        n_bad = int(mask_bad_zero.sum())
        if n_bad:
            logger.debug(
                "%s: masked %d zero-nav rows (active fund)", self.task_stage, n_bad
            )
            out.loc[mask_bad_zero, "VL_QUOTA"] = np.nan

        out = out.rename(
            columns={
                "CNPJ_FUNDO_CLASSE": "fund_cnpj",
                "ID_SUBCLASSE": "subclass_id",
                "DT_COMPTC": "date",
                "VL_QUOTA": "nav",
                "VL_PATRIM_LIQ": "aum",
                "CAPTC_DIA": "inflows",
                "RESG_DIA": "outflows",
                "NR_COTST": "shareholders",
            }
        )[_COLS]

        if seeds is not None and not seeds.empty:
            # Prepend seed rows (last known NAV per fund before the batch window)
            # so the ffill can cross the incremental boundary.
            seed_rows = seeds[["fund_cnpj", "subclass_id", "date", "nav"]].reindex(
                columns=_COLS
            )
            seed_rows["_seed"] = True
            out["_seed"] = False
            combined = pd.concat([seed_rows, out], ignore_index=True).sort_values(
                ["fund_cnpj", "subclass_id", "date"]
            )
            combined["nav"] = combined.groupby(
                ["fund_cnpj", "subclass_id"], dropna=False
            )["nav"].ffill()
            return (
                combined[~combined["_seed"]]
                .drop(columns=["_seed"])
                .reset_index(drop=True)
            )

        out["nav"] = out.groupby(["fund_cnpj", "subclass_id"], dropna=False)[
            "nav"
        ].ffill()
        return out

    # --- checks ---

    def _build_checks(self, df: pd.DataFrame, reference_date: date) -> list[Check]:
        return [
            self._check_row_count(df),
            self._check_no_null_cnpj(df),
            self._check_date_coverage(df, reference_date),
            self._check_nav_coverage(df),
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
            message=None if passed else "staging.daily_quotes is empty",
        )

    def _check_no_null_cnpj(self, df: pd.DataFrame) -> Check:
        n_null = int(df["fund_cnpj"].isna().sum())
        passed = n_null == 0
        return Check(
            name="no_null_fund_cnpj",
            passed=passed,
            severity="error",
            value=str(n_null),
            threshold="0",
            message=None if passed else f"{n_null} rows have null fund_cnpj",
        )

    def _check_date_coverage(
        self, df: pd.DataFrame, reference_date: date | None
    ) -> Check:
        max_date = df["date"].max() if not df.empty else None
        if reference_date is None or max_date is None:
            return Check(
                name="date_coverage",
                passed=False,
                severity="warning",
                value=str(max_date),
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
                else f"latest quote is {max_date}, {gap} days before reference_date"
            ),
        )

    def _check_nav_coverage(self, df: pd.DataFrame) -> Check:
        pct_null = df["nav"].isna().mean() * 100
        passed = pct_null < 5.0
        return Check(
            name="nav_coverage",
            passed=passed,
            severity="warning",
            value=f"{pct_null:.1f}%",
            threshold="< 5%",
            message=(
                None
                if passed
                else f"{pct_null:.1f}% of rows missing nav after forward-fill"
            ),
        )


def stage_daily_quotes(db: DuckDBWarehouse, reference_date: date) -> int:
    return DailyQuotesStager().stage(db, reference_date)


def validate_daily_quotes(db: DuckDBWarehouse, reference_date: date) -> list[Check]:
    return DailyQuotesStager().validate(db, reference_date)
