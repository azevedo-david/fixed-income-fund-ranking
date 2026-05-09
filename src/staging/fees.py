"""Staging transform and validation for CVM fee tables → staging.fees."""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from ._base import BaseStager
from ._utils import Check, fmt_cnpj
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)


class FeesStager(BaseStager):
    dataset = "staging.fees"
    task_stage = "stage_fees"
    task_validate = "validate_fees"

    def _fetch_raw(
        self, db: DuckDBWarehouse, reference_date: date
    ) -> pd.DataFrame | None:
        adm_snap = db.execute(
            "SELECT MAX(reference_date) FROM raw.cad_fi_hist_taxa_adm"
        ).fetchone()[0]

        if adm_snap is None:
            return None

        perf_snap = db.execute(
            "SELECT MAX(reference_date) FROM raw.cad_fi_hist_taxa_perfm"
        ).fetchone()[0]
        ext_snap = db.execute(
            "SELECT MAX(reference_date) FROM raw.extrato_fi"
        ).fetchone()[0]

        adm_raw = db.execute(
            "SELECT * FROM raw.cad_fi_hist_taxa_adm WHERE reference_date = ?",
            [adm_snap],
        ).df()
        perf_raw = (
            db.execute(
                "SELECT * FROM raw.cad_fi_hist_taxa_perfm WHERE reference_date = ?",
                [perf_snap],
            ).df()
            if perf_snap
            else pd.DataFrame()
        )
        ext_raw = (
            db.execute(
                "SELECT * FROM raw.extrato_fi WHERE reference_date = ?", [ext_snap]
            ).df()
            if ext_snap
            else pd.DataFrame()
        )

        df = self._clean(adm_raw, perf_raw, ext_raw)
        df["reference_date"] = adm_snap
        return df

    def _build_checks(self, df: pd.DataFrame, reference_date: date) -> list[Check]:
        return [
            self._check_row_count(df),
            self._check_no_null_cnpj(df),
            self._check_adm_fee_coverage(df),
            self._check_adm_fee_range(df),
            self._check_freshness(df, reference_date),
        ]

    def _clean(
        self, adm_raw: pd.DataFrame, perf_raw: pd.DataFrame, ext_raw: pd.DataFrame
    ) -> pd.DataFrame:
        hist = self._clean_cad_fi_hist(adm_raw, perf_raw)
        ext = self._clean_extrato(ext_raw) if not ext_raw.empty else pd.DataFrame()

        if ext.empty:
            df = hist
        else:
            df = hist.merge(
                ext.rename(
                    columns={
                        "adm_fee": "adm_fee_ext",
                        "has_perf_fee": "has_perf_fee_ext",
                    }
                ),
                how="left",
                on="fund_cnpj",
            )
            df["adm_fee"] = df["adm_fee"].fillna(df["adm_fee_ext"])
            df["has_perf_fee"] = df["has_perf_fee"].fillna(df["has_perf_fee_ext"])
            df = df.drop(columns=["adm_fee_ext", "has_perf_fee_ext"])

        df["adm_fee"] = df["adm_fee"].where(df["adm_fee"] <= 5, df["adm_fee"] / 100)
        df["perf_fee"] = df["perf_fee"].where(df["perf_fee"] <= 5, df["perf_fee"] / 100)

        return df[
            [
                "fund_cnpj",
                "adm_fee",
                "adm_fee_date",
                "perf_fee",
                "perf_fee_desc",
                "perf_fee_date",
                "has_perf_fee",
            ]
        ]

    def _clean_cad_fi_hist(
        self, adm_raw: pd.DataFrame, perf_raw: pd.DataFrame
    ) -> pd.DataFrame:
        adm = adm_raw.copy()
        adm["fund_cnpj"] = adm["CNPJ_FUNDO"].map(fmt_cnpj)
        adm["DT_INI_TAXA_ADM"] = pd.to_datetime(adm["DT_INI_TAXA_ADM"], errors="coerce")
        adm["TAXA_ADM"] = pd.to_numeric(adm["TAXA_ADM"], errors="coerce")
        adm = (
            adm.sort_values("DT_INI_TAXA_ADM")
            .groupby("fund_cnpj", as_index=False)
            .last()[["fund_cnpj", "TAXA_ADM", "DT_INI_TAXA_ADM"]]
            .rename(columns={"TAXA_ADM": "adm_fee", "DT_INI_TAXA_ADM": "adm_fee_date"})
        )

        if perf_raw.empty:
            adm["perf_fee"] = np.nan
            adm["perf_fee_desc"] = None
            adm["perf_fee_date"] = pd.NaT
            adm["has_perf_fee"] = np.nan
            return adm

        perf = perf_raw.copy()
        perf["fund_cnpj"] = perf["CNPJ_FUNDO"].map(fmt_cnpj)
        perf["DT_INI_TAXA_PERFM"] = pd.to_datetime(
            perf["DT_INI_TAXA_PERFM"], errors="coerce"
        )
        perf["VL_TAXA_PERFM"] = pd.to_numeric(perf["VL_TAXA_PERFM"], errors="coerce")
        perf = (
            perf.sort_values("DT_INI_TAXA_PERFM")
            .groupby("fund_cnpj", as_index=False)
            .last()[
                ["fund_cnpj", "VL_TAXA_PERFM", "DS_TAXA_PERFM", "DT_INI_TAXA_PERFM"]
            ]
            .rename(
                columns={
                    "VL_TAXA_PERFM": "perf_fee",
                    "DS_TAXA_PERFM": "perf_fee_desc",
                    "DT_INI_TAXA_PERFM": "perf_fee_date",
                }
            )
        )
        perf["has_perf_fee"] = perf["perf_fee"].map(
            lambda x: (x > 0) if pd.notna(x) else np.nan
        )

        return adm.merge(perf, how="outer", on="fund_cnpj")

    def _clean_extrato(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df[
            ["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "TAXA_ADM", "EXISTE_TAXA_PERFM"]
        ].copy()
        out["DT_COMPTC"] = pd.to_datetime(out["DT_COMPTC"], errors="coerce")
        out["TAXA_ADM"] = pd.to_numeric(out["TAXA_ADM"], errors="coerce")
        out = (
            out.sort_values("DT_COMPTC")
            .groupby("CNPJ_FUNDO_CLASSE", as_index=False)
            .last()[["CNPJ_FUNDO_CLASSE", "TAXA_ADM", "EXISTE_TAXA_PERFM"]]
            .rename(columns={"CNPJ_FUNDO_CLASSE": "fund_cnpj", "TAXA_ADM": "adm_fee"})
        )
        out["has_perf_fee"] = out["EXISTE_TAXA_PERFM"].map({"S": True, "N": False})
        return out[["fund_cnpj", "adm_fee", "has_perf_fee"]]

    def _check_row_count(self, df: pd.DataFrame) -> Check:
        n = len(df)
        passed = n > 0
        return Check(
            name="row_count_positive",
            passed=passed,
            severity="error",
            value=str(n),
            threshold="> 0",
            message=None if passed else "staging.fees is empty",
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

    def _check_adm_fee_coverage(self, df: pd.DataFrame) -> Check:
        pct_null = df["adm_fee"].isna().mean() * 100
        passed = pct_null < 20.0
        return Check(
            name="adm_fee_coverage",
            passed=passed,
            severity="warning",
            value=f"{pct_null:.1f}%",
            threshold="< 20%",
            message=None if passed else f"{pct_null:.1f}% of funds missing adm_fee",
        )

    def _check_adm_fee_range(self, df: pd.DataFrame) -> Check:
        out_of_range = int(((df["adm_fee"] < 0) | (df["adm_fee"] > 5)).sum())
        passed = out_of_range == 0
        return Check(
            name="adm_fee_range",
            passed=passed,
            severity="warning",
            value=str(out_of_range),
            threshold="0",
            message=(
                None if passed else f"{out_of_range} funds with adm_fee outside [0, 5%]"
            ),
        )


def stage_fees(db: DuckDBWarehouse, reference_date: date) -> int:
    return FeesStager().stage(db, reference_date)


def validate_fees(db: DuckDBWarehouse, reference_date: date) -> list[Check]:
    return FeesStager().validate(db, reference_date)
