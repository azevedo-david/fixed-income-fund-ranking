"""Staging transform and validation for CVM fund registry → staging.registry."""

from __future__ import annotations

import logging
import re
from datetime import date

import pandas as pd

from ._base import BaseStager
from ._utils import Check, fmt_cnpj
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_CNPJ_RE = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")
_ACTIVE_STATUS = "Em Funcionamento Normal"
_KNOWN_STATUSES = {
    "Em Funcionamento Normal",
    "Em Liquidação",
    "Cancelado",
    "Em Fase Pré-Operacional",
    "Em Processo de Transformação",
}


class RegistryStager(BaseStager):
    dataset = "staging.registry"
    task_stage = "stage_registry"
    task_validate = "validate_registry"

    def _fetch_raw(
        self, db: DuckDBWarehouse, reference_date: date
    ) -> pd.DataFrame | None:
        downloaded_at = db.execute(
            "SELECT MAX(downloaded_at) FROM raw.registro_classe WHERE downloaded_at <= ?",
            [reference_date],
        ).fetchone()[0]

        if downloaded_at is None:
            return None

        classe = db.execute(
            "SELECT * FROM raw.registro_classe WHERE downloaded_at = ?",
            [downloaded_at],
        ).df()
        subclasse = db.execute(
            "SELECT * FROM raw.registro_subclasse WHERE downloaded_at = ?",
            [downloaded_at],
        ).df()

        return self._clean(classe, subclasse)

    def _build_checks(self, df: pd.DataFrame) -> list[Check]:
        return [
            self._check_row_count(df),
            self._check_no_null_cnpj(df),
            self._check_cnpj_format(df),
            self._check_active_status_present(df),
            self._check_unknown_statuses(df),
            self._check_inception_date_coverage(df),
        ]

    def _clean(self, classe: pd.DataFrame, subclasse: pd.DataFrame) -> pd.DataFrame:
        return self._merge(self._clean_classe(classe), self._clean_subclasse(subclasse))

    def _clean_classe(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["Data_Inicio"] = pd.to_datetime(out["Data_Inicio"], errors="coerce")
        out["CNPJ_Classe"] = out["CNPJ_Classe"].map(fmt_cnpj)
        return out.rename(
            columns={
                "CNPJ_Classe": "fund_cnpj",
                "Denominacao_Social": "fund_name",
                "Data_Inicio": "inception_date",
                "Situacao": "status",
                "Classificacao_Anbima": "anbima_category",
                "Publico_Alvo": "target_investor",
                "Classe_Cotas": "share_class",
                "Forma_Condominio": "fund_structure",
                "Exclusivo": "is_exclusive",
            }
        )[
            [
                "fund_cnpj",
                "ID_Registro_Classe",
                "fund_name",
                "inception_date",
                "status",
                "anbima_category",
                "target_investor",
                "share_class",
                "fund_structure",
                "is_exclusive",
            ]
        ]

    def _clean_subclasse(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "ID_Subclasse": "subclass_id",
                "Denominacao_Social": "fund_name_sub",
                "Previdenciario": "is_pension",
            }
        )[["ID_Registro_Classe", "subclass_id", "fund_name_sub", "is_pension"]]

    def _merge(self, classe: pd.DataFrame, subclasse: pd.DataFrame) -> pd.DataFrame:
        merged = classe.merge(subclasse, how="left", on="ID_Registro_Classe")
        merged["fund_name"] = merged["fund_name_sub"].where(
            merged["fund_name_sub"].notna(), merged["fund_name"]
        )
        merged["is_pension"] = merged["is_pension"].fillna("N")
        return merged[
            [
                "fund_cnpj",
                "subclass_id",
                "fund_name",
                "inception_date",
                "status",
                "anbima_category",
                "target_investor",
                "share_class",
                "fund_structure",
                "is_exclusive",
                "is_pension",
            ]
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
            message=None if passed else "staging.registry is empty",
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

    def _check_cnpj_format(self, df: pd.DataFrame) -> Check:
        n_bad = int(
            df["fund_cnpj"].dropna().map(lambda v: not bool(_CNPJ_RE.match(v))).sum()
        )
        passed = n_bad == 0
        return Check(
            name="cnpj_format",
            passed=passed,
            severity="error",
            value=str(n_bad),
            threshold="0",
            message=(
                None
                if passed
                else f"{n_bad} fund_cnpj values do not match XX.XXX.XXX/XXXX-XX"
            ),
        )

    def _check_active_status_present(self, df: pd.DataFrame) -> Check:
        active = int((df["status"] == _ACTIVE_STATUS).sum())
        passed = active > 0
        return Check(
            name="active_status_present",
            passed=passed,
            severity="error",
            value=str(active),
            threshold="> 0",
            message=(
                None if passed else f"no funds with status '{_ACTIVE_STATUS}' found"
            ),
        )

    def _check_unknown_statuses(self, df: pd.DataFrame) -> Check:
        found = set(df["status"].dropna().unique())
        unknown = found - _KNOWN_STATUSES
        passed = len(unknown) == 0
        if not passed:
            logger.warning(
                "%s: unknown status values encountered: %s",
                self.task_validate,
                ", ".join(sorted(unknown)),
            )
        return Check(
            name="unknown_statuses",
            passed=passed,
            severity="warning",
            value=", ".join(sorted(unknown)) if unknown else None,
            threshold="all statuses in known set",
            message=(
                None
                if passed
                else f"unknown status values: {', '.join(sorted(unknown))}"
            ),
        )

    def _check_inception_date_coverage(self, df: pd.DataFrame) -> Check:
        pct_null = df["inception_date"].isna().mean() * 100
        passed = pct_null < 5.0
        return Check(
            name="inception_date_coverage",
            passed=passed,
            severity="warning",
            value=f"{pct_null:.1f}%",
            threshold="< 5%",
            message=(
                None if passed else f"{pct_null:.1f}% of rows missing inception_date"
            ),
        )


def stage_registry(db: DuckDBWarehouse, reference_date: date) -> int:
    return RegistryStager().stage(db, reference_date)


def validate_registry(db: DuckDBWarehouse, reference_date: date) -> list[Check]:
    return RegistryStager().validate(db, reference_date)
