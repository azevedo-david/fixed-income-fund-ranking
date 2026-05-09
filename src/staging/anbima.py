"""Staging transform and validation for raw.anbima_caracteristicas → staging.anbima."""

from __future__ import annotations

import logging

import pandas as pd

from ._base import BaseStager
from ._utils import Check, fmt_cnpj
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_COLS = [
    "fund_cnpj",
    "subclass_id",
    "anbima_code",
    "structure",
    "commercial_name",
    "category",
    "type",
    "level_1",
    "level_2",
    "level_3",
    "focus",
    "composition",
    "open_to_public",
    "is_esg",
    "target_taxation",
    "administrator",
    "lead_manager",
    "investor_type",
    "investor_profile",
    "min_initial_investment",
    "open_nav_quota",
    "redemption_days",
]


class AnbimaStager(BaseStager):
    dataset = "staging.anbima"
    raw_dataset = "raw.anbima_caracteristicas"
    task_stage = "stage_anbima"
    task_validate = "validate_anbima"

    def _fetch_raw(self, db: DuckDBWarehouse) -> pd.DataFrame | None:
        reference_date = db.execute(
            "SELECT MAX(reference_date) FROM raw.anbima_caracteristicas"
        ).fetchone()[0]

        if reference_date is None:
            return None

        raw = db.execute(
            "SELECT * FROM raw.anbima_caracteristicas WHERE reference_date = ?",
            [reference_date],
        ).df()

        df = self._clean(raw)
        df["reference_date"] = reference_date
        return df

    def _build_checks(self, df: pd.DataFrame) -> list[Check]:
        return [
            self._check_row_count(df),
            self._check_no_null_cnpj(df),
            self._check_freshness(df),
        ]

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.rename(
            columns={
                "CNPJ da Classe": "fund_cnpj",
                "Código ANBIMA": "anbima_code",
                "Estrutura": "structure",
                "Nome Comercial": "commercial_name",
                "Categoria ANBIMA": "category",
                "Tipo ANBIMA": "type",
                "Nível 1 Categoria": "level_1",
                "Nível 2 Categoria": "level_2",
                "Nível 3 Subcategoria": "level_3",
                "Foco Atuação": "focus",
                "Composição do Fundo": "composition",
                "Aberto Estatutariamente": "open_to_public",
                "Fundo ESG": "is_esg",
                "Tributação Alvo": "target_taxation",
                "Administrador": "administrator",
                "Gestor Principal": "lead_manager",
                "Tipo de Investidor": "investor_type",
                "Característica do Investidor": "investor_profile",
                "Aplicação Inicial Mínima": "min_initial_investment",
                "Cota de Abertura": "open_nav_quota",
                "Prazo Pagamento Resgate em dias": "redemption_days",
                "Código CVM Subclasse": "subclass_id",
            }
        )
        out["fund_cnpj"] = out["fund_cnpj"].map(fmt_cnpj)
        return out[_COLS]

    def _check_row_count(self, df: pd.DataFrame) -> Check:
        n = len(df)
        passed = n > 0
        return Check(
            name="row_count_positive",
            passed=passed,
            severity="error",
            value=str(n),
            threshold="> 0",
            message=None if passed else "staging.anbima is empty",
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


def stage_anbima(db: DuckDBWarehouse, force: bool = False) -> int:
    return AnbimaStager().stage(db, force=force)


def validate_anbima(db: DuckDBWarehouse) -> list[Check]:
    return AnbimaStager().validate(db)
