"""Staging transform for CVM fund registry → staging.registro."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from ._utils import fmt_cnpj
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)


def clean_registry(
    classe: pd.DataFrame,
    subclasse: pd.DataFrame,
    reference_date: date,
) -> pd.DataFrame:
    """Filter and merge registro_classe + registro_subclasse into a normalised registry.

    Applies status and temporal filters only; business-rule filters (Exclusivo,
    Forma_Condominio, Previdenciario) are preserved as columns for universe.py.
    """
    cl = _clean_classe(classe, reference_date)
    sub = _clean_subclasse(subclasse)
    return _merge(cl, sub, reference_date)


def stage_registro(db: DuckDBWarehouse, reference_date: date) -> int:
    """Read raw registry snapshots, clean, and write to staging.registro."""
    downloaded_at = db.execute(
        "SELECT MAX(downloaded_at) FROM raw.registro_classe "
        "WHERE downloaded_at <= ?",
        [reference_date],
    ).fetchone()[0]

    if downloaded_at is None:
        logger.warning("stage registro: no raw.registro_classe snapshot available")
        return 0

    classe = db.execute(
        "SELECT * FROM raw.registro_classe WHERE downloaded_at = ?",
        [downloaded_at],
    ).df()
    subclasse = db.execute(
        "SELECT * FROM raw.registro_subclasse WHERE downloaded_at = ?",
        [downloaded_at],
    ).df()

    df = clean_registry(classe, subclasse, reference_date)
    df["reference_date"] = reference_date
    rows = db.upsert_derived("staging", "registro", df, reference_date=reference_date)
    logger.info("stage registro: %d rows written", rows)
    return rows


def _clean_classe(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    out = df.copy()
    out["Data_Inicio"] = pd.to_datetime(out["Data_Inicio"], errors="coerce")
    out["CNPJ_Classe"] = out["CNPJ_Classe"].map(fmt_cnpj)

    out = out[
        (out["Situacao"] == "Em Funcionamento Normal")
        & (out["Classificacao_Anbima"].str.startswith("Renda Fixa", na=False))
        & (out["Data_Inicio"] <= pd.Timestamp(reference_date))
    ]

    out = (
        out.sort_values("Data_Inicio")
        .drop_duplicates(subset="CNPJ_Classe", keep="last")
        .rename(
            columns={
                "CNPJ_Classe": "CNPJ_FUNDO_CLASSE",
                "Denominacao_Social": "fund_name",
                "Data_Inicio": "dt_inicio",
                "Situacao": "situacao",
                "Classificacao_Anbima": "classificacao_anbima",
                "Publico_Alvo": "target_investor",
                "Classe_Cotas": "classe_cotas",
                "Forma_Condominio": "forma_condominio",
                "Exclusivo": "exclusivo",
            }
        )
    )

    return out[
        [
            "CNPJ_FUNDO_CLASSE",
            "ID_Registro_Classe",
            "fund_name",
            "dt_inicio",
            "situacao",
            "classificacao_anbima",
            "target_investor",
            "classe_cotas",
            "forma_condominio",
            "exclusivo",
        ]
    ]


def _clean_subclasse(df: pd.DataFrame) -> pd.DataFrame:
    out = df[df["Situacao"] == "Em Funcionamento Normal"].copy()
    out = out.rename(
        columns={
            "ID_Subclasse": "ID_SUBCLASSE",
            "Denominacao_Social": "fund_name_sub",
            "Previdenciario": "previdenciario",
        }
    )
    return out[
        ["ID_Registro_Classe", "ID_SUBCLASSE", "fund_name_sub", "previdenciario"]
    ]


def _merge(
    classe: pd.DataFrame,
    subclasse: pd.DataFrame,
    reference_date: date,
) -> pd.DataFrame:
    merged = classe.merge(subclasse, how="left", on="ID_Registro_Classe")

    # For single-class funds (no subclasse row), carry classe fund_name and blank previdenciario
    merged["fund_name"] = merged["fund_name_sub"].where(
        merged["fund_name_sub"].notna(), merged["fund_name"]
    )
    merged["previdenciario"] = merged["previdenciario"].fillna("N")

    return merged[
        [
            "CNPJ_FUNDO_CLASSE",
            "ID_SUBCLASSE",
            "fund_name",
            "dt_inicio",
            "situacao",
            "classificacao_anbima",
            "target_investor",
            "classe_cotas",
            "forma_condominio",
            "exclusivo",
            "previdenciario",
        ]
    ]
