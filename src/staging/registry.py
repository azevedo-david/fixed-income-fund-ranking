"""CVM fund/class registry: raw.registro_* → staging-ready DataFrame."""

from __future__ import annotations

import pandas as pd

from ._utils import fmt_cnpj
from ..storage import DuckDBWarehouse


def fetch_raw_registry(db: DuckDBWarehouse) -> pd.DataFrame | None:
    """Return the latest raw registry snapshot cleaned and ready for staging.registry."""
    reference_date = db.execute(
        "SELECT MAX(reference_date) FROM raw.registro_classe"
    ).fetchone()[0]
    if reference_date is None:
        return None

    classe = db.execute(
        "SELECT * FROM raw.registro_classe WHERE reference_date = ?",
        [reference_date],
    ).df()
    subclasse = db.execute(
        "SELECT * FROM raw.registro_subclasse WHERE reference_date = ?",
        [reference_date],
    ).df()

    df = _merge(_clean_classe(classe), _clean_subclasse(subclasse))
    df["reference_date"] = reference_date
    return df


_STATUS_PRIORITY = {
    "Em Funcionamento Normal": 4,
    "Fase Pré-Operacional": 3,
    "Em Processo de Transformação": 2,
    "Em Liquidação": 1,
    "Cancelado": 0,
}


def _clean_classe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Data_Inicio"] = pd.to_datetime(out["Data_Inicio"], errors="coerce")
    out["CNPJ_Classe"] = out["CNPJ_Classe"].map(fmt_cnpj)
    out = out.rename(
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
    # Primary sort: status priority (active > cancelled); secondary: inception_date.
    # Keeps the active registration when the same CNPJ appears as both active and cancelled.
    return (
        out.assign(_sp=out["status"].map(_STATUS_PRIORITY).fillna(-1))
        .sort_values(["_sp", "inception_date"], na_position="first")
        .drop(columns=["_sp"])
        .drop_duplicates(subset="fund_cnpj", keep="last")
        .reset_index(drop=True)
    )


def _clean_subclasse(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "ID_Subclasse": "subclass_id",
            "Denominacao_Social": "fund_name_sub",
            "Previdenciario": "is_pension",
        }
    )[["ID_Registro_Classe", "subclass_id", "fund_name_sub", "is_pension"]]


def _merge(classe: pd.DataFrame, subclasse: pd.DataFrame) -> pd.DataFrame:
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
