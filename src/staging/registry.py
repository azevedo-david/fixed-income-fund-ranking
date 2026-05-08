"""Staging transform and validation for CVM fund registry → staging.registro."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from ._utils import Check, fmt_cnpj, log_checks
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_DATASET = "staging.registro"
_TASK = "stage_registro"
_TASK_VALIDATE = "validate_registro"


def stage_registro(db: DuckDBWarehouse, reference_date: date) -> int:
    """Read the latest raw registry snapshot, clean, and write to staging.registro."""
    downloaded_at = db.execute(
        "SELECT MAX(downloaded_at) FROM raw.registro_classe WHERE downloaded_at <= ?",
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

    df = _clean(classe, subclasse, reference_date)
    df["reference_date"] = reference_date
    rows = db.upsert_derived("staging", "registro", df, reference_date=reference_date)
    logger.info("stage registro: %d rows written", rows)
    return rows


def validate_registro(db: DuckDBWarehouse, reference_date: date) -> list[Check]:
    """Run data quality checks on staging.registro and write results to logs.validation_log."""
    df = db.execute(
        "SELECT * FROM staging.registro WHERE reference_date = ?", [reference_date]
    ).df()

    checks = [
        _check_row_count(df),
        _check_no_null_cnpj(df),
        _check_anbima_category(df),
        _check_inception_date_coverage(df),
    ]

    log_checks(db, checks, _DATASET, _TASK_VALIDATE, reference_date)

    failed_errors = [c for c in checks if not c.passed and c.severity == "error"]
    if failed_errors:
        names = ", ".join(c.name for c in failed_errors)
        raise ValueError(f"validate registro: error-level checks failed: {names}")

    return checks


def _clean(
    classe: pd.DataFrame,
    subclasse: pd.DataFrame,
    reference_date: date,
) -> pd.DataFrame:
    cl = _clean_classe(classe, reference_date)
    sub = _clean_subclasse(subclasse)
    return _merge(cl, sub)


def _clean_classe(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    out = df.copy()
    out["Data_Inicio"] = pd.to_datetime(out["Data_Inicio"], errors="coerce")
    out["CNPJ_Classe"] = out["CNPJ_Classe"].map(fmt_cnpj)

    out = out[
        (out["Situacao"] == "Em Funcionamento Normal")
        & (out["Classificacao_Anbima"].str.startswith("Renda Fixa", na=False))
        & (out["Data_Inicio"] <= pd.Timestamp(reference_date))
    ]

    return (
        out.sort_values("Data_Inicio")
        .drop_duplicates(subset="CNPJ_Classe", keep="last")
        .rename(
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
    )


def _clean_subclasse(df: pd.DataFrame) -> pd.DataFrame:
    out = df[df["Situacao"] == "Em Funcionamento Normal"].copy()
    return out.rename(
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


def _check_row_count(df: pd.DataFrame) -> Check:
    n = len(df)
    passed = n > 0
    return Check(
        name="row_count_positive",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="> 0",
        message=None if passed else "staging.registro is empty",
    )


def _check_no_null_cnpj(df: pd.DataFrame) -> Check:
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


def _check_anbima_category(df: pd.DataFrame) -> Check:
    n_wrong = int((~df["anbima_category"].str.startswith("Renda Fixa", na=True)).sum())
    passed = n_wrong == 0
    return Check(
        name="anbima_category_renda_fixa",
        passed=passed,
        severity="warning",
        value=str(n_wrong),
        threshold="0",
        message=(
            None if passed else f"{n_wrong} rows with non-Renda Fixa anbima_category"
        ),
    )


def _check_inception_date_coverage(df: pd.DataFrame) -> Check:
    pct_null = df["inception_date"].isna().mean() * 100
    passed = pct_null < 5.0
    return Check(
        name="inception_date_coverage",
        passed=passed,
        severity="warning",
        value=f"{pct_null:.1f}%",
        threshold="< 5%",
        message=None if passed else f"{pct_null:.1f}% of rows missing inception_date",
    )
