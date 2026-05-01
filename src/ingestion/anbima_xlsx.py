"""ANBIMA public xlsx ingestion (RCVM 175 fund characteristics).

Source file (manually downloaded into data/raw/anbima/):
    FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx — fund characteristics (28 cols)

Uses CNPJ as raw 14-digit integer and "Código CVM Subclasse" as the
subclass identifier when Estrutura == "Subclasse".

Output is normalised to merge with cvm.ipynb's funds_df on:
    CNPJ_FUNDO_CLASSE  (formatted XX.XXX.XXX/XXXX-XX)
    ID_SUBCLASSE       (string, or NaN/None for class-level rows)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .cvm import fmt_cnpj

ANBIMA_RAW = Path("data/raw/anbima")
ANBIMA_PROCESSED = Path("data/processed/anbima")
CARACTERISTICAS_FILE = ANBIMA_RAW / "FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx"
CARACTERISTICAS_PARQUET = ANBIMA_PROCESSED / "caracteristicas.parquet"


def _cache_is_fresh(parquet: Path, source: Path) -> bool:
    return (
        parquet.exists()
        and source.exists()
        and parquet.stat().st_mtime >= source.stat().st_mtime
    )


def _normalise_cnpj(value) -> str | None:
    if pd.isna(value):
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return fmt_cnpj(digits) if digits else None


def _normalise_id_subclasse(value) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, float):
        return str(int(value))
    return str(value).strip() or None


def _add_merge_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Add CNPJ_FUNDO_CLASSE and ID_SUBCLASSE merge keys.

    Drops Estrutura=='Subclasse' rows without 'Código CVM Subclasse' — they
    can't be matched to CVM's funds_df.ID_Subclasse and would collide with
    their parent class row (both ending up with ID_SUBCLASSE=None).
    """
    df = df.copy()
    df["CNPJ_FUNDO_CLASSE"] = df["CNPJ da Classe"].map(_normalise_cnpj)
    if "Código CVM Subclasse" in df.columns:
        unmappable = (df["Estrutura"] == "Subclasse") & df["Código CVM Subclasse"].isna()
        df = df[~unmappable].copy()
        is_subclasse = df["Estrutura"] == "Subclasse"
        df["ID_SUBCLASSE"] = df["Código CVM Subclasse"].where(is_subclasse).map(_normalise_id_subclasse)
    else:
        df["ID_SUBCLASSE"] = None
    return df


def load_caracteristicas(
    path: Path = CARACTERISTICAS_FILE,
    cache: Path = CARACTERISTICAS_PARQUET,
    force: bool = False,
) -> pd.DataFrame:
    """Load and clean the CARACTERISTICAS xlsx, caching the result as parquet.

    On subsequent calls the parquet is read directly (~50x faster than re-parsing
    the xlsx). Cache is invalidated when the source xlsx mtime is newer than the
    parquet, or when force=True.
    """
    if not force and _cache_is_fresh(cache, path):
        return pd.read_parquet(cache)

    raw = pd.read_excel(path)
    df = _add_merge_keys(raw)
    cols = {
        "CNPJ_FUNDO_CLASSE": "CNPJ_FUNDO_CLASSE",
        "ID_SUBCLASSE": "ID_SUBCLASSE",
        "Código ANBIMA": "anbima_code",
        "Estrutura": "estrutura",
        "Nome Comercial": "nome_comercial",
        "Categoria ANBIMA": "categoria_anbima",
        "Tipo ANBIMA": "tipo_anbima",
        "Nível 1 Categoria": "nivel_1",
        "Nível 2 Categoria": "nivel_2",
        "Nível 3 Subcategoria": "nivel_3",
        "Foco Atuação": "foco_atuacao",
        "Composição do Fundo": "composicao",
        "Aberto Estatutariamente": "aberto_estatutariamente",
        "Fundo ESG": "fundo_esg",
        "Tributação Alvo": "tributacao_alvo",
        "Administrador": "administrador",
        "Gestor Principal": "gestor_principal",
        "Tipo de Investidor": "tipo_investidor",
        "Característica do Investidor": "caracteristica_investidor",
        "Aplicação Inicial Mínima": "aplicacao_inicial_minima",
        "Cota de Abertura": "cota_abertura",
        "Prazo Pagamento Resgate em dias": "prazo_pagamento_resgate",
    }
    out = df[list(cols)].rename(columns=cols)

    out["prazo_pagamento_resgate"] = pd.to_numeric(
        out["prazo_pagamento_resgate"], errors="coerce"
    ).astype("Int32")
    out["aplicacao_inicial_minima"] = pd.to_numeric(
        out["aplicacao_inicial_minima"], errors="coerce"
    )

    out = out.drop_duplicates(subset=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"], keep="first")
    cache.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cache, index=False)
    return out


