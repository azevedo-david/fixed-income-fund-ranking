"""ANBIMA public xlsx ingestion for RCVM 175 fund characteristics."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ..config import RAW_DIR

ANBIMA_RAW = RAW_DIR / "anbima"
CARACTERISTICAS_FILE = ANBIMA_RAW / "FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx"

_MAX_FILE_AGE_DAYS = 15

_COL_RENAME = {
    "CNPJ da Classe": "Cnpj_Da_Classe",
    "Código ANBIMA": "Codigo_Anbima",
    "Estrutura": "Estrutura",
    "Nome Comercial": "Nome_Comercial",
    "Categoria ANBIMA": "Categoria_Anbima",
    "Tipo ANBIMA": "Tipo_Anbima",
    "Nível 1 Categoria": "Nivel_1_Categoria",
    "Nível 2 Categoria": "Nivel_2_Categoria",
    "Nível 3 Subcategoria": "Nivel_3_Subcategoria",
    "Foco Atuação": "Foco_Atuacao",
    "Composição do Fundo": "Composicao_Do_Fundo",
    "Aberto Estatutariamente": "Aberto_Estatutariamente",
    "Fundo ESG": "Fundo_Esg",
    "Tributação Alvo": "Tributacao_Alvo",
    "Administrador": "Administrador",
    "Gestor Principal": "Gestor_Principal",
    "Tipo de Investidor": "Tipo_De_Investidor",
    "Característica do Investidor": "Caracteristica_Do_Investidor",
    "Aplicação Inicial Mínima": "Aplicacao_Inicial_Minima",
    "Cota de Abertura": "Cota_De_Abertura",
    "Prazo Pagamento Resgate em dias": "Prazo_Pagamento_Resgate_Em_Dias",
    "Código CVM Subclasse": "Codigo_Cvm_Subclasse",
}


def fetch_caracteristicas(path: Path = CARACTERISTICAS_FILE) -> pd.DataFrame:
    """Read the ANBIMA characteristics xlsx and return the raw DataFrame with normalised column names."""
    _guard_xlsx_age(path)
    df = pd.read_excel(path)
    return df.rename(columns=_COL_RENAME)


def _guard_xlsx_age(path: Path) -> None:
    """Raise if the xlsx file does not exist or is older than _MAX_FILE_AGE_DAYS."""
    if not path.exists():
        raise FileNotFoundError(
            f"ANBIMA xlsx not found: {path}. Download it manually from the ANBIMA portal."
        )
    age_days = (date.today() - date.fromtimestamp(path.stat().st_mtime)).days
    if age_days > _MAX_FILE_AGE_DAYS:
        raise RuntimeError(
            f"ANBIMA xlsx is {age_days} days old (max {_MAX_FILE_AGE_DAYS}): {path}. "
            "Download a fresh copy from the ANBIMA portal."
        )
