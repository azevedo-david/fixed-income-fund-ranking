"""ANBIMA characteristics: raw.anbima_caracteristicas → staging-ready DataFrame."""

from __future__ import annotations

import pandas as pd

from ._utils import fmt_cnpj
from ..storage import DuckDBWarehouse

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


def fetch_raw_anbima(db: DuckDBWarehouse) -> pd.DataFrame | None:
    """Return the latest ANBIMA snapshot cleaned and ready for staging.anbima."""
    reference_date = db.execute(
        "SELECT MAX(reference_date) FROM raw.anbima_caracteristicas"
    ).fetchone()[0]
    if reference_date is None:
        return None

    raw = db.execute(
        "SELECT * FROM raw.anbima_caracteristicas WHERE reference_date = ?",
        [reference_date],
    ).df()

    df = _clean(raw)
    df["reference_date"] = reference_date
    return df


def _normalise_subclass_id(value) -> str | None:
    """Coerce Excel float codes (123.0 → '123') and strip whitespace."""
    if pd.isna(value):
        return None
    if isinstance(value, float):
        return str(int(value))
    return str(value).strip() or None


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Subclasse rows with no CVM code would collide with their parent class
    # (both mapping to subclass_id=None); non-subclasse rows must be None.
    if "Código CVM Subclasse" in out.columns:
        unmappable = (out["Estrutura"] == "Subclasse") & out[
            "Código CVM Subclasse"
        ].isna()
        out = out[~unmappable].copy()
        out["Código CVM Subclasse"] = out["Código CVM Subclasse"].where(
            out["Estrutura"] == "Subclasse"
        )
    else:
        out["Código CVM Subclasse"] = None

    out = out.rename(
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
    out["subclass_id"] = out["subclass_id"].map(_normalise_subclass_id)
    out["redemption_days"] = pd.to_numeric(out["redemption_days"], errors="coerce")
    out["min_initial_investment"] = pd.to_numeric(
        out["min_initial_investment"], errors="coerce"
    )
    return out[_COLS].drop_duplicates(subset=["fund_cnpj", "subclass_id"], keep="first")
