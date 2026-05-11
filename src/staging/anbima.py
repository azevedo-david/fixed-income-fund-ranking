"""ANBIMA characteristics: raw.anbima_caracteristicas → staging-ready DataFrame."""

from __future__ import annotations

import pandas as pd

from ._utils import fmt_cnpj
from ..storage import DuckDBWarehouse

_RENAME = {
    "Cnpj_Da_Classe": "fund_cnpj",
    "Codigo_Cvm_Subclasse": "subclass_id",
    "Codigo_Anbima": "anbima_code",
    "Estrutura": "structure",
    "Nome_Comercial": "commercial_name",
    "Categoria_Anbima": "category",
    "Tipo_Anbima": "type",
    "Nivel_1_Categoria": "level_1",
    "Nivel_2_Categoria": "level_2",
    "Nivel_3_Subcategoria": "level_3",
    "Foco_Atuacao": "focus",
    "Composicao_Do_Fundo": "composition",
    "Aberto_Estatutariamente": "open_to_public",
    "Fundo_Esg": "is_esg",
    "Tributacao_Alvo": "target_taxation",
    "Administrador": "administrator",
    "Gestor_Principal": "lead_manager",
    "Tipo_De_Investidor": "investor_type",
    "Caracteristica_Do_Investidor": "investor_profile",
    "Aplicacao_Inicial_Minima": "min_initial_investment",
    "Cota_De_Abertura": "open_nav_quota",
    "Prazo_Pagamento_Resgate_Em_Dias": "redemption_days",
}


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
    if "Codigo_Cvm_Subclasse" in out.columns:
        unmappable = (out["Estrutura"] == "Subclasse") & out[
            "Codigo_Cvm_Subclasse"
        ].isna()
        out = out[~unmappable].copy()
        out["Codigo_Cvm_Subclasse"] = out["Codigo_Cvm_Subclasse"].where(
            out["Estrutura"] == "Subclasse"
        )
    else:
        out["Codigo_Cvm_Subclasse"] = None

    out = out.rename(columns=_RENAME)
    out["fund_cnpj"] = out["fund_cnpj"].map(fmt_cnpj)
    out["subclass_id"] = out["subclass_id"].map(_normalise_subclass_id)
    out["redemption_days"] = pd.to_numeric(out["redemption_days"], errors="coerce")
    out["min_initial_investment"] = pd.to_numeric(
        out["min_initial_investment"], errors="coerce"
    )
    return out[list(_RENAME.values())].drop_duplicates(
        subset=["fund_cnpj", "subclass_id"], keep="first"
    )
