"""Fund universe construction.

Pipeline:
    1. CVM registro_fundo_classe → eligible classes + subclasses → funds_df.
    2. Enrich with adm_fee and has_perf_fee (cad_fi historical primary,
       extrato_fi as fallback for funds missing in cad_fi).
    3. Enrich with median PL and median holders over the AuM lookback window;
       filter by min_aum and min_cotistas.
    4. Enrich with ANBIMA xlsx: redemption_days, target_taxation, min_investment.

All ingestion (downloads, parsing, cleaning) goes through ``src.ingestion``.
This module owns only filtering, merging and aggregation.
"""
from __future__ import annotations

import logging

import pandas as pd

from ..config import Settings
from ..ingestion.anbima_xlsx import load_caracteristicas
from ..ingestion.cvm import (
    fetch_registro_fundo_classe,
    fmt_cnpj,
    load_cad_fi_taxa,
    load_extrato_taxa,
    load_inf_diario,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1: registry filters and merge
# ---------------------------------------------------------------------------

CLASSE_KEEP = [
    "CNPJ_Classe",
    "fund_name",
    "ID_Registro_Classe",
    "Codigo_CVM",
    "Classificacao_Anbima",
    "Data_Inicio",
    "target_investor",
    "Classe_Cotas",
    "Forma_Condominio",
]

SUBCLASSE_KEEP = [
    "ID_Registro_Classe",
    "ID_Subclasse",
    "fund_name_subclasse",
    "Situacao_Subclasse",
    "target_investor_subclasse",
]


def build_classe_eligible(classe: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    """Filter classes: active, Renda Fixa, not exclusive, not closed-end,
    started on or before the reference date."""
    df = classe.copy()
    df["Data_Inicio"] = pd.to_datetime(df["Data_Inicio"], errors="coerce")

    eligible = (
        df[
            (df["Situacao"] == "Em Funcionamento Normal")
            & (df["Classificacao_Anbima"].str.startswith("Renda Fixa", na=False))
            & ~(df["Exclusivo"] == "S")               # NaN allowed
            & ~(df["Forma_Condominio"] == "Fechado")  # NaN allowed
            & (df["Data_Inicio"] <= reference_date)
        ]
        .rename(columns={"Denominacao_Social": "fund_name", "Publico_Alvo": "target_investor"})
        [CLASSE_KEEP].copy()
    )

    eligible["CNPJ_Classe"] = eligible["CNPJ_Classe"].apply(fmt_cnpj)
    eligible = (
        eligible.sort_values("Data_Inicio")
        .drop_duplicates(subset="CNPJ_Classe", keep="last")
    )
    return eligible


def build_subclasse_eligible(subclasse: pd.DataFrame) -> pd.DataFrame:
    """Filter subclasses: active, non-pension, open-end, not exclusive."""
    eligible = subclasse[
        (subclasse["Situacao"] == "Em Funcionamento Normal")
        & (subclasse["Previdenciario"] == "N")
        & ~(subclasse["Forma_Condominio"] == "Fechado")
        & ~(subclasse["Exclusivo"] == "S")
        & ~(subclasse["Exclusivo_INR"] == "S")
    ]
    renamed = eligible.rename(columns={
        "Denominacao_Social": "fund_name_subclasse",
        "Situacao":           "Situacao_Subclasse",
        "Publico_Alvo":       "target_investor_subclasse",
    })
    return renamed[SUBCLASSE_KEEP]


def merge_classe_subclasse(
    classe_eligible: pd.DataFrame,
    subclasse_eligible: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join classe with eligible subclasses; rename keys to canonical names."""
    merged = classe_eligible.merge(
        subclasse_eligible, how="left", on="ID_Registro_Classe"
    )
    return merged.rename(columns={
        "CNPJ_Classe": "CNPJ_FUNDO_CLASSE",
        "ID_Subclasse": "ID_SUBCLASSE",
    })


# ---------------------------------------------------------------------------
# Step 2: adm_fee + has_perf_fee enrichment
# ---------------------------------------------------------------------------

def enrich_with_fees(
    funds_df: pd.DataFrame,
    extrato_year: int,
    force: bool = False,
) -> pd.DataFrame:
    """Add adm_fee and has_perf_fee.

    Primary source: cad_fi historical (load_cad_fi_taxa).
    Fallback: extrato_fi (load_extrato_taxa) for funds missing in cad_fi.
    Scale fix: values > 5 are likely basis points (e.g. 80 → 0.80%).
    """
    fees_hist = load_cad_fi_taxa(force=force).copy()

    out = funds_df.merge(
        fees_hist[["CNPJ_FUNDO_CLASSE", "adm_fee", "has_perf_fee"]],
        how="left",
        on="CNPJ_FUNDO_CLASSE",
    )

    extrato = load_extrato_taxa(extrato_year, force=force).rename(columns={
        "adm_fee":      "adm_fee_ext",
        "has_perf_fee": "has_perf_fee_ext",
    })
    out = out.merge(extrato, how="left", on="CNPJ_FUNDO_CLASSE")

    out["adm_fee"]      = out["adm_fee"].fillna(out["adm_fee_ext"])
    out["has_perf_fee"] = out["has_perf_fee"].fillna(out["has_perf_fee_ext"])
    out = out.drop(columns=["adm_fee_ext", "has_perf_fee_ext"])

    out["adm_fee"] = out["adm_fee"].where(out["adm_fee"] <= 5, out["adm_fee"] / 100)
    return out


# ---------------------------------------------------------------------------
# Step 3: AuM and holders aggregation
# ---------------------------------------------------------------------------

def _aggregate_aum_holders(
    df_clean: pd.DataFrame,
    reference_date: pd.Timestamp,
    aum_lookback_days: int,
) -> pd.DataFrame:
    """Aggregate median PL and holders per (CNPJ, ID_SUBCLASSE) over the lookback window.

    Restricts to dates in (reference_date - aum_lookback_days, reference_date].
    Masks NR_COTST=0 with PL>0 as NaN — a known CVM data error where the
    holder count drops to zero on a day the fund still has positive PL.
    """
    df = df_clean.copy()
    window_start = reference_date - pd.Timedelta(days=aum_lookback_days)
    df = df[(df["DT_COMPTC"] >= window_start) & (df["DT_COMPTC"] <= reference_date)]

    bad_holders = (df["NR_COTST"] == 0) & (df["VL_PATRIM_LIQ"] > 0)
    df.loc[bad_holders, "NR_COTST"] = pd.NA

    return (
        df.groupby(["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"], dropna=False)
          .agg(
              median_pl=("VL_PATRIM_LIQ", "median"),
              median_cotistas=("NR_COTST", "median"),
              n_rows=("DT_COMPTC", "count"),
          )
          .reset_index()
    )


def enrich_with_aum_holders(
    funds_df: pd.DataFrame,
    settings: Settings,
    force: bool = False,
) -> pd.DataFrame:
    """Inner-join median PL/holders, filtering by min_aum and min_cotistas."""
    universe_keys = set(zip(
        funds_df["CNPJ_FUNDO_CLASSE"],
        funds_df["ID_SUBCLASSE"].where(funds_df["ID_SUBCLASSE"].notna(), None),
    ))

    inf_diario = load_inf_diario(
        start=settings.reference_date - pd.Timedelta(days=settings.universe.aum_lookback_days),
        end=settings.reference_date,
        universe_keys=universe_keys,
        force=force,
    )

    agg = _aggregate_aum_holders(
        inf_diario,
        reference_date=pd.Timestamp(settings.reference_date),
        aum_lookback_days=settings.universe.aum_lookback_days,
    )

    enriched = funds_df.merge(agg, how="inner", on=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"])
    return enriched[
        (enriched["median_cotistas"] > settings.universe.min_cotistas)
        & (enriched["median_pl"] > settings.universe.min_aum)
    ].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 4: ANBIMA enrichment
# ---------------------------------------------------------------------------

ANBIMA_KEEP = [
    "CNPJ_FUNDO_CLASSE",
    "ID_SUBCLASSE",
    "prazo_pagamento_resgate",
    "tributacao_alvo",
    "aplicacao_inicial_minima",
]

_ANBIMA_RENAME = {
    "prazo_pagamento_resgate":  "redemption_days",
    "tributacao_alvo":          "target_taxation",
    "aplicacao_inicial_minima": "min_investment",
}


def enrich_with_anbima(funds_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join ANBIMA characteristics: redemption_days, target_taxation, min_investment."""
    car = load_caracteristicas()
    merged = funds_df.merge(
        car[ANBIMA_KEEP], how="left", on=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"]
    )
    return merged.rename(columns=_ANBIMA_RENAME)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def build_universe(settings: Settings, force: bool = False) -> pd.DataFrame:
    """End-to-end universe construction keyed by (CNPJ_FUNDO_CLASSE, ID_SUBCLASSE).

    The extrato year used for the fee fallback is taken from the year of
    ``settings.reference_date``.
    """
    tables = fetch_registro_fundo_classe(force=force)
    classe_eligible = build_classe_eligible(
        tables["registro_classe"], pd.Timestamp(settings.reference_date),
    )
    subclasse_eligible = build_subclasse_eligible(tables["registro_subclasse"])
    funds_df = merge_classe_subclasse(classe_eligible, subclasse_eligible)

    funds_df = enrich_with_fees(
        funds_df, extrato_year=settings.reference_date.year, force=force
    )
    funds_df = enrich_with_aum_holders(funds_df, settings, force=force)
    funds_df = enrich_with_anbima(funds_df)

    logger.info("universe: %d eligible funds", len(funds_df))
    return funds_df.rename(columns={
        "CNPJ_FUNDO_CLASSE":         "cnpj",
        "ID_SUBCLASSE":              "subclass_id",
        "ID_Registro_Classe":        "class_registry_id",
        "Codigo_CVM":                "cvm_code",
        "Classificacao_Anbima":      "anbima_class",
        "Data_Inicio":               "start_date",
        "Classe_Cotas":              "share_class",
        "Forma_Condominio":          "fund_structure",
        "fund_name_subclasse":       "subclass_name",
        "Situacao_Subclasse":        "subclass_status",
        "target_investor_subclasse": "subclass_target_investor",
        "median_pl":                 "median_aum",
        "median_cotistas":           "median_holders",
    })