"""Validation checks for raw.* tables (post-ingestion gate)."""

from __future__ import annotations

from datetime import date

from ._base import (
    ValidateResult,
    check_and_gate,
    check_column_no_nulls,
    check_date_freshness,
    check_known_values,
    check_row_count,
    check_snapshot_exists,
    snapshot_date,
)
from ..storage import DuckDBWarehouse

_TASK = "validate_ingestion"
_DATASET = "raw"

_KNOWN_SITUACAO = {
    "Em Funcionamento Normal",
    "Em Liquidação",
    "Cancelado",
    "Fase Pré-Operacional",
    "Em Processo de Transformação",
}
_KNOWN_EXCLUSIVO = {"S", "N"}
_KNOWN_FORMA_CONDOMINIO = {"Aberto", "Fechado"}
_KNOWN_ESTRUTURA = {"Classe", "Subclasse"}
_ANBIMA_REQUIRED_COLUMNS = {
    "Cnpj_Da_Classe",
    "Estrutura",
    "Categoria_Anbima",
    "Tipo_De_Investidor",
    "Tributacao_Alvo",
    "Codigo_Cvm_Subclasse",
}
_KNOWN_TRIBUTACAO_ALVO = {
    "Longo Prazo",  # config-mapped
    "Isento",  # config-mapped
    "Curto Prazo",  # config-mapped
    "Alíquota de 15%",  # config-mapped
    "Previdenciário",  # pension funds — falls back to default_rate
    "Outros",  # catch-all — falls back to default_rate
    "Não Aplicável",  # falls back to default_rate
    "Especifica",  # falls back to default_rate
    "Indefinido",  # falls back to default_rate
}


def validate_ingestion(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    """Run all raw-layer checks, write to log, and raise on error-level failures."""
    return check_and_gate(
        db, _build_checks(db, reference_date), _TASK, _DATASET, reference_date
    )


def _build_checks(db: DuckDBWarehouse, reference_date: date) -> list[ValidateResult]:
    return [
        *_checks_inf_diario(db, reference_date),
        *_checks_cdi_daily(db, reference_date),
        *_checks_registro_classe(db, reference_date),
        *_checks_registro_subclasse(db, reference_date),
        *_checks_cad_fi_hist_taxa_adm(db, reference_date),
        *_checks_cad_fi_hist_taxa_perfm(db, reference_date),
        *_checks_extrato_fi(db, reference_date),
        *_checks_anbima_caracteristicas(db, reference_date),
    ]


def _checks_inf_diario(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_row_count(db, "raw.inf_diario", name="inf_diario_row_count"),
        check_date_freshness(
            db,
            "raw.inf_diario",
            "DT_COMPTC",
            reference_date,
            name="inf_diario_date_freshness",
        ),
        _inf_diario_nav_not_fully_null(db),
        _inf_diario_aum_null_rate(db),
        _inf_diario_shareholders_null_rate(db),
    ]


def _inf_diario_nav_not_fully_null(db: DuckDBWarehouse) -> ValidateResult:
    """A fully-null VL_QUOTA column means ffill produces no valid NAV, causing silent NaN returns throughout."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN VL_QUOTA IS NULL THEN 1.0 ELSE 0.0 END) FROM raw.inf_diario"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="inf_diario_nav_not_fully_null",
        passed=passed,
        severity="error",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of VL_QUOTA is null — ffill will produce no valid NAV"
        ),
    )


def _inf_diario_aum_null_rate(db: DuckDBWarehouse) -> ValidateResult:
    """AuM null rate > 50% means median_aum will be null for most funds, silently excluding them from universe."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN VL_PATRIM_LIQ IS NULL THEN 1.0 ELSE 0.0 END) FROM raw.inf_diario"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="inf_diario_aum_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of VL_PATRIM_LIQ is null — most funds will have null median_aum and be excluded from universe"
        ),
    )


def _inf_diario_shareholders_null_rate(db: DuckDBWarehouse) -> ValidateResult:
    """Shareholders null rate > 50% means most funds fail the min_cotistas filter and are silently excluded from universe."""
    null_rate = (
        db.execute(
            "SELECT AVG(CASE WHEN NR_COTST IS NULL THEN 1.0 ELSE 0.0 END) FROM raw.inf_diario"
        ).fetchone()[0]
        or 0.0
    )
    passed = null_rate < 0.5
    return ValidateResult(
        check_name="inf_diario_shareholders_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 50%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of NR_COTST is null — most funds will have null median_holders and be excluded from universe"
        ),
    )


def _checks_cdi_daily(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_row_count(db, "raw.cdi_daily", name="cdi_daily_row_count"),
        check_date_freshness(
            db,
            "raw.cdi_daily",
            "date",
            reference_date,
            name="cdi_daily_date_freshness",
        ),
        _cdi_rate_not_negative(db),
        _cdi_rate_in_daily_range(db),
    ]


def _cdi_rate_not_negative(db: DuckDBWarehouse) -> ValidateResult:
    n = db.execute("SELECT COUNT(*) FROM raw.cdi_daily WHERE rate < 0").fetchone()[0]
    passed = n == 0
    return ValidateResult(
        check_name="cdi_daily_rate_not_negative",
        passed=passed,
        severity="error",
        value=str(n),
        threshold="0",
        message=(
            None if passed else f"{n} CDI rates are negative — physically impossible"
        ),
    )


def _cdi_rate_in_daily_range(db: DuckDBWarehouse) -> ValidateResult:
    """Daily CDI >= 1% implies >1000% annualised; likely the source sent annual % instead of daily decimal."""
    n = db.execute("SELECT COUNT(*) FROM raw.cdi_daily WHERE rate >= 0.01").fetchone()[
        0
    ]
    passed = n == 0
    return ValidateResult(
        check_name="cdi_daily_rate_in_daily_range",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0 rows with rate >= 0.01",
        message=(
            None
            if passed
            else f"{n} CDI rates >= 0.01 (likely annual % sent as daily decimal)"
        ),
    )


def _checks_registro_classe(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.registro_classe",
            reference_date,
            name="registro_classe_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.registro_classe",
            name="registro_classe_row_count",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "raw.registro_classe",
            "ID_Registro_Classe",
            name="registro_classe_id_not_null",
            reference_date=reference_date,
        ),
        _registro_classe_cnpj_numeric_le14(db, reference_date),
        check_known_values(
            db,
            "raw.registro_classe",
            "Situacao",
            _KNOWN_SITUACAO,
            name="registro_classe_situacao_known",
            reference_date=reference_date,
        ),
        check_known_values(
            db,
            "raw.registro_classe",
            "Exclusivo",
            _KNOWN_EXCLUSIVO,
            name="registro_classe_exclusivo_yn",
            reference_date=reference_date,
        ),
        check_known_values(
            db,
            "raw.registro_classe",
            "Forma_Condominio",
            _KNOWN_FORMA_CONDOMINIO,
            name="registro_classe_forma_condominio_known",
            reference_date=reference_date,
        ),
        _registro_classe_renda_fixa_coverage(db, reference_date),
        _registro_classe_inception_date_null_rate(db, reference_date),
    ]


def _registro_classe_cnpj_numeric_le14(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """fmt_cnpj strips non-digits and zfills to 14; a value with >14 digits can never produce a valid CNPJ."""
    snap = snapshot_date(db, "raw.registro_classe", reference_date)
    n_bad = (
        db.execute(
            """
            SELECT COUNT(*) FROM raw.registro_classe
            WHERE reference_date = ?
              AND LENGTH(regexp_replace(CNPJ_Classe, '[^0-9]', '', 'g')) > 14
            """,
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n_bad == 0
    return ValidateResult(
        check_name="registro_classe_cnpj_numeric_le14",
        passed=passed,
        severity="error",
        value=str(n_bad),
        threshold="0",
        message=(
            None
            if passed
            else f"{n_bad} CNPJ_Classe values have >14 digits after stripping — zfill(14) cannot produce a valid CNPJ"
        ),
    )


def _registro_classe_renda_fixa_coverage(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Fewer than 1000 Renda Fixa funds at the raw layer likely means Classificacao_Anbima was not populated."""
    snap = snapshot_date(db, "raw.registro_classe", reference_date)
    n = (
        db.execute(
            "SELECT COUNT(*) FROM raw.registro_classe "
            "WHERE reference_date = ? AND Classificacao_Anbima LIKE 'Renda Fixa%'",
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n >= 1000
    return ValidateResult(
        check_name="registro_classe_renda_fixa_coverage",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold=">= 1000",
        message=(
            None
            if passed
            else f"only {n} rows have Classificacao_Anbima starting with 'Renda Fixa' — universe will be near-empty"
        ),
    )


def _registro_classe_inception_date_null_rate(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """High Data_Inicio null rate at the raw layer means staging coerces many funds to null inception_date, silently excluding them from universe."""
    snap = snapshot_date(db, "raw.registro_classe", reference_date)
    if snap is None:
        return ValidateResult(
            check_name="registro_classe_inception_date_null_rate",
            passed=False,
            severity="warning",
            value=None,
            threshold="< 5%",
            message="no snapshot available — cannot check Data_Inicio null rate",
        )
    total, n_null = db.execute(
        "SELECT COUNT(*), SUM(CASE WHEN Data_Inicio IS NULL THEN 1 ELSE 0 END) "
        "FROM raw.registro_classe WHERE reference_date = ?",
        [snap],
    ).fetchone()
    null_rate = (n_null / total) if total > 0 else 0.0
    passed = null_rate < 0.05
    return ValidateResult(
        check_name="registro_classe_inception_date_null_rate",
        passed=passed,
        severity="warning",
        value=f"{null_rate:.1%}",
        threshold="< 5%",
        message=(
            None
            if passed
            else f"{null_rate:.1%} of Data_Inicio is null — those funds will be silently excluded from universe"
        ),
    )


def _checks_registro_subclasse(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.registro_subclasse",
            reference_date,
            name="registro_subclasse_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.registro_subclasse",
            name="registro_subclasse_row_count",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "raw.registro_subclasse",
            "ID_Registro_Classe",
            name="registro_subclasse_merge_key_not_null",
            reference_date=reference_date,
        ),
        check_column_no_nulls(
            db,
            "raw.registro_subclasse",
            "ID_Subclasse",
            name="registro_subclasse_id_not_null",
            reference_date=reference_date,
            severity="warning",
        ),
        _registro_subclasse_referential_integrity(db, reference_date),
        check_known_values(
            db,
            "raw.registro_subclasse",
            "Previdenciario",
            {"S", "N"},
            name="registro_subclasse_previdenciario_yn",
            reference_date=reference_date,
        ),
    ]


def _registro_subclasse_referential_integrity(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Orphaned subclass rows can never join to a class; high orphan rate indicates a structural data problem."""
    snap_sub = snapshot_date(db, "raw.registro_subclasse", reference_date)
    snap_cls = snapshot_date(db, "raw.registro_classe", reference_date)
    if snap_sub is None or snap_cls is None:
        return ValidateResult(
            check_name="registro_subclasse_referential_integrity",
            passed=False,
            severity="warning",
            value=None,
            threshold=">= 95% matched",
            message="cannot check referential integrity without snapshots for both tables",
        )
    total, matched = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN c.ID_Registro_Classe IS NOT NULL THEN 1 ELSE 0 END) AS matched
        FROM raw.registro_subclasse s
        LEFT JOIN raw.registro_classe c
            ON s.ID_Registro_Classe = c.ID_Registro_Classe
            AND c.reference_date = ?
        WHERE s.reference_date = ?
          AND s.ID_Registro_Classe IS NOT NULL
        """,
        [snap_cls, snap_sub],
    ).fetchone()
    pct = matched / total if total > 0 else 1.0
    passed = pct >= 0.95
    return ValidateResult(
        check_name="registro_subclasse_referential_integrity",
        passed=passed,
        severity="warning",
        value=f"{pct:.1%}",
        threshold=">= 95%",
        message=(
            None
            if passed
            else f"only {pct:.1%} of subclass ID_Registro_Classe values match a class row"
        ),
    )


def _checks_cad_fi_hist_taxa_adm(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.cad_fi_hist_taxa_adm",
            reference_date,
            name="taxa_adm_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.cad_fi_hist_taxa_adm",
            name="taxa_adm_row_count",
            reference_date=reference_date,
        ),
        _taxa_adm_no_ambiguous_values(db, reference_date),
    ]


def _taxa_adm_no_ambiguous_values(
    db: DuckDBWarehouse, reference_date: date
) -> ValidateResult:
    """Staging converts TAXA_ADM > 5 as basis points (/100); values in (5, 20) are ambiguous between % and bps."""
    snap = snapshot_date(db, "raw.cad_fi_hist_taxa_adm", reference_date)
    n = (
        db.execute(
            """
            SELECT COUNT(*) FROM raw.cad_fi_hist_taxa_adm
            WHERE reference_date = ?
              AND TAXA_ADM > 5 AND TAXA_ADM < 20
            """,
            [snap],
        ).fetchone()[0]
        if snap is not None
        else 0
    )
    passed = n == 0
    return ValidateResult(
        check_name="taxa_adm_no_ambiguous_values",
        passed=passed,
        severity="warning",
        value=str(n),
        threshold="0",
        message=(
            None
            if passed
            else f"{n} TAXA_ADM values in (5, 20) — ambiguous between % and basis points"
        ),
    )


def _checks_cad_fi_hist_taxa_perfm(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.cad_fi_hist_taxa_perfm",
            reference_date,
            name="taxa_perfm_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.cad_fi_hist_taxa_perfm",
            name="taxa_perfm_row_count",
            reference_date=reference_date,
        ),
    ]


def _checks_extrato_fi(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.extrato_fi",
            reference_date,
            name="extrato_fi_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.extrato_fi",
            name="extrato_fi_row_count",
            reference_date=reference_date,
        ),
        check_known_values(
            db,
            "raw.extrato_fi",
            "EXISTE_TAXA_PERFM",
            {"S", "N"},
            name="extrato_fi_existe_taxa_perfm_yn",
            reference_date=reference_date,
        ),
    ]


def _checks_anbima_caracteristicas(
    db: DuckDBWarehouse, reference_date: date
) -> list[ValidateResult]:
    return [
        check_snapshot_exists(
            db,
            "raw.anbima_caracteristicas",
            reference_date,
            name="anbima_snapshot_exists",
        ),
        check_row_count(
            db,
            "raw.anbima_caracteristicas",
            name="anbima_row_count",
            reference_date=reference_date,
        ),
        check_known_values(
            db,
            "raw.anbima_caracteristicas",
            "Estrutura",
            _KNOWN_ESTRUTURA,
            name="anbima_estrutura_known",
            reference_date=reference_date,
        ),
        check_known_values(
            db,
            "raw.anbima_caracteristicas",
            "Tributacao_Alvo",
            _KNOWN_TRIBUTACAO_ALVO,
            name="anbima_tributacao_alvo_known",
            reference_date=reference_date,
        ),
        _anbima_required_columns_present(db),
    ]


def _anbima_required_columns_present(db: DuckDBWarehouse) -> ValidateResult:
    """ANBIMA occasionally changes column headers; a missing column causes a silent KeyError or NaN column in staging."""
    rows = db.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'anbima_caracteristicas'
        """).fetchall()
    existing = {r[0] for r in rows}
    missing = _ANBIMA_REQUIRED_COLUMNS - existing
    passed = len(missing) == 0
    return ValidateResult(
        check_name="anbima_required_columns_present",
        passed=passed,
        severity="error",
        value=", ".join(sorted(missing)) if missing else None,
        threshold="all required columns present",
        message=(
            None
            if passed
            else f"missing columns in raw.anbima_caracteristicas: {', '.join(sorted(missing))}"
        ),
    )
