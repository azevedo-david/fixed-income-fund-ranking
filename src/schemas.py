"""DDL constants for every DuckDB table; DuckDBWarehouse executes these on init so schemas are enforced, not inferred."""

RAW_REGISTRO_CLASSE = """\
CREATE TABLE IF NOT EXISTS raw.registro_classe (
    ID_Registro_Fundo                        BIGINT,
    ID_Registro_Classe                       BIGINT,
    CNPJ_Classe                              VARCHAR NOT NULL,
    Codigo_CVM                               BIGINT,
    Data_Registro                            DATE,
    Data_Constituicao                        DATE,
    Data_Inicio                              DATE,
    Tipo_Classe                              VARCHAR,
    Denominacao_Social                       VARCHAR,
    Situacao                                 VARCHAR,
    Data_Inicio_Situacao                     DATE,
    Classificacao                            VARCHAR,
    Indicador_Desempenho                     VARCHAR,
    Classe_Cotas                             VARCHAR,
    Classificacao_Anbima                     VARCHAR,
    Tributacao_Longo_Prazo                   VARCHAR,
    Entidade_Investimento                    VARCHAR,
    Permitido_Aplicacao_CemPorCento_Exterior VARCHAR,
    Classe_ESG                               VARCHAR,
    Forma_Condominio                         VARCHAR,
    Exclusivo                                VARCHAR,
    Publico_Alvo                             VARCHAR,
    Patrimonio_Liquido                       DOUBLE,
    Data_Patrimonio_Liquido                  DATE,
    CNPJ_Auditor                             DOUBLE,
    Auditor                                  VARCHAR,
    CNPJ_Custodiante                         DOUBLE,
    Custodiante                              VARCHAR,
    CNPJ_Controlador                         DOUBLE,
    Controlador                              VARCHAR,
    downloaded_at                            DATE NOT NULL
)"""

RAW_REGISTRO_SUBCLASSE = """\
CREATE TABLE IF NOT EXISTS raw.registro_subclasse (
    ID_Registro_Classe                  BIGINT,
    ID_Subclasse                        VARCHAR,
    Codigo_CVM                          DOUBLE,
    Data_Constituicao                   DATE,
    Data_Inicio                         DATE,
    Denominacao_Social                  VARCHAR,
    Situacao                            VARCHAR,
    Data_Inicio_Situacao                DATE,
    Forma_Condominio                    VARCHAR,
    Exclusivo                           VARCHAR,
    Publico_Alvo                        VARCHAR,
    Previdenciario                      VARCHAR,
    Exclusivo_INR                       VARCHAR,
    Exclusivo_Previdencia_Complementar  VARCHAR,
    downloaded_at                       DATE NOT NULL
)"""

RAW_INF_DIARIO = """\
CREATE TABLE IF NOT EXISTS raw.inf_diario (
    TP_FUNDO_CLASSE   VARCHAR,
    CNPJ_FUNDO_CLASSE VARCHAR NOT NULL,
    ID_SUBCLASSE      VARCHAR,
    DT_COMPTC         DATE    NOT NULL,
    VL_QUOTA          DOUBLE,
    VL_PATRIM_LIQ     DOUBLE,
    CAPTC_DIA         DOUBLE,
    RESG_DIA          DOUBLE,
    NR_COTST          INTEGER
)"""

RAW_CAD_FI_HIST_TAXA_ADM = """\
CREATE TABLE IF NOT EXISTS raw.cad_fi_hist_taxa_adm (
    CNPJ_FUNDO        VARCHAR NOT NULL,
    DT_REG            DATE,
    TAXA_ADM          DOUBLE,
    INF_TAXA_ADM      VARCHAR,
    DT_INI_TAXA_ADM   DATE,
    downloaded_at     DATE NOT NULL
)"""

RAW_CAD_FI_HIST_TAXA_PERFM = """\
CREATE TABLE IF NOT EXISTS raw.cad_fi_hist_taxa_perfm (
    CNPJ_FUNDO          VARCHAR NOT NULL,
    DT_REG              DATE,
    VL_TAXA_PERFM       DOUBLE,
    DS_TAXA_PERFM       VARCHAR,
    DT_INI_TAXA_PERFM   DATE,
    downloaded_at       DATE NOT NULL
)"""

RAW_EXTRATO_FI = """\
CREATE TABLE IF NOT EXISTS raw.extrato_fi (
    CNPJ_FUNDO_CLASSE   VARCHAR NOT NULL,
    DT_COMPTC           DATE,
    TAXA_ADM            DOUBLE,
    EXISTE_TAXA_PERFM   VARCHAR,
    downloaded_at       DATE NOT NULL
)"""

# ANBIMA xlsx has Portuguese column names with accents — stored as-is from the source file.
RAW_ANBIMA_CARACTERISTICAS = """\
CREATE TABLE IF NOT EXISTS raw.anbima_caracteristicas (
    "CNPJ da Classe"                  VARCHAR NOT NULL,
    "Código ANBIMA"                   VARCHAR,
    "Estrutura"                       VARCHAR,
    "Nome Comercial"                  VARCHAR,
    "Categoria ANBIMA"                VARCHAR,
    "Tipo ANBIMA"                     VARCHAR,
    "Nível 1 Categoria"               VARCHAR,
    "Nível 2 Categoria"               VARCHAR,
    "Nível 3 Subcategoria"            VARCHAR,
    "Foco Atuação"                    VARCHAR,
    "Composição do Fundo"             VARCHAR,
    "Aberto Estatutariamente"         VARCHAR,
    "Fundo ESG"                       VARCHAR,
    "Tributação Alvo"                 VARCHAR,
    "Administrador"                   VARCHAR,
    "Gestor Principal"                VARCHAR,
    "Tipo de Investidor"              VARCHAR,
    "Característica do Investidor"    VARCHAR,
    "Aplicação Inicial Mínima"        DOUBLE,
    "Cota de Abertura"                VARCHAR,
    "Prazo Pagamento Resgate em dias" INTEGER,
    "Código CVM Subclasse"            VARCHAR,
    downloaded_at                     DATE NOT NULL
)"""

RAW_CDI_DAILY = """\
CREATE TABLE IF NOT EXISTS raw.cdi_daily (
    date  DATE   NOT NULL,
    rate  DOUBLE NOT NULL
)"""

RAW_PIPELINE_RUNS = """\
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    reference_date  DATE        NOT NULL,
    task            VARCHAR     NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    status          VARCHAR,
    rows_written    BIGINT,
    notes           VARCHAR,
    PRIMARY KEY (reference_date, task)
)"""

STAGING_REGISTRO = """\
CREATE TABLE IF NOT EXISTS staging.registro (
    fund_cnpj        VARCHAR NOT NULL,
    subclass_id      VARCHAR,
    fund_name        VARCHAR,
    inception_date   DATE,
    status           VARCHAR,
    anbima_category  VARCHAR,
    target_investor  VARCHAR,
    share_class      VARCHAR,
    fund_structure   VARCHAR,
    is_exclusive     VARCHAR,
    is_pension       VARCHAR,
    reference_date   DATE    NOT NULL
)"""

STAGING_INF_DIARIO = """\
CREATE TABLE IF NOT EXISTS staging.inf_diario (
    fund_cnpj      VARCHAR NOT NULL,
    subclass_id    VARCHAR,
    date           DATE    NOT NULL,
    nav            DOUBLE,
    aum            DOUBLE,
    inflows        DOUBLE,
    outflows       DOUBLE,
    shareholders   INTEGER,
    reference_date DATE    NOT NULL
)"""

STAGING_FEES = """\
CREATE TABLE IF NOT EXISTS staging.fees (
    fund_cnpj      VARCHAR NOT NULL,
    adm_fee        DOUBLE,
    adm_fee_date   DATE,
    perf_fee       DOUBLE,
    perf_fee_desc  VARCHAR,
    perf_fee_date  DATE,
    has_perf_fee   BOOLEAN,
    reference_date DATE    NOT NULL
)"""

STAGING_CDI_DAILY = """\
CREATE TABLE IF NOT EXISTS staging.cdi_daily (
    date           DATE   NOT NULL,
    rate           DOUBLE NOT NULL,
    reference_date DATE   NOT NULL
)"""

STAGING_ANBIMA = """\
CREATE TABLE IF NOT EXISTS staging.anbima (
    fund_cnpj              VARCHAR NOT NULL,
    subclass_id            VARCHAR,
    anbima_code            VARCHAR,
    structure              VARCHAR,
    commercial_name        VARCHAR,
    category               VARCHAR,
    type                   VARCHAR,
    level_1                VARCHAR,
    level_2                VARCHAR,
    level_3                VARCHAR,
    focus                  VARCHAR,
    composition            VARCHAR,
    open_to_public         VARCHAR,
    is_esg                 VARCHAR,
    target_taxation        VARCHAR,
    administrator          VARCHAR,
    lead_manager           VARCHAR,
    investor_type          VARCHAR,
    investor_profile       VARCHAR,
    min_initial_investment DOUBLE,
    open_nav_quota         VARCHAR,
    redemption_days        INTEGER,
    reference_date         DATE    NOT NULL
)"""

MARTS_UNIVERSE = """\
CREATE TABLE IF NOT EXISTS marts.universe (
    CNPJ_FUNDO_CLASSE VARCHAR NOT NULL,
    ID_SUBCLASSE      VARCHAR,
    fund_name         VARCHAR,
    reference_date    DATE    NOT NULL
)"""

MARTS_METRICS = """\
CREATE TABLE IF NOT EXISTS marts.metrics (
    CNPJ_FUNDO_CLASSE        VARCHAR NOT NULL,
    ID_SUBCLASSE             VARCHAR,
    return_annualized_net    DOUBLE,
    alpha_12m_net            DOUBLE,
    alpha_3m_net             DOUBLE,
    alpha_6m_net             DOUBLE,
    alpha_24m_net            DOUBLE,
    alpha_36m_net            DOUBLE,
    return_12m_net           DOUBLE,
    sharpe_excess            DOUBLE,
    pct_months_above_cdi     DOUBLE,
    max_drawdown             DOUBLE,
    volatility               DOUBLE,
    redemption_days          INTEGER,
    reference_date           DATE    NOT NULL
)"""

MARTS_RANKINGS = """\
CREATE TABLE IF NOT EXISTS marts.rankings (
    CNPJ_FUNDO_CLASSE VARCHAR NOT NULL,
    ID_SUBCLASSE      VARCHAR,
    purpose           VARCHAR NOT NULL,
    profile           VARCHAR NOT NULL,
    investor_type     VARCHAR NOT NULL,
    rank              INTEGER NOT NULL,
    score             DOUBLE,
    reference_date    DATE    NOT NULL
)"""

VALIDATION_LOG = """\
CREATE TABLE IF NOT EXISTS logs.validation_log (
    reference_date  DATE        NOT NULL,
    task            VARCHAR     NOT NULL,
    dataset         VARCHAR     NOT NULL,
    check_name      VARCHAR     NOT NULL,
    severity        VARCHAR     NOT NULL,
    passed          BOOLEAN     NOT NULL,
    value           VARCHAR,
    threshold       VARCHAR,
    message         VARCHAR,
    logged_at       TIMESTAMPTZ DEFAULT current_timestamp
)"""

ALL_DDLS: list[str] = [
    RAW_REGISTRO_CLASSE,
    RAW_REGISTRO_SUBCLASSE,
    RAW_INF_DIARIO,
    RAW_CAD_FI_HIST_TAXA_ADM,
    RAW_CAD_FI_HIST_TAXA_PERFM,
    RAW_EXTRATO_FI,
    RAW_ANBIMA_CARACTERISTICAS,
    RAW_CDI_DAILY,
    RAW_PIPELINE_RUNS,
    STAGING_REGISTRO,
    STAGING_INF_DIARIO,
    STAGING_FEES,
    STAGING_CDI_DAILY,
    STAGING_ANBIMA,
    MARTS_UNIVERSE,
    MARTS_METRICS,
    MARTS_RANKINGS,
    VALIDATION_LOG,
]
