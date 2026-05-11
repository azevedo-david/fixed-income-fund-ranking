# Fixed Income Fund Ranking

One-command pipeline that sources, scores, and ranks Brazilian CVM-registered
fixed-income funds against a configurable reference date. Designed to run on a
weekly cadence in production.

[Video walkthrough](https://www.youtube.com/watch?v=Rynx-ReAG5g)

## Quick start

```bash
poetry install
poetry run python run.py
```

Output: [`output/ranking.md`](output/ranking.md) — top-5 funds per customer
segment, with the metrics and score that drove each rank.

## How it works

```
CVM Dados Abertos ──┐
BCB SGS (CDI)       ├─► raw → staging → marts → publish → output/
ANBIMA xlsx ────────┘
```

Data flows through four layers stored in a local DuckDB file (`data/fund_ranking.duckdb`):

| Layer | What it holds |
|-------|---------------|
| `raw` | Downloaded files as-is (parquet shadows of CVM ZIPs, BCB JSON, ANBIMA xlsx) |
| `staging` | Cleaned, renamed, typed tables — one row per natural key |
| `marts` | Derived analytical tables: `universe`, `metrics`, `rankings` |
| `logs` | Validation results written after each layer |

The pipeline steps:

| Step | Module | What it does |
|------|--------|--------------|
| 1. Ingest | `src/ingestion/` | Downloads CVM registry, daily quotes, fees, BCB CDI, ANBIMA xlsx |
| 2. Stage | `src/staging/` | Cleans and normalises each source into `staging.*` tables |
| 3. Universe | `src/marts/universe.py` | Filters to active, open-end Renda Fixa funds with AuM ≥ R$15M and ≥ 300 holders |
| 4. Metrics | `src/marts/metrics.py` | Trailing returns, alpha vs CDI, Sharpe, max drawdown, volatility, % months > CDI, CAGR — gross and net of IR |
| 5. Rankings | `src/marts/ranking.py` | Converts each metric to a percentile score, applies purpose × profile weights, filters by investor access |
| 6. Publish | `src/publish/` | Writes `output/ranking.md`, `output/ranking.json`, `output/metrics.parquet` |

## Reproducing from scratch

### Prerequisites

- Python 3.13+
- [Poetry](https://python-poetry.org/docs/#installation)

### ANBIMA data (one-time manual download)

The pipeline uses one public ANBIMA file that must be placed in `data/raw/anbima/`:

| File | Source |
|------|--------|
| `FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx` | [data.anbima.com.br](https://data.anbima.com.br/datasets/fundos-175-caracteristicas-publico/detalhes) → Datasets → Fundos 175 Características → Download |

This file is not auto-fetched because the ANBIMA public portal requires a browser
session. Once placed, the pipeline caches it as Parquet and re-reads from cache on
every subsequent run.

### Install and run

```bash
poetry install

# Run the full pipeline (fetches CVM + BCB data automatically)
poetry run python run.py

# Override the reference date without editing config.yaml
poetry run python run.py --reference-date 2025-12-31

# Re-download all upstream data and recompute
poetry run python run.py --force
```

CVM and BCB data are fetched automatically on the first run and cached under
`data/raw/`. Re-runs use the cache unless `--force` is passed.

## Outputs

All outputs land in `output/` after every run.

| File | Description |
|------|-------------|
| `output/ranking.md` | Top-5 funds per segment + methodology (human-readable) |
| `output/ranking.json` | Same ranking, machine-readable — fixed data contract |
| `output/metrics.parquet` | Full metrics for all eligible funds (40+ columns) |
| `logs/pipeline.log` | Appended log of every run |

`ranking.json` and `metrics.parquet` are designed to be consumed downstream:
upload to S3, insert into a database, or serve as an API response by swapping
the sink in `run.py` — no pipeline code changes required.

## Configuration

All tunable parameters live in [`config.yaml`](config.yaml):

| Key | Description |
|-----|-------------|
| `reference_date` | Ranking reference date — `null` defaults to today |
| `history_start` | Earliest date to fetch from CVM (default `2021-01-01`) |
| `universe.*` | AuM, holder count, span, freshness, and sparseness thresholds |
| `windows` | Trailing return windows (months) |
| `tax` | IR rates by taxation regime and exempt-fund keyword list |
| `scoring` | Feature weights per purpose × profile |
| `rankings` | List of purpose × profile × investor-type combos to produce |
| `top_n` | Number of funds to show per segment (default 5) |

## Production path

The pipeline ships with an Airflow setup under `airflow/`. Four DAGs run in sequence:

```
ingest_dag → staging_dag → marts_dag → publish_dag
```

To run locally with Airflow:

```bash
cd airflow
docker compose -f docker-compose-airflow.yml up
```

For a minimal cron without Airflow:

```
0 8 * * 2  cd /pipeline && poetry run python run.py --force >> output/pipeline.log 2>&1
```

Leave `reference_date: null` in `config.yaml` so the pipeline always ranks
against today's date.

## Project layout

```
run.py                         # pipeline entry point
config.yaml                    # all tunable parameters
src/
├── config.py                  # Settings dataclass, path constants
├── storage.py                 # DuckDBWarehouse (upsert, snapshot, temp_view)
├── schemas.py                 # DDL for all DuckDB tables
├── ingestion/
│   ├── cvm.py                 # CVM Dados Abertos bulk download + parse
│   ├── bcb.py                 # BCB SGS CDI series
│   ├── anbima_xlsx.py         # ANBIMA public xlsx parse + cache
│   ├── ingest.py              # orchestrates all ingestion sources
│   └── _utils.py              # shared HTTP helpers
├── staging/
│   ├── registry.py            # fund registry → staging.registry
│   ├── daily_quotes.py        # inf_diario → staging.daily_quotes
│   ├── fees.py                # cad_fi_hist → staging.fees
│   ├── cdi_rates.py           # BCB CDI → staging.cdi_rates
│   ├── anbima.py              # ANBIMA xlsx → staging.anbima
│   └── stage.py               # orchestrates all staging transforms
├── marts/
│   ├── universe.py            # eligible fund universe
│   ├── metrics.py             # return, risk, and tax-layer metrics
│   ├── ranking.py             # percentile scoring and ranking
│   ├── mart.py                # orchestrates universe → metrics → rankings
│   └── compute/
│       ├── returns.py         # trailing returns, monthly compounding, CAGR
│       ├── risk.py            # volatility, Sharpe, max drawdown
│       └── tax.py             # IR net-return helpers
├── publish/
│   ├── report.py              # markdown report generation
│   └── publish.py             # ranking.json and metrics.parquet writers
└── validation/
    ├── validate_ingestion.py  # row-count and freshness checks on raw tables
    ├── validate_staging.py    # null-rate, range, and join checks on staging
    └── validate_marts.py      # universe size and metric-completeness checks
airflow/
├── dags/
│   ├── ingest_dag.py
│   ├── staging_dag.py
│   ├── marts_dag.py
│   └── publish_dag.py
└── docker-compose-airflow.yml
data/
├── raw/                       # cached downloads (gitignored)
│   ├── cvm/
│   ├── bcb/
│   └── anbima/
└── processed/                 # parsed Parquet caches (gitignored)
output/
├── ranking.md
├── ranking.json
└── metrics.parquet
tests/
├── test_compute.py            # unit tests for returns, risk, and metrics compute layer
├── test_storage.py            # DuckDBWarehouse write-pattern tests
└── test_validation.py         # validation check tests
```
