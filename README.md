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
BCB SGS (CDI)       ├─► universe ─► metrics ─► score & rank ─► output/
ANBIMA xlsx ────────┘
```

| Step | Module | What it does |
|------|--------|--------------|
| 1. Universe | `src/compute/universe.py` | Filters CVM registry to active, open-end Renda Fixa funds with AuM ≥ R$15M and ≥ 300 holders |
| 2. Metrics | `src/compute/metrics.py` | Computes trailing returns, alpha vs CDI, Sharpe, max drawdown, volatility, % months > CDI, CAGR — gross and net of IR |
| 3. Score & rank | `src/compute/ranking.py` | Converts each metric to a percentile score, applies purpose × profile weights, filters by investor access |
| 4. Report | `src/compute/report.py` | Writes `output/ranking.md` with methodology and top-N tables |
| 5. Publish | `src/compute/publish.py` | Serialises the ranking to `output/ranking.json` via a pluggable sink interface |

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

# Re-download all upstream data (CVM inf_diario + BCB CDI) and recompute
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
| `output/metrics.parquet` | Full metrics for all eligible funds (all 40+ columns) |
| `logs/pipeline.log` | Appended log of every run |

`ranking.json` and `metrics.parquet` are designed to be consumed downstream:
upload to S3, insert into a database, or serve as an API response by swapping
the sink in `run.py` — no pipeline code changes required.

## Configuration

All tunable parameters live in [`config.yaml`](config.yaml):

| Key | Description |
|-----|-------------|
| `reference_date` | Ranking reference date — `null` defaults to today |
| `universe.*` | AuM, holder, and span-history thresholds |
| `windows` | Trailing return windows (months) |
| `tax` | IR rates by taxation regime |
| `scoring` | Feature weights per purpose × profile |
| `rankings` | List of purpose × profile × investor-type combos to produce |
| `top_n` | Number of funds to show per segment (default 5) |

## Production path

The pipeline is stateless and config-driven. A minimal weekly cron:

```
0 8 * * 2  cd /pipeline && poetry run python run.py --force >> output/pipeline.log 2>&1
```

For a fully automated daily run with no human in the loop:

1. Schedule the cron above (runs after CVM publishes monthly inf_diario files).
2. Refresh the ANBIMA xlsx manually (see Prerequisites above) before each run.
3. Leave `reference_date: null` in `config.yaml` so the pipeline always ranks
   against today's date.

## Project layout

```
run.py                   # pipeline entry point
config.yaml              # all tunable parameters
src/
├── config.py            # Settings dataclass, path constants
├── compute/
│   ├── universe.py      # fund universe construction
│   ├── metrics.py       # return, risk, and tax-layer metrics
│   ├── returns.py       # trailing returns, monthly compounding, CAGR
│   ├── risk.py          # volatility, Sharpe, max drawdown
│   ├── ranking.py       # percentile scoring and ranking
│   ├── report.py        # markdown report generation
│   └── publish.py       # data contract + sink abstraction (JSON, S3, DB, …)
└── ingestion/
    ├── cvm.py           # CVM Dados Abertos bulk download + parse
    ├── bcb.py           # BCB SGS CDI series
    ├── anbima_xlsx.py   # ANBIMA public xlsx parse + cache
    └── _utils.py        # shared HTTP helpers
data/
├── raw/                 # cached downloads (gitignored)
│   ├── cvm/             # CVM inf_diario_fi ZIPs
│   ├── bcb/             # BCB CDI JSON
│   └── anbima/          # ANBIMA xlsx (manual download)
└── processed/           # parsed Parquet caches (gitignored)
output/
├── ranking.md           # top-5 ranking report
├── ranking.json         # machine-readable ranking
└── metrics.parquet      # full metrics table
logs/
└── pipeline.log         # appended log of every run
```