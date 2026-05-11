"""Typed project settings loaded from config.yaml; all paths and URLs live here."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"

CVM_BASE_URL = "https://dados.cvm.gov.br/dados/FI"
BCB_SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


@dataclass(frozen=True)
class UniverseConfig:
    min_span_days: int
    aum_lookback_days: int
    min_aum: float
    min_cotistas: int
    max_quote_staleness_days: int
    min_obs_ratio: float


@dataclass(frozen=True)
class TaxConfig:
    cdi_ir_rate: float
    rates_by_taxation: dict[str, float]
    default_rate: float
    exempt_keywords: list[str]


@dataclass(frozen=True)
class BcbConfig:
    serie_cdi: int
    url: str
    timeout_seconds: int


@dataclass(frozen=True)
class FeatureSpec:
    col: str
    ascending: bool


@dataclass(frozen=True)
class ScoringConfig:
    cont_features: list[FeatureSpec]
    span_lambda: float
    liquidity_lambda: dict[str, float]
    accessibility_scale: float
    accessibility_weight: float
    weights: dict[str, dict[str, list[float]]]


@dataclass(frozen=True)
class RankingCombo:
    purpose: str
    profile: str
    investor_type: str


@dataclass(frozen=True)
class OutputConfig:
    ranking_md: Path
    ranking_json: Path
    metrics_parquet: Path


@dataclass(frozen=True)
class Settings:
    history_start: date
    reference_date: date
    force_download: bool
    universe: UniverseConfig
    windows: dict[str, int]
    tax: TaxConfig
    bcb: BcbConfig
    scoring: ScoringConfig
    rankings: list[RankingCombo]
    top_n: int
    output: OutputConfig
    db_path: Path = PROJECT_ROOT / "data" / "fund_ranking.duckdb"

    @property
    def max_window_months(self) -> int:
        """Largest trailing window — defines how much quote history to download."""
        return max(self.windows.values())

    @property
    def quotes_end(self) -> date:
        return self.reference_date

    @property
    def quotes_start(self) -> date:
        """reference_date minus max(windows) months."""
        return self.reference_date - relativedelta(months=self.max_window_months)

    @property
    def aum_lookback_months(self) -> list[str]:
        """YYYYMM strings for every month in the AuM lookback window, inclusive."""
        start = self.reference_date - relativedelta(
            days=self.universe.aum_lookback_days
        )
        months: list[str] = []
        cursor = date(start.year, start.month, 1)
        last = date(self.reference_date.year, self.reference_date.month, 1)
        while cursor <= last:
            months.append(cursor.strftime("%Y%m"))
            cursor += relativedelta(months=1)
        return months

    @classmethod
    def from_yaml(cls, path: Path = DEFAULT_CONFIG_PATH) -> "Settings":
        with open(path) as f:
            cfg: dict[str, Any] = yaml.safe_load(f)

        universe_raw = cfg["universe"]
        universe = UniverseConfig(**universe_raw)
        if universe.aum_lookback_days <= 0:
            raise ValueError(
                f"universe.aum_lookback_days must be positive, got {universe.aum_lookback_days}"
            )

        windows = {k: int(v) for k, v in cfg["windows"].items()}
        if not windows:
            raise ValueError("windows dict must not be empty")

        scoring_raw = cfg["scoring"]
        scoring = ScoringConfig(
            cont_features=[FeatureSpec(**f) for f in scoring_raw["cont_features"]],
            span_lambda=scoring_raw["span_lambda"],
            liquidity_lambda=scoring_raw["liquidity_lambda"],
            accessibility_scale=scoring_raw["accessibility_scale"],
            accessibility_weight=scoring_raw["accessibility_weight"],
            weights=scoring_raw["weights"],
        )

        for purpose, profiles in scoring.weights.items():
            for profile, vec in profiles.items():
                total = sum(vec)
                if abs(total - 1.0) > 1e-9:
                    raise ValueError(
                        f"weights[{purpose}][{profile}] must sum to 1.0; got {total}"
                    )

        output_raw = cfg["output"]
        output = OutputConfig(
            ranking_md=PROJECT_ROOT / output_raw["ranking_md"],
            ranking_json=PROJECT_ROOT / output_raw["ranking_json"],
            metrics_parquet=PROJECT_ROOT / output_raw["metrics_parquet"],
        )

        raw_ref = cfg.get("reference_date")
        reference_date = (
            _parse_date(raw_ref) if raw_ref else date.today() - timedelta(days=1)
        )

        return cls(
            history_start=_parse_date(cfg.get("history_start", "2021-01-01")),
            reference_date=reference_date,
            force_download=bool(cfg.get("force_download", False)),
            universe=universe,
            windows=windows,
            tax=TaxConfig(
                cdi_ir_rate=cfg["tax"]["cdi_ir_rate"],
                rates_by_taxation=cfg["tax"]["rates_by_taxation"],
                default_rate=cfg["tax"]["default_rate"],
                exempt_keywords=cfg["tax"]["exempt_keywords"],
            ),
            bcb=BcbConfig(**cfg["bcb"]),
            scoring=scoring,
            rankings=[RankingCombo(**r) for r in cfg["rankings"]],
            top_n=int(cfg["top_n"]),
            output=output,
        )


def _parse_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def ensure_dirs() -> None:
    for d in (DATA_DIR, RAW_DIR, PROCESSED_DIR, OUTPUT_DIR):
        d.mkdir(parents=True, exist_ok=True)
