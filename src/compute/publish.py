"""Structured ranking output: typed data contract, payload builder, and sink abstraction."""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypedDict

logger = logging.getLogger(__name__)

import pandas as pd

from ..config import Settings
from .ranking import rank_funds

SCHEMA_VERSION = "1.1"


class FundEntry(TypedDict):
    rank: int
    cnpj: str
    subclass_id: str | None
    fund_name: str
    return_annualized_net: float | None
    alpha_12m_net: float | None
    return_12m_net: float | None
    sharpe_excess: float | None
    pct_months_above_cdi: float | None
    max_drawdown: float | None
    redemption_days: int | None
    volatility: float | None


class SegmentResult(TypedDict):
    purpose: str
    profile: str
    investor_type: str
    eligible_funds: int
    funds: list[FundEntry]


class RankingPayload(TypedDict):
    schema_version: str
    generated_at: str  # ISO-8601
    reference_date: str  # YYYY-MM-DD
    universe_size: int
    segments: list[SegmentResult]


Sink = Callable[[RankingPayload], None]


def _coerce(v: Any) -> Any:
    """NaN → None; keep everything else as-is."""
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _fund_entry(rank: int, cnpj: str, subclass_id: Any, row: pd.Series) -> FundEntry:
    def g(col: str) -> Any:
        return _coerce(row.get(col))

    rd = g("redemption_days")
    return FundEntry(
        rank=rank,
        cnpj=cnpj,
        subclass_id=None if pd.isna(subclass_id) else str(subclass_id),
        fund_name=str(g("fund_name") or ""),
        return_annualized_net=g("return_annualized_net"),
        alpha_12m_net=g("alpha_12m_net"),
        return_12m_net=g("return_12m_net"),
        sharpe_excess=g("sharpe_excess"),
        pct_months_above_cdi=g("pct_months_above_cdi"),
        max_drawdown=g("max_drawdown"),
        redemption_days=None if rd is None else int(rd),
        volatility=g("volatility"),
    )


def build_payload(metrics_df: pd.DataFrame, settings: Settings) -> RankingPayload:
    """Build the canonical ``RankingPayload`` from ``metrics_df``.

    This is the single authoritative representation of the ranking result.
    All sinks consume this dict — never raw DataFrames.
    """
    segments: list[SegmentResult] = []

    for combo in settings.rankings:
        ranked = rank_funds(
            metrics_df, combo.purpose, settings, combo.profile, combo.investor_type
        )
        funds: list[FundEntry] = []
        for rank_i, (idx, row) in enumerate(ranked.head(settings.top_n).iterrows(), 1):
            cnpj, subclass_id = idx
            funds.append(_fund_entry(rank_i, cnpj, subclass_id, row))

        segments.append(
            SegmentResult(
                purpose=combo.purpose,
                profile=combo.profile,
                investor_type=combo.investor_type,
                eligible_funds=len(ranked),
                funds=funds,
            )
        )

    return RankingPayload(
        schema_version=SCHEMA_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat(),
        reference_date=str(settings.reference_date),
        universe_size=len(metrics_df),
        segments=segments,
    )


def local_json_sink(path: Path) -> Sink:
    """Sink that writes the payload as indented UTF-8 JSON to ``path``."""

    def _sink(payload: RankingPayload) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        logger.info("ranking.json written (%d segments)", len(payload["segments"]))

    return _sink


def publish(payload: RankingPayload, sinks: list[Sink]) -> None:
    """Deliver ``payload`` to every registered sink in order."""
    for sink in sinks:
        sink(payload)
