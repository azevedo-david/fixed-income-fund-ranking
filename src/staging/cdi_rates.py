"""BCB CDI daily rates: raw.cdi_daily → staging-ready DataFrame."""

from __future__ import annotations

import pandas as pd

from ..storage import DuckDBWarehouse


def fetch_raw_cdi_rates(
    db: DuckDBWarehouse, force: bool = False
) -> pd.DataFrame | None:
    """Return CDI rates not yet in staging, or all rates when ``force`` is True."""
    last_date = None if force else db.get_max_date("staging", "cdi_rates", "date")
    if last_date is not None:
        raw = db.execute(
            "SELECT date, rate FROM raw.cdi_daily WHERE date > ?", [last_date]
        ).df()
    else:
        raw = db.execute("SELECT date, rate FROM raw.cdi_daily").df()
    return raw if not raw.empty else None
