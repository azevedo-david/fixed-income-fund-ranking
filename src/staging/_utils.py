"""Shared staging helpers."""

from __future__ import annotations


def fmt_cnpj(value: str | int) -> str:
    """Format any CNPJ value (raw digits, integer, or already-formatted) to XX.XXX.XXX/XXXX-XX."""
    d = "".join(c for c in str(value) if c.isdigit()).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
