"""General helper utilities."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_directories(paths: list[Path]) -> None:
    """Create directories when missing."""

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def parse_run_date(value: str | None) -> date:
    """Parse a run date string or default to today."""

    if value is None:
        return datetime.utcnow().date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def json_dumps(value: Any) -> str:
    """Serialize values safely for storage."""

    return json.dumps(value, ensure_ascii=True, default=str)


def clean_text_series(series: pd.Series) -> pd.Series:
    """Normalize text values by trimming and collapsing nulls."""

    return series.astype("string").str.strip()
