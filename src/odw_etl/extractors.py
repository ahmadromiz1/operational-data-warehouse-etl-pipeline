"""Source extraction logic."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from .api.mock_api import fetch_branches
from .config import AppConfig


@dataclass(slots=True)
class ExtractResult:
    """In-memory representation of an extracted source."""

    source_name: str
    source_type: str
    source_path: str | None
    frame: pd.DataFrame


def _extract_transactions(config: AppConfig) -> ExtractResult:
    path = config.path_for("transactions_csv")
    frame = pd.read_csv(path)
    return ExtractResult("transactions", "csv", str(path), frame)


def _extract_customers(config: AppConfig) -> ExtractResult:
    path = config.path_for("customers_excel")
    frame = pd.read_excel(path)
    return ExtractResult("customers", "excel", str(path), frame)


def _extract_products(config: AppConfig) -> ExtractResult:
    path = config.path_for("products_json")
    frame = pd.read_json(path)
    return ExtractResult("products", "json", str(path), frame)


def _extract_branches(_: AppConfig) -> ExtractResult:
    frame = pd.DataFrame(fetch_branches())
    return ExtractResult("branches", "mock_api", "mock_api://branches", frame)


EXTRACTORS: dict[str, Callable[[AppConfig], ExtractResult]] = {
    "transactions": _extract_transactions,
    "customers": _extract_customers,
    "products": _extract_products,
    "branches": _extract_branches,
}


def extract_sources(config: AppConfig, source_scope: str) -> dict[str, ExtractResult]:
    """Extract one or all sources based on the CLI selection."""

    selected = EXTRACTORS.keys() if source_scope == "all" else [source_scope]
    return {name: EXTRACTORS[name](config) for name in selected}

