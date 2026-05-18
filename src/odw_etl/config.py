"""Configuration loading utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class AppConfig:
    """Typed wrapper around the YAML configuration."""

    raw: dict[str, Any]
    root_dir: Path

    @property
    def database_url(self) -> str:
        return str(self.raw["database"]["url"])

    @property
    def log_file(self) -> Path:
        return self.root_dir / self.raw["paths"]["log_file"]

    @property
    def rejected_records_dir(self) -> Path:
        return self.root_dir / self.raw["paths"]["rejected_records_dir"]

    @property
    def reports_dir(self) -> Path:
        return self.root_dir / self.raw["paths"]["reports_dir"]

    def path_for(self, key: str) -> Path:
        return self.root_dir / self.raw["paths"][key]

    @property
    def allowed_sources(self) -> list[str]:
        return list(self.raw["pipeline"]["allowed_sources"])

    @property
    def default_mode(self) -> str:
        return str(self.raw["pipeline"]["default_mode"])

    @property
    def required_transaction_fields(self) -> list[str]:
        return list(self.raw["validation"]["required_transaction_fields"])


def load_config(config_path: str | Path = "config/pipeline.yaml") -> AppConfig:
    """Load the project YAML configuration."""

    path = Path(config_path)
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return AppConfig(raw=raw, root_dir=path.resolve().parent.parent)

