"""Command-line interface."""

from __future__ import annotations

import argparse
import logging

from .config import load_config
from .db import create_db_engine
from .logging_utils import setup_logging
from .pipeline import ETLPipeline
from .utils import ensure_directories, parse_run_date


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(description="Operational Data Warehouse ETL Pipeline")
    parser.add_argument("--run-date", help="Run date in YYYY-MM-DD format")
    parser.add_argument(
        "--mode",
        default=None,
        choices=["incremental", "full-refresh"],
        help="Pipeline execution mode",
    )
    parser.add_argument(
        "--source",
        default="all",
        choices=["all", "transactions", "customers", "products", "branches"],
        help="Source scope to process",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Program entry point."""

    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config()
    ensure_directories(
        [
            config.log_file.parent,
            config.rejected_records_dir,
            config.reports_dir,
            config.root_dir / "database",
        ]
    )
    setup_logging(config.log_file)

    run_date = parse_run_date(args.run_date)
    mode = args.mode or config.default_mode

    engine = create_db_engine(config.database_url)
    pipeline = ETLPipeline(config, engine)
    result = pipeline.run(run_date=run_date, mode=mode, source_scope=args.source)
    logging.getLogger(__name__).info(
        "Job %s finished with status=%s, read=%s, loaded=%s, rejected=%s",
        result.job_id,
        result.status,
        result.records_read,
        result.records_loaded,
        result.records_rejected,
    )
    return 0 if result.status == "SUCCESS" else 1
