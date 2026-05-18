"""Integration tests for the ETL pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from odw_etl.config import load_config
from odw_etl.db import create_db_engine, read_table
from odw_etl.pipeline import ETLPipeline


def test_full_refresh_pipeline_builds_warehouse_and_marts(tmp_path: Path) -> None:
    """The pipeline should load dimensions, facts, marts, and rejected outputs."""

    config = load_config()
    config.raw["database"]["url"] = f"sqlite:///{tmp_path / 'warehouse.db'}"
    config.raw["paths"]["rejected_records_dir"] = str(tmp_path / "rejected")
    config.raw["paths"]["reports_dir"] = str(tmp_path / "reports")
    config.raw["paths"]["log_file"] = str(tmp_path / "etl.log")

    engine = create_db_engine(config.database_url)
    pipeline = ETLPipeline(config, engine)
    result = pipeline.run(run_date=date(2026, 5, 18), mode="full-refresh", source_scope="all")

    assert result.status == "SUCCESS"

    fact = read_table(engine, "SELECT * FROM fact_transactions")
    daily = read_table(engine, "SELECT * FROM mart_daily_sales")
    recon = read_table(engine, "SELECT * FROM mart_reconciliation_summary")
    rejected = read_table(engine, "SELECT * FROM rejected_records")

    assert len(fact) == 5
    assert len(daily) == 3
    assert len(recon) == 1
    assert len(rejected) == 7

    reports_dir = config.reports_dir
    assert (reports_dir / "mart_daily_sales.csv").exists()
    assert (reports_dir / "mart_branch_performance.csv").exists()
    assert (reports_dir / "mart_product_performance.csv").exists()
    assert (reports_dir / "mart_reconciliation_summary.csv").exists()

