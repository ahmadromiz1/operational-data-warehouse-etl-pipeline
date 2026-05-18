"""Pipeline orchestration."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import text

from .config import AppConfig
from .db import begin, initialize_database, read_table, truncate_table, write_dataframe
from .extractors import ExtractResult, extract_sources
from .transformers import (
    build_dim_date,
    build_fact_transactions,
    build_mart_branch_performance,
    build_mart_daily_sales,
    build_mart_product_performance,
    build_mart_reconciliation_summary,
)
from .utils import ensure_directories
from .validators import build_issue_rows, build_rejected_rows, normalize_master_data, validate_transactions


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineResult:
    """Summarize a pipeline run."""

    job_id: str
    status: str
    records_read: int
    records_loaded: int
    records_rejected: int
    message: str


class ETLPipeline:
    """End-to-end ETL pipeline controller."""

    def __init__(self, config: AppConfig, engine) -> None:
        self.config = config
        self.engine = engine
        ensure_directories(
            [
                self.config.rejected_records_dir,
                self.config.reports_dir,
                self.config.log_file.parent,
                self.config.root_dir / "database",
            ]
        )
        initialize_database(engine)

    def run(self, run_date: date, mode: str, source_scope: str) -> PipelineResult:
        """Execute the ETL pipeline."""

        job_id = f"job-{uuid.uuid4().hex[:12]}"
        started_at = datetime.now(UTC).replace(tzinfo=None)
        self._insert_job_log(job_id, run_date, mode, source_scope, "RUNNING", started_at)
        records_read = 0
        records_loaded = 0
        records_rejected = 0

        try:
            if mode == "full-refresh":
                self._full_refresh()

            extracts = extract_sources(self.config, source_scope)
            records_read = sum(len(result.frame) for result in extracts.values())
            self._log_raw_extracts(job_id, run_date, mode, extracts)
            staged = self._load_staging(job_id, run_date, mode, extracts)
            records_loaded = sum(len(frame) for frame in staged.values())

            if source_scope in {"all", "transactions", "customers", "products", "branches"}:
                rejected_count = self._build_warehouse_and_marts(
                    job_id=job_id,
                    run_date=run_date,
                    mode=mode,
                    staged=staged,
                )
                records_rejected += rejected_count

            finished_at = datetime.now(UTC).replace(tzinfo=None)
            self._update_job_log(
                job_id,
                "SUCCESS",
                finished_at,
                records_read,
                records_loaded,
                records_rejected,
                "Pipeline completed successfully.",
            )
            return PipelineResult(job_id, "SUCCESS", records_read, records_loaded, records_rejected, "Pipeline completed successfully.")
        except Exception as exc:
            LOGGER.exception("Pipeline failed")
            finished_at = datetime.now(UTC).replace(tzinfo=None)
            self._update_job_log(
                job_id,
                "FAILED",
                finished_at,
                records_read,
                records_loaded,
                records_rejected,
                str(exc),
            )
            return PipelineResult(job_id, "FAILED", records_read, records_loaded, records_rejected, str(exc))

    def _full_refresh(self) -> None:
        for table_name in [
            "stg_transactions",
            "stg_customers",
            "stg_products",
            "stg_branches",
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_branch",
            "fact_transactions",
            "mart_daily_sales",
            "mart_branch_performance",
            "mart_product_performance",
            "mart_reconciliation_summary",
            "validation_errors",
            "rejected_records",
            "raw_file_logs",
        ]:
            truncate_table(self.engine, table_name)

    def _last_successful_run_date(self) -> date | None:
        query = """
        SELECT run_date
        FROM etl_job_logs
        WHERE status = 'SUCCESS'
        ORDER BY run_date DESC, started_at DESC
        LIMIT 1
        """
        frame = read_table(self.engine, query)
        if frame.empty:
            return None
        return pd.to_datetime(frame.loc[0, "run_date"]).date()

    def _log_raw_extracts(
        self,
        job_id: str,
        run_date: date,
        mode: str,
        extracts: dict[str, ExtractResult],
    ) -> None:
        now = datetime.now(UTC).replace(tzinfo=None)
        rows = [
            {
                "job_id": job_id,
                "source_name": item.source_name,
                "source_type": item.source_type,
                "source_path": item.source_path,
                "record_count": len(item.frame),
                "load_mode": mode,
                "run_date": run_date,
                "loaded_at": now,
            }
            for item in extracts.values()
        ]
        write_dataframe(self.engine, "raw_file_logs", pd.DataFrame(rows))

    def _load_staging(
        self,
        job_id: str,
        run_date: date,
        mode: str,
        extracts: dict[str, ExtractResult],
    ) -> dict[str, pd.DataFrame]:
        staged: dict[str, pd.DataFrame] = {}
        last_run_date = self._last_successful_run_date() if mode == "incremental" else None

        if "customers" in extracts:
            customers = normalize_master_data(extracts["customers"].frame, "customer_id")
            customers["job_id"] = job_id
            truncate_table(self.engine, "stg_customers")
            write_dataframe(self.engine, "stg_customers", customers)
            staged["customers"] = customers

        if "products" in extracts:
            products = normalize_master_data(extracts["products"].frame, "product_code")
            products["job_id"] = job_id
            truncate_table(self.engine, "stg_products")
            write_dataframe(self.engine, "stg_products", products)
            staged["products"] = products

        if "branches" in extracts:
            branches = normalize_master_data(extracts["branches"].frame, "branch_code")
            branches["job_id"] = job_id
            truncate_table(self.engine, "stg_branches")
            write_dataframe(self.engine, "stg_branches", branches)
            staged["branches"] = branches

        if "transactions" in extracts:
            transactions = extracts["transactions"].frame.copy()
            transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"], errors="coerce")
            if last_run_date is not None:
                transactions = transactions[
                    (transactions["transaction_date"].dt.date > last_run_date)
                    & (transactions["transaction_date"].dt.date <= run_date)
                ]
            else:
                transactions = transactions[transactions["transaction_date"].dt.date <= run_date]
            transactions["transaction_date"] = transactions["transaction_date"].dt.strftime("%Y-%m-%d")
            staged["transactions"] = transactions

        return staged

    def _build_warehouse_and_marts(
        self,
        job_id: str,
        run_date: date,
        mode: str,
        staged: dict[str, pd.DataFrame],
    ) -> int:
        stg_customers = read_table(self.engine, "SELECT * FROM stg_customers")
        stg_products = read_table(self.engine, "SELECT * FROM stg_products")
        stg_branches = read_table(self.engine, "SELECT * FROM stg_branches")

        transactions_source = staged.get("transactions", pd.DataFrame())
        if transactions_source.empty:
            self._refresh_dimensions_and_marts(job_id, run_date)
            return 0

        existing_transaction_ids = self._existing_transaction_ids(mode)
        validation_result = validate_transactions(
            transactions_source,
            customer_ids=set(stg_customers["customer_id"].astype(str)),
            product_codes=set(stg_products["product_code"].astype(str)),
            branch_codes=set(stg_branches["branch_code"].astype(str)),
            config=self.config,
            existing_transaction_ids=existing_transaction_ids,
        )

        valid_transactions = validation_result.valid_frame.copy()
        rejected_transactions = validation_result.rejected_frame.copy()
        valid_transactions["job_id"] = job_id

        if mode == "full-refresh":
            truncate_table(self.engine, "stg_transactions")
        if not valid_transactions.empty:
            write_dataframe(self.engine, "stg_transactions", valid_transactions)

        issue_rows = build_issue_rows(job_id, validation_result.issues)
        rejected_rows = build_rejected_rows(job_id, "transactions", rejected_transactions)
        write_dataframe(self.engine, "validation_errors", issue_rows)
        write_dataframe(self.engine, "rejected_records", rejected_rows)
        self._write_rejected_file(job_id, rejected_transactions)

        self._refresh_dimensions_and_marts(job_id, run_date, transactions_source, valid_transactions, rejected_transactions)
        return len(rejected_transactions)

    def _refresh_dimensions_and_marts(
        self,
        job_id: str,
        run_date: date,
        source_transactions: pd.DataFrame | None = None,
        valid_transactions: pd.DataFrame | None = None,
        rejected_transactions: pd.DataFrame | None = None,
    ) -> None:
        stg_transactions = read_table(self.engine, "SELECT * FROM stg_transactions")
        stg_customers = read_table(self.engine, "SELECT * FROM stg_customers")
        stg_products = read_table(self.engine, "SELECT * FROM stg_products")
        stg_branches = read_table(self.engine, "SELECT * FROM stg_branches")

        dim_date = build_dim_date(stg_transactions)
        dim_customer = stg_customers.drop(columns=["job_id"], errors="ignore").copy()
        dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))
        dim_product = stg_products.drop(columns=["job_id"], errors="ignore").copy()
        dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))
        dim_branch = stg_branches.drop(columns=["job_id"], errors="ignore").copy()
        dim_branch.insert(0, "branch_key", range(1, len(dim_branch) + 1))

        for table_name in [
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_branch",
            "fact_transactions",
            "mart_daily_sales",
            "mart_branch_performance",
            "mart_product_performance",
            "mart_reconciliation_summary",
        ]:
            truncate_table(self.engine, table_name)

        write_dataframe(self.engine, "dim_date", dim_date)
        write_dataframe(self.engine, "dim_customer", dim_customer)
        write_dataframe(self.engine, "dim_product", dim_product)
        write_dataframe(self.engine, "dim_branch", dim_branch)

        fact = build_fact_transactions(stg_transactions, dim_date, dim_customer, dim_product, dim_branch)
        write_dataframe(self.engine, "fact_transactions", fact)

        mart_daily_sales = build_mart_daily_sales(fact, dim_date)
        mart_branch = build_mart_branch_performance(fact, dim_branch)
        mart_product = build_mart_product_performance(fact, dim_product)
        reconciliation = build_mart_reconciliation_summary(
            run_date,
            source_transactions if source_transactions is not None else pd.DataFrame(),
            valid_transactions if valid_transactions is not None else pd.DataFrame(),
            rejected_transactions if rejected_transactions is not None else pd.DataFrame(),
        )

        write_dataframe(self.engine, "mart_daily_sales", mart_daily_sales)
        write_dataframe(self.engine, "mart_branch_performance", mart_branch)
        write_dataframe(self.engine, "mart_product_performance", mart_product)
        write_dataframe(self.engine, "mart_reconciliation_summary", reconciliation)
        self._export_reports()

    def _existing_transaction_ids(self, mode: str) -> set[str]:
        """Return transaction IDs already loaded to staging for incremental deduping."""

        if mode == "full-refresh":
            return set()
        frame = read_table(self.engine, "SELECT transaction_id FROM stg_transactions")
        if frame.empty:
            return set()
        return set(frame["transaction_id"].astype(str))

    def _write_rejected_file(self, job_id: str, rejected_frame: pd.DataFrame) -> None:
        if rejected_frame.empty:
            return
        path = self.config.rejected_records_dir / f"{job_id}_transactions_rejected.csv"
        rejected_frame.to_csv(path, index=False)

    def _export_reports(self) -> None:
        report_queries = {
            "mart_daily_sales.csv": "SELECT * FROM mart_daily_sales",
            "mart_branch_performance.csv": "SELECT * FROM mart_branch_performance",
            "mart_product_performance.csv": "SELECT * FROM mart_product_performance",
            "mart_reconciliation_summary.csv": "SELECT * FROM mart_reconciliation_summary",
        }
        for filename, query in report_queries.items():
            frame = read_table(self.engine, query)
            frame.to_csv(self.config.reports_dir / filename, index=False)

    def _insert_job_log(self, job_id: str, run_date: date, mode: str, source_scope: str, status: str, started_at: datetime) -> None:
        frame = pd.DataFrame(
            [
                {
                    "job_id": job_id,
                    "run_date": run_date,
                    "mode": mode,
                    "source_scope": source_scope,
                    "status": status,
                    "started_at": started_at,
                    "finished_at": None,
                    "records_read": 0,
                    "records_loaded": 0,
                    "records_rejected": 0,
                    "message": None,
                }
            ]
        )
        write_dataframe(self.engine, "etl_job_logs", frame)

    def _update_job_log(
        self,
        job_id: str,
        status: str,
        finished_at: datetime,
        records_read: int,
        records_loaded: int,
        records_rejected: int,
        message: str,
    ) -> None:
        with begin(self.engine) as connection:
            connection.execute(
                text(
                    """
                    UPDATE etl_job_logs
                    SET status = :status,
                        finished_at = :finished_at,
                        records_read = :records_read,
                        records_loaded = :records_loaded,
                        records_rejected = :records_rejected,
                        message = :message
                    WHERE job_id = :job_id
                    """
                ),
                {
                    "job_id": job_id,
                    "status": status,
                    "finished_at": finished_at,
                    "records_read": records_read,
                    "records_loaded": records_loaded,
                    "records_rejected": records_rejected,
                    "message": message,
                },
            )
