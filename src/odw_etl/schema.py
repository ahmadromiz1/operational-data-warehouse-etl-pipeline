"""Database schema definitions."""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text


metadata = MetaData()

raw_file_logs = Table(
    "raw_file_logs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), nullable=False),
    Column("source_name", String(50), nullable=False),
    Column("source_type", String(30), nullable=False),
    Column("source_path", String(255), nullable=True),
    Column("record_count", Integer, nullable=False),
    Column("load_mode", String(20), nullable=False),
    Column("run_date", Date, nullable=False),
    Column("loaded_at", DateTime, nullable=False),
)

etl_job_logs = Table(
    "etl_job_logs",
    metadata,
    Column("job_id", String(64), primary_key=True),
    Column("run_date", Date, nullable=False),
    Column("mode", String(20), nullable=False),
    Column("source_scope", String(50), nullable=False),
    Column("status", String(20), nullable=False),
    Column("started_at", DateTime, nullable=False),
    Column("finished_at", DateTime, nullable=True),
    Column("records_read", Integer, nullable=False, default=0),
    Column("records_loaded", Integer, nullable=False, default=0),
    Column("records_rejected", Integer, nullable=False, default=0),
    Column("message", Text, nullable=True),
)

validation_errors = Table(
    "validation_errors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), nullable=False),
    Column("source_name", String(50), nullable=False),
    Column("record_key", String(100), nullable=True),
    Column("error_code", String(50), nullable=False),
    Column("error_message", String(255), nullable=False),
    Column("payload", Text, nullable=False),
    Column("created_at", DateTime, nullable=False),
)

rejected_records = Table(
    "rejected_records",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), nullable=False),
    Column("source_name", String(50), nullable=False),
    Column("record_key", String(100), nullable=True),
    Column("rejection_reason", String(255), nullable=False),
    Column("payload", Text, nullable=False),
    Column("created_at", DateTime, nullable=False),
)

stg_transactions = Table(
    "stg_transactions",
    metadata,
    Column("transaction_id", String(50), primary_key=True),
    Column("transaction_date", Date, nullable=False),
    Column("customer_id", String(50), nullable=False),
    Column("product_code", String(50), nullable=False),
    Column("branch_code", String(50), nullable=False),
    Column("quantity", Float, nullable=False),
    Column("unit_price", Float, nullable=False),
    Column("total_amount", Float, nullable=False),
    Column("payment_method", String(30), nullable=True),
    Column("job_id", String(64), nullable=False),
)

stg_customers = Table(
    "stg_customers",
    metadata,
    Column("customer_id", String(50), primary_key=True),
    Column("customer_name", String(100), nullable=False),
    Column("customer_segment", String(50), nullable=True),
    Column("city", String(50), nullable=True),
    Column("country", String(50), nullable=True),
    Column("active_flag", String(5), nullable=False),
    Column("job_id", String(64), nullable=False),
)

stg_products = Table(
    "stg_products",
    metadata,
    Column("product_code", String(50), primary_key=True),
    Column("product_name", String(100), nullable=False),
    Column("category", String(50), nullable=True),
    Column("brand", String(50), nullable=True),
    Column("unit_cost", Float, nullable=True),
    Column("active_flag", String(5), nullable=False),
    Column("job_id", String(64), nullable=False),
)

stg_branches = Table(
    "stg_branches",
    metadata,
    Column("branch_code", String(50), primary_key=True),
    Column("branch_name", String(100), nullable=False),
    Column("region", String(50), nullable=True),
    Column("city", String(50), nullable=True),
    Column("manager_name", String(100), nullable=True),
    Column("active_flag", String(5), nullable=False),
    Column("job_id", String(64), nullable=False),
)

dim_date = Table(
    "dim_date",
    metadata,
    Column("date_key", Integer, primary_key=True),
    Column("full_date", Date, nullable=False, unique=True),
    Column("day_of_week", String(20), nullable=False),
    Column("day_of_month", Integer, nullable=False),
    Column("month_number", Integer, nullable=False),
    Column("month_name", String(20), nullable=False),
    Column("quarter_number", Integer, nullable=False),
    Column("year_number", Integer, nullable=False),
)

dim_customer = Table(
    "dim_customer",
    metadata,
    Column("customer_key", Integer, primary_key=True, autoincrement=True),
    Column("customer_id", String(50), nullable=False, unique=True),
    Column("customer_name", String(100), nullable=False),
    Column("customer_segment", String(50), nullable=True),
    Column("city", String(50), nullable=True),
    Column("country", String(50), nullable=True),
    Column("active_flag", String(5), nullable=False),
)

dim_product = Table(
    "dim_product",
    metadata,
    Column("product_key", Integer, primary_key=True, autoincrement=True),
    Column("product_code", String(50), nullable=False, unique=True),
    Column("product_name", String(100), nullable=False),
    Column("category", String(50), nullable=True),
    Column("brand", String(50), nullable=True),
    Column("unit_cost", Float, nullable=True),
    Column("active_flag", String(5), nullable=False),
)

dim_branch = Table(
    "dim_branch",
    metadata,
    Column("branch_key", Integer, primary_key=True, autoincrement=True),
    Column("branch_code", String(50), nullable=False, unique=True),
    Column("branch_name", String(100), nullable=False),
    Column("region", String(50), nullable=True),
    Column("city", String(50), nullable=True),
    Column("manager_name", String(100), nullable=True),
    Column("active_flag", String(5), nullable=False),
)

fact_transactions = Table(
    "fact_transactions",
    metadata,
    Column("transaction_key", Integer, primary_key=True, autoincrement=True),
    Column("transaction_id", String(50), nullable=False, unique=True),
    Column("date_key", Integer, nullable=False),
    Column("customer_key", Integer, nullable=False),
    Column("product_key", Integer, nullable=False),
    Column("branch_key", Integer, nullable=False),
    Column("quantity", Float, nullable=False),
    Column("unit_price", Float, nullable=False),
    Column("total_amount", Float, nullable=False),
    Column("payment_method", String(30), nullable=True),
    Column("job_id", String(64), nullable=False),
)

mart_daily_sales = Table(
    "mart_daily_sales",
    metadata,
    Column("sales_date", Date, primary_key=True),
    Column("transaction_count", Integer, nullable=False),
    Column("total_quantity", Float, nullable=False),
    Column("total_sales", Float, nullable=False),
)

mart_branch_performance = Table(
    "mart_branch_performance",
    metadata,
    Column("branch_code", String(50), primary_key=True),
    Column("branch_name", String(100), nullable=False),
    Column("transaction_count", Integer, nullable=False),
    Column("total_quantity", Float, nullable=False),
    Column("total_sales", Float, nullable=False),
)

mart_product_performance = Table(
    "mart_product_performance",
    metadata,
    Column("product_code", String(50), primary_key=True),
    Column("product_name", String(100), nullable=False),
    Column("transaction_count", Integer, nullable=False),
    Column("total_quantity", Float, nullable=False),
    Column("total_sales", Float, nullable=False),
)

mart_reconciliation_summary = Table(
    "mart_reconciliation_summary",
    metadata,
    Column("run_date", Date, primary_key=True),
    Column("source_transaction_count", Integer, nullable=False),
    Column("valid_transaction_count", Integer, nullable=False),
    Column("rejected_transaction_count", Integer, nullable=False),
    Column("source_total_amount", Float, nullable=False),
    Column("loaded_total_amount", Float, nullable=False),
    Column("rejected_total_amount", Float, nullable=False),
    Column("amount_difference", Float, nullable=False),
)


all_tables = [
    raw_file_logs,
    etl_job_logs,
    validation_errors,
    rejected_records,
    stg_transactions,
    stg_customers,
    stg_products,
    stg_branches,
    dim_date,
    dim_customer,
    dim_product,
    dim_branch,
    fact_transactions,
    mart_daily_sales,
    mart_branch_performance,
    mart_product_performance,
    mart_reconciliation_summary,
]

