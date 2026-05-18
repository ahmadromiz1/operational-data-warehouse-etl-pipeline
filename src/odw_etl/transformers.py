"""Warehouse and data mart transformations."""

from __future__ import annotations

from datetime import date

import pandas as pd


def build_dim_date(transactions: pd.DataFrame) -> pd.DataFrame:
    """Create a date dimension from staged transactions."""

    if transactions.empty:
        return pd.DataFrame(
            columns=[
                "date_key",
                "full_date",
                "day_of_week",
                "day_of_month",
                "month_number",
                "month_name",
                "quarter_number",
                "year_number",
            ]
        )
    unique_dates = pd.Series(pd.to_datetime(transactions["transaction_date"]).dt.date.unique()).sort_values()
    frame = pd.DataFrame({"full_date": unique_dates})
    frame["date_key"] = frame["full_date"].apply(lambda x: int(x.strftime("%Y%m%d")))
    frame["day_of_week"] = frame["full_date"].apply(lambda x: x.strftime("%A"))
    frame["day_of_month"] = frame["full_date"].apply(lambda x: x.day)
    frame["month_number"] = frame["full_date"].apply(lambda x: x.month)
    frame["month_name"] = frame["full_date"].apply(lambda x: x.strftime("%B"))
    frame["quarter_number"] = frame["full_date"].apply(lambda x: ((x.month - 1) // 3) + 1)
    frame["year_number"] = frame["full_date"].apply(lambda x: x.year)
    return frame


def build_fact_transactions(
    stg_transactions: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_branch: pd.DataFrame,
) -> pd.DataFrame:
    """Join staged transactions to dimensions and return a fact table."""

    transactions = stg_transactions.copy()
    dates = dim_date.copy()
    transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"]).dt.date
    dates["full_date"] = pd.to_datetime(dates["full_date"]).dt.date

    fact = transactions.merge(
        dates[["date_key", "full_date"]],
        left_on="transaction_date",
        right_on="full_date",
        how="inner",
    ).merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="inner",
    ).merge(
        dim_product[["product_key", "product_code"]],
        on="product_code",
        how="inner",
    ).merge(
        dim_branch[["branch_key", "branch_code"]],
        on="branch_code",
        how="inner",
    )
    return fact[
        [
            "transaction_id",
            "date_key",
            "customer_key",
            "product_key",
            "branch_key",
            "quantity",
            "unit_price",
            "total_amount",
            "payment_method",
            "job_id",
        ]
    ].copy()


def build_mart_daily_sales(fact: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily sales performance."""

    merged = fact.merge(dim_date[["date_key", "full_date"]], on="date_key", how="left")
    mart = (
        merged.groupby("full_date", as_index=False)
        .agg(
            transaction_count=("transaction_id", "nunique"),
            total_quantity=("quantity", "sum"),
            total_sales=("total_amount", "sum"),
        )
        .rename(columns={"full_date": "sales_date"})
    )
    return mart


def build_mart_branch_performance(fact: pd.DataFrame, dim_branch: pd.DataFrame) -> pd.DataFrame:
    """Aggregate branch sales performance."""

    merged = fact.merge(dim_branch[["branch_key", "branch_code", "branch_name"]], on="branch_key", how="left")
    return merged.groupby(["branch_code", "branch_name"], as_index=False).agg(
        transaction_count=("transaction_id", "nunique"),
        total_quantity=("quantity", "sum"),
        total_sales=("total_amount", "sum"),
    )


def build_mart_product_performance(fact: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    """Aggregate product sales performance."""

    merged = fact.merge(dim_product[["product_key", "product_code", "product_name"]], on="product_key", how="left")
    return merged.groupby(["product_code", "product_name"], as_index=False).agg(
        transaction_count=("transaction_id", "nunique"),
        total_quantity=("quantity", "sum"),
        total_sales=("total_amount", "sum"),
    )


def build_mart_reconciliation_summary(
    run_date: date,
    source_transactions: pd.DataFrame,
    valid_transactions: pd.DataFrame,
    rejected_transactions: pd.DataFrame,
) -> pd.DataFrame:
    """Produce a run-level reconciliation summary."""

    source_total = float(source_transactions["total_amount"].fillna(0).sum()) if not source_transactions.empty else 0.0
    loaded_total = float(valid_transactions["total_amount"].fillna(0).sum()) if not valid_transactions.empty else 0.0
    rejected_total = float(rejected_transactions["total_amount"].fillna(0).sum()) if not rejected_transactions.empty else 0.0
    return pd.DataFrame(
        [
            {
                "run_date": run_date,
                "source_transaction_count": int(source_transactions["transaction_id"].count()) if "transaction_id" in source_transactions.columns else 0,
                "valid_transaction_count": int(valid_transactions["transaction_id"].count()) if "transaction_id" in valid_transactions.columns else 0,
                "rejected_transaction_count": int(rejected_transactions["transaction_id"].count()) if "transaction_id" in rejected_transactions.columns else 0,
                "source_total_amount": source_total,
                "loaded_total_amount": loaded_total,
                "rejected_total_amount": rejected_total,
                "amount_difference": round(source_total - (loaded_total + rejected_total), 2),
            }
        ]
    )
