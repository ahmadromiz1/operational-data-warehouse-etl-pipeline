"""Validation and cleansing logic."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from .config import AppConfig
from .utils import clean_text_series, json_dumps


@dataclass(slots=True)
class ValidationIssue:
    """A single row validation failure."""

    source_name: str
    record_key: str | None
    error_code: str
    error_message: str
    payload: str


@dataclass(slots=True)
class ValidationResult:
    """Validation result bundle."""

    valid_frame: pd.DataFrame
    rejected_frame: pd.DataFrame
    issues: list[ValidationIssue]


def normalize_master_data(frame: pd.DataFrame, code_column: str) -> pd.DataFrame:
    """Trim text and uppercase business codes for master data."""

    normalized = frame.copy()
    for column in normalized.columns:
        if normalized[column].dtype == "object":
            normalized[column] = clean_text_series(normalized[column])
    normalized[code_column] = normalized[code_column].str.upper()
    return normalized


def validate_transactions(
    frame: pd.DataFrame,
    customer_ids: set[str],
    product_codes: set[str],
    branch_codes: set[str],
    config: AppConfig,
    existing_transaction_ids: set[str] | None = None,
) -> ValidationResult:
    """Validate transaction data against business rules."""

    working = frame.copy()
    existing_transaction_ids = existing_transaction_ids or set()
    for column in working.columns:
        if working[column].dtype == "object":
            working[column] = clean_text_series(working[column])

    for code_column in ["product_code", "branch_code"]:
        working[code_column] = working[code_column].str.upper()

    working["customer_id"] = working["customer_id"].astype("string")
    working["parsed_transaction_date"] = pd.to_datetime(
        working["transaction_date"], errors="coerce"
    ).dt.date
    numeric_columns = ["quantity", "unit_price", "total_amount"]
    for column in numeric_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")

    duplicate_mask = working["transaction_id"].duplicated(keep=False)
    issues: list[ValidationIssue] = []
    rejected_indices: set[int] = set()

    for index, row in working.iterrows():
        record_key = None if pd.isna(row.get("transaction_id")) else str(row["transaction_id"])
        row_errors: list[tuple[str, str]] = []

        for field in config.required_transaction_fields:
            if pd.isna(row.get(field)) or str(row.get(field)).strip() == "":
                row_errors.append(("REQUIRED_FIELD_MISSING", f"{field} must not be empty"))

        if pd.isna(row["parsed_transaction_date"]):
            row_errors.append(("INVALID_TRANSACTION_DATE", "transaction_date must be valid"))

        if duplicate_mask.loc[index] or record_key in existing_transaction_ids:
            row_errors.append(("DUPLICATE_TRANSACTION_ID", "transaction_id must be unique"))

        if str(row.get("customer_id")) not in customer_ids:
            row_errors.append(("UNKNOWN_CUSTOMER_ID", "customer_id must exist in customer master"))

        if str(row.get("product_code")) not in product_codes:
            row_errors.append(("UNKNOWN_PRODUCT_CODE", "product_code must exist in product master"))

        if str(row.get("branch_code")) not in branch_codes:
            row_errors.append(("UNKNOWN_BRANCH_CODE", "branch_code must exist in branch master"))

        if pd.isna(row["quantity"]) or float(row["quantity"]) <= 0:
            row_errors.append(("INVALID_QUANTITY", "quantity must be greater than zero"))

        if pd.isna(row["unit_price"]) or float(row["unit_price"]) <= 0:
            row_errors.append(("INVALID_UNIT_PRICE", "unit_price must be greater than zero"))

        expected_total = None
        if not pd.isna(row["quantity"]) and not pd.isna(row["unit_price"]):
            expected_total = round(float(row["quantity"]) * float(row["unit_price"]), 2)
        if expected_total is None or pd.isna(row["total_amount"]) or round(float(row["total_amount"]), 2) != expected_total:
            row_errors.append(
                ("AMOUNT_RECONCILIATION_FAILED", "total_amount must equal quantity * unit_price")
            )

        if row_errors:
            rejected_indices.add(index)
            for error_code, error_message in row_errors:
                issues.append(
                    ValidationIssue(
                        source_name="transactions",
                        record_key=record_key,
                        error_code=error_code,
                        error_message=error_message,
                        payload=json_dumps(row.to_dict()),
                    )
                )

    valid_frame = working.loc[~working.index.isin(rejected_indices)].copy()
    rejected_frame = working.loc[working.index.isin(rejected_indices)].copy()
    valid_frame["transaction_date"] = valid_frame["parsed_transaction_date"]
    rejected_frame["transaction_date"] = rejected_frame["parsed_transaction_date"]
    valid_frame = valid_frame.drop(columns=["parsed_transaction_date"])
    rejected_frame = rejected_frame.drop(columns=["parsed_transaction_date"])
    return ValidationResult(valid_frame=valid_frame, rejected_frame=rejected_frame, issues=issues)


def build_issue_rows(job_id: str, issues: list[ValidationIssue]) -> pd.DataFrame:
    """Convert issues into a DataFrame for insertion."""

    created_at = datetime.now(UTC).replace(tzinfo=None)
    return pd.DataFrame(
        [
            {
                "job_id": job_id,
                "source_name": issue.source_name,
                "record_key": issue.record_key,
                "error_code": issue.error_code,
                "error_message": issue.error_message,
                "payload": issue.payload,
                "created_at": created_at,
            }
            for issue in issues
        ]
    )


def build_rejected_rows(job_id: str, source_name: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Convert rejected rows into a DataFrame for insertion."""

    if frame.empty:
        return pd.DataFrame()
    created_at = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        rows.append(
            {
                "job_id": job_id,
                "source_name": source_name,
                "record_key": None if pd.isna(row.get("transaction_id")) else str(row.get("transaction_id")),
                "rejection_reason": "Validation failed",
                "payload": json_dumps(row.to_dict()),
                "created_at": created_at,
            }
        )
    return pd.DataFrame(rows)
