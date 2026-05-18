"""Validator unit tests."""

from __future__ import annotations

import pandas as pd

from odw_etl.config import load_config
from odw_etl.validators import validate_transactions


def test_validate_transactions_rejects_invalid_rows() -> None:
    """Validation should reject duplicates, bad references, and bad amounts."""

    config = load_config()
    frame = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "transaction_date": "2026-05-18",
                "customer_id": "C001",
                "product_code": "PRD001",
                "branch_code": "BR01",
                "quantity": 2,
                "unit_price": 10,
                "total_amount": 20,
                "payment_method": "CARD",
            },
            {
                "transaction_id": "TXN1",
                "transaction_date": "bad-date",
                "customer_id": "C999",
                "product_code": "PRD404",
                "branch_code": "BR99",
                "quantity": 0,
                "unit_price": -1,
                "total_amount": 999,
                "payment_method": "CASH",
            },
        ]
    )

    result = validate_transactions(
        frame=frame,
        customer_ids={"C001"},
        product_codes={"PRD001"},
        branch_codes={"BR01"},
        config=config,
        existing_transaction_ids=set(),
    )

    assert len(result.valid_frame) == 0
    assert len(result.rejected_frame) == 2
    codes = {issue.error_code for issue in result.issues}
    assert "DUPLICATE_TRANSACTION_ID" in codes
    assert "INVALID_TRANSACTION_DATE" in codes
    assert "UNKNOWN_CUSTOMER_ID" in codes
    assert "UNKNOWN_PRODUCT_CODE" in codes
    assert "UNKNOWN_BRANCH_CODE" in codes
    assert "INVALID_QUANTITY" in codes
    assert "INVALID_UNIT_PRICE" in codes
    assert "AMOUNT_RECONCILIATION_FAILED" in codes

