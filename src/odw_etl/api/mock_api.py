"""Mock branch master API."""

from __future__ import annotations

from typing import Any


def fetch_branches() -> list[dict[str, Any]]:
    """Return branch master records from a mock API source."""

    return [
        {
            "branch_code": "BR01",
            "branch_name": "Central Jakarta",
            "region": "West",
            "city": "Jakarta",
            "manager_name": "Aulia Pratama",
            "active_flag": "Y",
        },
        {
            "branch_code": "BR02",
            "branch_name": "Bandung Dago",
            "region": "West",
            "city": "Bandung",
            "manager_name": "Nadya Putri",
            "active_flag": "Y",
        },
        {
            "branch_code": "BR03",
            "branch_name": "Surabaya Tunjungan",
            "region": "East",
            "city": "Surabaya",
            "manager_name": "Rizky Mahendra",
            "active_flag": "Y",
        },
    ]

