"""Data validation for raw CSV data.

Validates order data against business rules before loading into the pipeline.
Uses pandas for portable validation that doesn't depend on GE version.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def validate_csv(path_to_csv):
    """
    Validate the CSV file against business rules:
      1) 'order_id' must not be null
      2) 'amount' must be > 0
      3) 'customer_id' must be >= 1
    """
    df = pd.read_csv(path_to_csv)
    logger.info("Validating %d rows from %s", len(df), path_to_csv)

    # Expect order_id to not be null
    null_order_ids = df["order_id"].isna().sum()
    if null_order_ids > 0:
        raise ValueError(f"Validation failed: Found {null_order_ids} null values in 'order_id'")

    # Expect amount to be > 0
    invalid_amounts = (df["amount"] <= 0).sum()
    if invalid_amounts > 0:
        raise ValueError(f"Validation failed: 'amount' must be > 0 ({invalid_amounts} invalid rows)")

    # Expect customer_id to be >= 1
    invalid_customers = (df["customer_id"] < 1).sum()
    if invalid_customers > 0:
        raise ValueError(f"Validation failed: 'customer_id' must be >= 1 ({invalid_customers} invalid rows)")

    logger.info("Validation passed for %s (%d rows).", path_to_csv, len(df))


if __name__ == "__main__":
    validate_csv("/tmp/orders.csv")
