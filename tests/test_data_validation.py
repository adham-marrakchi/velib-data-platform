"""Tests for data validation logic used in Spark and Airflow."""

import os
import sys
import tempfile

import pandas as pd
import pytest

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "great_expectations", "expectations"))


def test_valid_orders_csv():
    """Test that valid orders pass validation."""
    from raw_data_validation import validate_csv

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("order_id,customer_id,amount\n")
        f.write("1,1,100.50\n")
        f.write("2,2,250.00\n")
        f.write("3,3,75.25\n")
        path = f.name

    try:
        validate_csv(path)  # Should not raise
    finally:
        os.unlink(path)


def test_null_order_id_fails_validation():
    """Test that null order_id fails validation."""
    from raw_data_validation import validate_csv

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("order_id,customer_id,amount\n")
        f.write(",1,100.50\n")
        path = f.name

    try:
        with pytest.raises(ValueError, match="order_id"):
            validate_csv(path)
    finally:
        os.unlink(path)


def test_negative_amount_fails_validation():
    """Test that negative amount fails validation."""
    from raw_data_validation import validate_csv

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("order_id,customer_id,amount\n")
        f.write("1,1,-50.00\n")
        path = f.name

    try:
        with pytest.raises(ValueError, match="amount"):
            validate_csv(path)
    finally:
        os.unlink(path)


def test_zero_customer_id_fails_validation():
    """Test that customer_id=0 fails validation."""
    from raw_data_validation import validate_csv

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("order_id,customer_id,amount\n")
        f.write("1,0,100.50\n")
        path = f.name

    try:
        with pytest.raises(ValueError, match="customer_id"):
            validate_csv(path)
    finally:
        os.unlink(path)
