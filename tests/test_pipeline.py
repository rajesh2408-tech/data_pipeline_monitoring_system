import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database import init_db, read_table, read_query
from src.pipeline import process_dataframe
from src.monitoring import evaluate_monitoring

def fresh_db(tmp_path):
    db = tmp_path / "test.sqlite"
    init_db(db)
    return db

def orders_frame():
    return pd.DataFrame([
        {"order_id": "O1", "customer_id": "C1", "amount": 100, "order_timestamp": "2026-10-29T08:30:00+00:00"},
        {"order_id": "O2", "customer_id": "C2", "amount": 200, "order_timestamp": "2026-10-29T08:40:00+00:00"},
    ])

def test_happy_path_and_idempotent_rerun(tmp_path):
    db = fresh_db(tmp_path)
    frame = orders_frame()
    first = process_dataframe("orders", frame, "orders.csv", db)
    assert first["status"] == "success"
    assert first["published_rows"] == 2
    second = process_dataframe("orders", frame, "orders.csv", db)
    assert second["status"] == "success"
    assert second["published_rows"] == 0
    assert second["skipped_existing_rows"] == 2
    assert len(read_table("orders", db)) == 2

def test_empty_input_is_safe(tmp_path):
    db = fresh_db(tmp_path)
    frame = pd.DataFrame(columns=["order_id", "customer_id", "amount", "order_timestamp"])
    result = process_dataframe("orders", frame, "empty.csv", db)
    assert result["status"] == "success"
    assert result["input_rows"] == 0
    assert len(read_table("orders", db)) == 0

def test_missing_required_value_is_quarantined(tmp_path):
    db = fresh_db(tmp_path)
    frame = orders_frame()
    frame.loc[0, "customer_id"] = None
    result = process_dataframe("orders", frame, "orders.csv", db)
    assert result["rejected_rows"] == 1
    assert len(read_table("orders", db)) == 1
    q = read_query("SELECT * FROM quarantined_records", db_path=db)
    assert "missing required field: customer_id" in q.iloc[0]["reject_reason"]

def test_duplicate_key_within_batch_is_rejected(tmp_path):
    db = fresh_db(tmp_path)
    frame = pd.concat([orders_frame(), orders_frame().iloc[[0]]], ignore_index=True)
    result = process_dataframe("orders", frame, "duplicates.csv", db)
    assert result["published_rows"] == 2
    assert result["rejected_rows"] == 1

def test_unexpected_schema_fails_without_publishing(tmp_path):
    db = fresh_db(tmp_path)
    frame = orders_frame().assign(unexpected_column="x")
    result = process_dataframe("orders", frame, "schema.csv", db)
    assert result["status"] == "failed"
    assert "unexpected columns" in result["failure_reason"]
    assert len(read_table("orders", db)) == 0

def test_domain_edge_cases_negative_amount_and_bad_timestamp(tmp_path):
    db = fresh_db(tmp_path)
    frame = orders_frame()
    frame.loc[0, "amount"] = -1
    frame.loc[1, "order_timestamp"] = "not-a-date"
    result = process_dataframe("orders", frame, "bad_values.csv", db)
    assert result["rejected_rows"] == 2
    assert result["published_rows"] == 0

def test_payment_reference_integrity(tmp_path):
    db = fresh_db(tmp_path)
    process_dataframe("orders", orders_frame(), "orders.csv", db)
    payments = pd.DataFrame([
        {"payment_id": "PAY1", "order_id": "O1", "amount": 100, "payment_status": "paid", "payment_timestamp": "2026-10-29T08:45:00+00:00"},
        {"payment_id": "PAY2", "order_id": "UNKNOWN", "amount": 100, "payment_status": "paid", "payment_timestamp": "2026-10-29T08:45:00+00:00"},
    ])
    result = process_dataframe("payments", payments, "payments.csv", db)
    assert result["published_rows"] == 1
    assert result["rejected_rows"] == 1

def test_reconciliation_counts(tmp_path):
    db = fresh_db(tmp_path)
    frame = orders_frame()
    frame.loc[0, "amount"] = -1
    result = process_dataframe("orders", frame, "orders.csv", db)
    assert result["input_rows"] == (
        result["published_rows"] + result["rejected_rows"] + result["skipped_existing_rows"]
    )
