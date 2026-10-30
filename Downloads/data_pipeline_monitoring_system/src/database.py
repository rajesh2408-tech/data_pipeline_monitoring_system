from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime

DEFAULT_DB = Path("data/pipeline_monitor.db")

DATASET_CONFIG = [
    {
        "dataset_name": "orders",
        "expected_arrival": "09:00",
        "expected_completion": "09:15",
        "freshness_threshold_minutes": 60,
        "primary_key": "order_id",
        "timestamp_column": "order_timestamp",
        "description": "Customer orders and order amounts",
    },
    {
        "dataset_name": "inventory",
        "expected_arrival": "09:15",
        "expected_completion": "09:30",
        "freshness_threshold_minutes": 120,
        "primary_key": "product_id",
        "timestamp_column": "updated_at",
        "description": "Current product stock levels",
    },
    {
        "dataset_name": "payments",
        "expected_arrival": "09:30",
        "expected_completion": "09:45",
        "freshness_threshold_minutes": 60,
        "primary_key": "payment_id",
        "timestamp_column": "payment_timestamp",
        "description": "Payments associated with orders",
    },
]

SCHEMAS = {
    "orders": {
        "columns": ["order_id", "customer_id", "amount", "order_timestamp"],
        "types": {"order_id": "text", "customer_id": "text", "amount": "number", "order_timestamp": "timestamp"},
    },
    "inventory": {
        "columns": ["product_id", "stock_quantity", "updated_at"],
        "types": {"product_id": "text", "stock_quantity": "integer", "updated_at": "timestamp"},
    },
    "payments": {
        "columns": ["payment_id", "order_id", "amount", "payment_status", "payment_timestamp"],
        "types": {"payment_id": "text", "order_id": "text", "amount": "number", "payment_status": "text", "payment_timestamp": "timestamp"},
    },
}

def connect(db_path: str | Path = DEFAULT_DB) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db(db_path: str | Path = DEFAULT_DB) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dataset_config (
                dataset_name TEXT PRIMARY KEY,
                expected_arrival TEXT NOT NULL,
                expected_completion TEXT NOT NULL,
                freshness_threshold_minutes INTEGER NOT NULL,
                primary_key TEXT NOT NULL,
                timestamp_column TEXT NOT NULL,
                description TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                dataset_name TEXT NOT NULL,
                source_file TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                input_rows INTEGER NOT NULL DEFAULT 0,
                published_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                skipped_existing_rows INTEGER NOT NULL DEFAULT 0,
                duration_seconds REAL,
                failure_reason TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dataset_arrivals (
                arrival_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name TEXT NOT NULL,
                arrival_at TEXT NOT NULL,
                source_file TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                related_run_id TEXT,
                alert_key TEXT NOT NULL UNIQUE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS target_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount REAL NOT NULL,
                order_timestamp TEXT NOT NULL,
                source_file TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS target_inventory (
                product_id TEXT PRIMARY KEY,
                stock_quantity INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                source_file TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS target_payments (
                payment_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                amount REAL NOT NULL,
                payment_status TEXT NOT NULL,
                payment_timestamp TEXT NOT NULL,
                source_file TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quarantined_records (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_row_number INTEGER NOT NULL,
                record_json TEXT NOT NULL,
                reject_reason TEXT NOT NULL,
                rejected_at TEXT NOT NULL
            )
        """)
        for item in DATASET_CONFIG:
            conn.execute("""
                INSERT OR IGNORE INTO dataset_config
                (dataset_name, expected_arrival, expected_completion,
                 freshness_threshold_minutes, primary_key, timestamp_column, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item["dataset_name"], item["expected_arrival"], item["expected_completion"],
                item["freshness_threshold_minutes"], item["primary_key"],
                item["timestamp_column"], item["description"]
            ))

def get_config(db_path: str | Path = DEFAULT_DB) -> list[dict]:
    init_db(db_path)
    with connect(db_path) as conn:
        return [dict(row) for row in conn.execute(
            "SELECT * FROM dataset_config ORDER BY expected_arrival"
        ).fetchall()]

def table_for_dataset(dataset_name: str) -> str:
    if dataset_name not in SCHEMAS:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    return f"target_{dataset_name}"

def read_table(dataset_name: str, db_path: str | Path = DEFAULT_DB):
    import pandas as pd
    init_db(db_path)
    table = table_for_dataset(dataset_name)
    with connect(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table} ORDER BY 1", conn)

def read_query(query: str, params: tuple = (), db_path: str | Path = DEFAULT_DB):
    import pandas as pd
    init_db(db_path)
    with connect(db_path) as conn:
        return pd.read_sql_query(query, conn, params=params)
