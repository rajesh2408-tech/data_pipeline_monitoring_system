from __future__ import annotations

import json
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .database import (
    DEFAULT_DB, SCHEMAS, connect, init_db, table_for_dataset
)

def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")

def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip()) or pd.isna(value)

def _parse_timestamp(value: Any) -> str | None:
    if _is_blank(value):
        return None
    try:
        ts = pd.Timestamp(str(value))
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.isoformat()
    except Exception:
        return None

def _schema_error(dataset_name: str, columns: list[str]) -> str | None:
    schema = SCHEMAS[dataset_name]["columns"]
    missing = [c for c in schema if c not in columns]
    unexpected = [c for c in columns if c not in schema]
    if missing or unexpected:
        parts = []
        if missing:
            parts.append("missing required columns: " + ", ".join(missing))
        if unexpected:
            parts.append("unexpected columns: " + ", ".join(unexpected))
        return "; ".join(parts)
    return None

def _validate_record(dataset_name: str, row: dict, existing_reference_keys: set[str]) -> list[str]:
    errors: list[str] = []
    required = SCHEMAS[dataset_name]["columns"]
    for col in required:
        if _is_blank(row.get(col)):
            errors.append(f"missing required field: {col}")

    if errors:
        return errors

    if dataset_name == "orders":
        try:
            amount = float(row["amount"])
            if amount < 0:
                errors.append("amount must be greater than or equal to 0")
        except (TypeError, ValueError):
            errors.append("amount must be numeric")
        if not _parse_timestamp(row["order_timestamp"]):
            errors.append("order_timestamp must be a valid timestamp")

    elif dataset_name == "inventory":
        try:
            stock = float(row["stock_quantity"])
            if not stock.is_integer() or stock < 0:
                errors.append("stock_quantity must be a non-negative integer")
        except (TypeError, ValueError):
            errors.append("stock_quantity must be a non-negative integer")
        if not _parse_timestamp(row["updated_at"]):
            errors.append("updated_at must be a valid timestamp")

    elif dataset_name == "payments":
        try:
            amount = float(row["amount"])
            if amount < 0:
                errors.append("amount must be greater than or equal to 0")
        except (TypeError, ValueError):
            errors.append("amount must be numeric")
        if str(row["payment_status"]).strip().lower() not in {"paid", "pending", "failed", "refunded"}:
            errors.append("payment_status must be paid, pending, failed, or refunded")
        if not _parse_timestamp(row["payment_timestamp"]):
            errors.append("payment_timestamp must be a valid timestamp")
        if str(row["order_id"]).strip() not in existing_reference_keys:
            errors.append("order_id does not exist in the published orders target")

    return errors

def process_dataframe(
    dataset_name: str,
    dataframe: pd.DataFrame,
    source_file: str = "uploaded.csv",
    db_path: str | Path = DEFAULT_DB,
) -> dict:
    """Validate and publish one CSV batch. Existing target keys are skipped on rerun."""
    if dataset_name not in SCHEMAS:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    init_db(db_path)
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    start_clock = time.perf_counter()
    frame = dataframe.copy()
    input_rows = int(len(frame))
    published_rows = 0
    rejected_rows = 0
    skipped_existing_rows = 0
    failure_reason = None
    status = "success"
    quarantine_items = []

    schema_issue = _schema_error(dataset_name, list(frame.columns))
    if schema_issue:
        status = "failed"
        failure_reason = "Schema validation failed: " + schema_issue

    table = table_for_dataset(dataset_name)
    key_column = SCHEMAS[dataset_name]["columns"][0]
    timestamp_columns = {
        "orders": "order_timestamp",
        "inventory": "updated_at",
        "payments": "payment_timestamp",
    }

    try:
        with connect(db_path) as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                (run_id, dataset_name, source_file, started_at, status, input_rows)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (run_id, dataset_name, Path(source_file).name, started_at, "running", input_rows))

            if status == "success":
                # Reference checks use the already-published orders target.
                existing_reference_keys = {
                    str(r[0]) for r in conn.execute("SELECT order_id FROM target_orders").fetchall()
                }
                existing_target_keys = {
                    str(r[0]) for r in conn.execute(f"SELECT {key_column} FROM {table}").fetchall()
                }
                seen_batch_keys: set[str] = set()

                for index, series in frame.iterrows():
                    row = {str(k): (None if pd.isna(v) else v) for k, v in series.to_dict().items()}
                    source_row_number = int(index) + 2 if isinstance(index, int) else len(seen_batch_keys) + 2
                    errors = _validate_record(dataset_name, row, existing_reference_keys)
                    raw_key = row.get(key_column)
                    key = "" if _is_blank(raw_key) else str(raw_key).strip()

                    if not errors and key in seen_batch_keys:
                        errors.append(f"duplicate key within input batch: {key}")

                    if not errors and key in existing_target_keys:
                        # Re-uploading an already published key is an idempotent no-op.
                        skipped_existing_rows += 1
                        continue

                    if not errors:
                        seen_batch_keys.add(key)
                        clean = dict(row)
                        clean[key_column] = key
                        clean[timestamp_columns[dataset_name]] = _parse_timestamp(clean[timestamp_columns[dataset_name]])
                        if dataset_name in {"orders", "payments"}:
                            clean["amount"] = float(clean["amount"])
                        if dataset_name == "inventory":
                            clean["stock_quantity"] = int(float(clean["stock_quantity"]))

                        clean["source_file"] = Path(source_file).name
                        clean["run_id"] = run_id
                        columns = list(clean.keys())
                        placeholders = ", ".join(["?"] * len(columns))
                        col_sql = ", ".join(columns)
                        conn.execute(
                            f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})",
                            tuple(clean[c] for c in columns)
                        )
                        published_rows += 1
                        existing_target_keys.add(key)
                        if dataset_name == "orders":
                            existing_reference_keys.add(key)
                    else:
                        rejected_rows += 1
                        quarantine_items.append({
                            "source_row_number": source_row_number,
                            "record_json": json.dumps(row, default=str, ensure_ascii=False),
                            "reject_reason": "; ".join(errors),
                        })

                for item in quarantine_items:
                    conn.execute("""
                        INSERT INTO quarantined_records
                        (run_id, dataset_name, source_file, source_row_number,
                         record_json, reject_reason, rejected_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        run_id, dataset_name, Path(source_file).name,
                        item["source_row_number"], item["record_json"],
                        item["reject_reason"], now_iso()
                    ))

                conn.execute("""
                    INSERT INTO dataset_arrivals (dataset_name, arrival_at, source_file, run_id)
                    VALUES (?, ?, ?, ?)
                """, (dataset_name, started_at, Path(source_file).name, run_id))

            completed_at = now_iso()
            duration = round(time.perf_counter() - start_clock, 6)
            conn.execute("""
                UPDATE pipeline_runs
                SET completed_at=?, status=?, published_rows=?, rejected_rows=?,
                    skipped_existing_rows=?, duration_seconds=?, failure_reason=?
                WHERE run_id=?
            """, (
                completed_at, status, published_rows, rejected_rows,
                skipped_existing_rows, duration, failure_reason, run_id
            ))

    except Exception as exc:
        status = "failed"
        failure_reason = f"{type(exc).__name__}: {exc}"
        completed_at = now_iso()
        duration = round(time.perf_counter() - start_clock, 6)
        with connect(db_path) as conn:
            conn.execute("""
                UPDATE pipeline_runs SET completed_at=?, status='failed',
                    published_rows=?, rejected_rows=?, skipped_existing_rows=?,
                    duration_seconds=?, failure_reason=? WHERE run_id=?
            """, (
                completed_at, published_rows, rejected_rows, skipped_existing_rows,
                duration, failure_reason, run_id
            ))

    return {
        "run_id": run_id,
        "dataset_name": dataset_name,
        "source_file": Path(source_file).name,
        "status": status,
        "input_rows": input_rows,
        "published_rows": published_rows,
        "rejected_rows": rejected_rows,
        "skipped_existing_rows": skipped_existing_rows,
        "duration_seconds": duration,
        "failure_reason": failure_reason,
        "started_at": started_at,
        "completed_at": completed_at,
    }

def process_csv_file(dataset_name: str, file_path: str | Path, db_path: str | Path = DEFAULT_DB) -> dict:
    path = Path(file_path)
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        init_db(db_path)
        run_id = str(uuid.uuid4())
        now = now_iso()
        with connect(db_path) as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                (run_id, dataset_name, source_file, started_at, completed_at, status,
                 input_rows, duration_seconds, failure_reason)
                VALUES (?, ?, ?, ?, ?, 'failed', 0, 0, ?)
            """, (run_id, dataset_name, path.name, now, now, f"Could not parse CSV: {exc}"))
        return {
            "run_id": run_id, "dataset_name": dataset_name, "source_file": path.name,
            "status": "failed", "input_rows": 0, "published_rows": 0,
            "rejected_rows": 0, "skipped_existing_rows": 0, "duration_seconds": 0,
            "failure_reason": f"Could not parse CSV: {exc}", "started_at": now, "completed_at": now
        }
    return process_dataframe(dataset_name, frame, path.name, db_path)
