from __future__ import annotations

from datetime import datetime, date, time as dt_time
from pathlib import Path

import pandas as pd

from .database import DEFAULT_DB, connect, get_config, init_db, read_query
from .pipeline import now_iso

def _local_datetime(day: date, clock: str) -> datetime:
    current_tz = datetime.now().astimezone().tzinfo
    hour, minute = map(int, clock.split(":"))
    return datetime.combine(day, dt_time(hour, minute)).replace(tzinfo=current_tz)

def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            result = result.astimezone()
        return result
    except (ValueError, TypeError):
        return None

def evaluate_monitoring(db_path: str | Path = DEFAULT_DB, now: datetime | None = None) -> list[dict]:
    """Evaluate today's arrival/completion SLAs and current target freshness."""
    init_db(db_path)
    current = now or datetime.now().astimezone()
    day = current.date()
    config = get_config(db_path)
    results = []

    for item in config:
        name = item["dataset_name"]
        expected_arrival = _local_datetime(day, item["expected_arrival"])
        expected_completion = _local_datetime(day, item["expected_completion"])
        arrivals = read_query(
            "SELECT * FROM dataset_arrivals WHERE dataset_name=? AND date(arrival_at)=? ORDER BY arrival_at",
            (name, day.isoformat()), db_path
        )
        runs = read_query(
            "SELECT * FROM pipeline_runs WHERE dataset_name=? AND date(started_at)=? ORDER BY started_at DESC",
            (name, day.isoformat()), db_path
        )

        arrival_at = _parse_dt(arrivals.iloc[-1]["arrival_at"]) if not arrivals.empty else None
        if arrival_at is None:
            arrival_status = "Awaiting arrival" if current < expected_arrival else "Missing"
            arrival_delay_minutes = None
        else:
            arrival_delay_minutes = max(0, round((arrival_at - expected_arrival).total_seconds() / 60, 2))
            arrival_status = "On time" if arrival_at <= expected_arrival else "Late"

        latest_run = runs.iloc[0].to_dict() if not runs.empty else None
        completed_at = _parse_dt(latest_run.get("completed_at")) if latest_run else None
        if latest_run and latest_run.get("status") == "failed":
            completion_status = "Failed"
        elif completed_at:
            completion_status = "On time" if completed_at <= expected_completion else "Late"
        elif current > expected_completion:
            completion_status = "Overdue"
        else:
            completion_status = "Pending"

        timestamp_column = item["timestamp_column"]
        table = f"target_{name}"
        timestamp_frame = read_query(
            f"SELECT MAX({timestamp_column}) AS latest_timestamp FROM {table}", (), db_path
        )
        latest_data_timestamp = timestamp_frame.iloc[0]["latest_timestamp"] if not timestamp_frame.empty else None
        latest_data_dt = _parse_dt(latest_data_timestamp)
        if latest_data_dt is None:
            freshness_minutes = None
            freshness_status = "No data"
        else:
            freshness_minutes = max(0, round((current - latest_data_dt).total_seconds() / 60, 2))
            freshness_status = (
                "Fresh" if freshness_minutes <= int(item["freshness_threshold_minutes"]) else "Stale"
            )

        results.append({
            "dataset_name": name,
            "expected_arrival": item["expected_arrival"],
            "actual_arrival": arrival_at.isoformat(timespec="seconds") if arrival_at else None,
            "arrival_status": arrival_status,
            "arrival_delay_minutes": arrival_delay_minutes,
            "expected_completion": item["expected_completion"],
            "completion_status": completion_status,
            "latest_data_timestamp": latest_data_timestamp,
            "freshness_minutes": freshness_minutes,
            "freshness_threshold_minutes": item["freshness_threshold_minutes"],
            "freshness_status": freshness_status,
            "latest_run_status": latest_run.get("status") if latest_run else None,
            "latest_run_id": latest_run.get("run_id") if latest_run else None,
        })
    return results

def refresh_alerts(db_path: str | Path = DEFAULT_DB, now: datetime | None = None) -> list[dict]:
    """Create deduplicated alerts for current monitor findings."""
    current = now or datetime.now().astimezone()
    findings = evaluate_monitoring(db_path, current)
    day_key = current.date().isoformat()
    with connect(db_path) as conn:
        for item in findings:
            alerts_to_add = []
            name = item["dataset_name"]
            if item["arrival_status"] == "Missing":
                alerts_to_add.append((
                    "missing_load", "critical",
                    f"{name} has not arrived by its expected time ({item['expected_arrival']}).",
                    "Check the source producer, delivery schedule, and file transfer logs."
                ))
            elif item["arrival_status"] == "Late":
                alerts_to_add.append((
                    "late_load", "warning",
                    f"{name} arrived late by approximately {item['arrival_delay_minutes']} minutes.",
                    "Check upstream processing and adjust the schedule only if the delay is expected."
                ))
            if item["completion_status"] in {"Failed", "Late", "Overdue"}:
                alerts_to_add.append((
                    "completion_sla", "critical" if item["completion_status"] == "Failed" else "warning",
                    f"{name} completion status is {item['completion_status'].lower()}.",
                    "Inspect the latest run details and resolve the failure or processing bottleneck."
                ))
            if item["freshness_status"] == "Stale":
                alerts_to_add.append((
                    "stale_data", "warning",
                    f"{name} data is {item['freshness_minutes']} minutes old; threshold is {item['freshness_threshold_minutes']} minutes.",
                    "Check whether the source is producing current data and whether the latest load succeeded."
                ))
            for alert_type, severity, message, action in alerts_to_add:
                alert_key = f"{day_key}:{name}:{alert_type}"
                conn.execute("""
                    INSERT OR IGNORE INTO alerts
                    (dataset_name, alert_type, severity, message, recommended_action,
                     detected_at, related_run_id, alert_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    name, alert_type, severity, message, action, current.isoformat(timespec="seconds"),
                    item.get("latest_run_id"), alert_key
                ))
    return findings
