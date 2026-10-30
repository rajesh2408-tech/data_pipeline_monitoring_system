#!/usr/bin/env python3
"""Poll a local incoming folder, process stable CSV files, and refresh SLA alerts."""
from __future__ import annotations

import argparse
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database import DEFAULT_DB, SCHEMAS, init_db
from src.pipeline import process_csv_file
from src.monitoring import refresh_alerts

DATA_DIR = ROOT / "data"
INCOMING = DATA_DIR / "incoming"
SAMPLE_INPUT = DATA_DIR / "input"
SAMPLE_SEED_MARKER = DATA_DIR / ".sample_data_seeded"
PROCESSED = DATA_DIR / "processed"
FAILED = DATA_DIR / "failed"
LOGS = ROOT / "logs"
DB_PATH = ROOT / DEFAULT_DB


def dataset_from_filename(path: Path) -> str | None:
    """Accept orders.csv or orders__batch_001.csv (same for inventory/payments)."""
    prefix = path.stem.split("__", 1)[0].lower()
    return prefix if prefix in SCHEMAS else None


def is_stable(path: Path, previous_sizes: dict[str, tuple[int, float, float]], stable_seconds: float) -> bool:
    """Only process files whose size and modification time stay unchanged for stable_seconds."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return False
    now = time.monotonic()
    prior = previous_sizes.get(str(path))
    if prior is None or prior[0] != stat.st_size or prior[1] != stat.st_mtime:
        previous_sizes[str(path)] = (stat.st_size, stat.st_mtime, now)
        return False
    return now - prior[2] >= stable_seconds


def move_without_overwrite(source: Path, destination_dir: Path) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / source.name
    if destination.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        destination = destination_dir / f"{source.stem}_{stamp}{source.suffix}"
    return Path(shutil.move(str(source), str(destination)))


def seed_sample_files_once() -> int:
    """On first start, automatically queue bundled sample datasets for a no-upload demo.

    This is a one-time local-demo seed, not a substitute for connecting a production source.
    """
    if SAMPLE_SEED_MARKER.exists():
        return 0
    INCOMING.mkdir(parents=True, exist_ok=True)
    queued = 0
    for name in ("orders.csv", "inventory.csv", "payments.csv"):
        source = SAMPLE_INPUT / name
        destination = INCOMING / name
        if source.exists() and not destination.exists():
            shutil.copy2(source, destination)
            queued += 1
            logging.info("Automatically queued bundled sample: %s", name)
    SAMPLE_SEED_MARKER.write_text(
        "Bundled sample datasets were automatically queued on first watcher start.\n",
        encoding="utf-8",
    )
    return queued


def process_ready_files(previous_sizes: dict[str, tuple[int, float, float]], stable_seconds: float) -> int:
    INCOMING.mkdir(parents=True, exist_ok=True)
    ready = []
    for path in INCOMING.glob("*.csv"):
        dataset = dataset_from_filename(path)
        if dataset is None:
            logging.warning("Ignoring %s: filename must start with orders, inventory, or payments", path.name)
            continue
        if is_stable(path, previous_sizes, stable_seconds):
            ready.append((dataset, path))

    # Process dependencies first so payments can reference newly published orders.
    order = {"orders": 0, "inventory": 1, "payments": 2}
    ready.sort(key=lambda item: (order[item[0]], item[1].name.lower()))
    processed_count = 0
    for dataset, path in ready:
        if not path.exists():
            continue
        logging.info("Detected stable file: %s (dataset=%s)", path.name, dataset)
        result = process_csv_file(dataset, path, DB_PATH)
        if result["status"] == "success":
            destination = move_without_overwrite(path, PROCESSED)
            logging.info(
                "SUCCESS %s -> %s | input=%s published=%s rejected=%s skipped=%s",
                path.name, destination.name, result["input_rows"], result["published_rows"],
                result["rejected_rows"], result["skipped_existing_rows"]
            )
        else:
            destination = move_without_overwrite(path, FAILED)
            logging.error("FAILED %s -> %s | %s", path.name, destination.name, result.get("failure_reason"))
        previous_sizes.pop(str(path), None)
        processed_count += 1
    return processed_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Automatically ingest CSV files from data/incoming")
    parser.add_argument("--poll-seconds", type=float, default=5, help="How often to scan the folder (default: 5)")
    parser.add_argument("--stable-seconds", type=float, default=2, help="Wait for file size to stop changing (default: 2)")
    parser.add_argument("--monitor-seconds", type=float, default=30, help="How often to refresh SLA alerts (default: 30)")
    args = parser.parse_args()
    if args.poll_seconds <= 0 or args.stable_seconds < 0 or args.monitor_seconds <= 0:
        parser.error("poll/monitor seconds must be positive and stable-seconds cannot be negative")

    for folder in (INCOMING, PROCESSED, FAILED, LOGS, DB_PATH.parent):
        folder.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOGS / "folder_watcher.log")],
    )
    init_db(DB_PATH)
    seeded = seed_sample_files_once()
    if seeded:
        logging.info("Automatically queued %d bundled sample datasets; no manual copy/upload needed for the first demo", seeded)
    logging.info("Folder watcher started. Watching: %s", INCOMING)
    logging.info("Filename format: orders.csv / inventory.csv / payments.csv (or dataset__batch.csv)")
    logging.info("Stop with Control+C. Dashboard: streamlit run app.py")

    previous_sizes: dict[str, tuple[int, float, float]] = {}
    last_monitor = 0.0
    try:
        while True:
            process_ready_files(previous_sizes, args.stable_seconds)
            now = time.monotonic()
            if now - last_monitor >= args.monitor_seconds:
                findings = refresh_alerts(DB_PATH)
                logging.info("Monitoring refreshed for %d configured datasets", len(findings))
                last_monitor = now
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        logging.info("Folder watcher stopped by user")


if __name__ == "__main__":
    main()
