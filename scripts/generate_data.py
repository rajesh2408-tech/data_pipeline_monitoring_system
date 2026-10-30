from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import shutil

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input"
INCOMING_DIR = ROOT / "data" / "incoming"

def generate_samples(output_dir: Path = INPUT_DIR, rows: int = 100, seed: int = 42, queue_for_ingestion: bool = True) -> None:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)

    customers = [f"C{i:05d}" for i in range(1, max(rows // 2, 10) + 1)]
    orders = []
    for i in range(rows):
        orders.append({
            "order_id": f"O{i+1:07d}",
            "customer_id": rng.choice(customers),
            "amount": round(rng.uniform(10, 2500), 2),
            "order_timestamp": (now - timedelta(minutes=rng.randint(0, 180))).isoformat(),
        })
    if rows >= 8:
        # Deliberately labelled examples are kept in the input; validation should reject them.
        orders[1]["amount"] = -25.0
        orders[2]["customer_id"] = ""
        orders.append(dict(orders[0]))  # duplicate key inside the same batch
    pd.DataFrame(orders).to_csv(output_dir / "orders.csv", index=False)

    inventory = []
    for i in range(max(10, rows // 5)):
        inventory.append({
            "product_id": f"P{i+1:06d}",
            "stock_quantity": rng.randint(0, 500),
            "updated_at": (now - timedelta(minutes=rng.randint(0, 240))).isoformat(),
        })
    if len(inventory) >= 3:
        inventory[1]["stock_quantity"] = -4
        inventory[2]["updated_at"] = "not-a-timestamp"
    pd.DataFrame(inventory).to_csv(output_dir / "inventory.csv", index=False)

    payments = []
    for i in range(max(10, rows // 2)):
        # Valid references point to generated order IDs; one deliberate invalid reference is added below.
        order_num = rng.randint(1, max(rows, 1))
        payments.append({
            "payment_id": f"PAY{i+1:07d}",
            "order_id": f"O{order_num:07d}",
            "amount": round(rng.uniform(10, 2500), 2),
            "payment_status": rng.choice(["paid", "pending", "failed"]),
            "payment_timestamp": (now - timedelta(minutes=rng.randint(0, 180))).isoformat(),
        })
    if len(payments) >= 2:
        payments[1]["order_id"] = "O9999999"  # deliberate invalid reference
    pd.DataFrame(payments).to_csv(output_dir / "payments.csv", index=False)

    # Separate expected-answer labels for teaching/reconciliation; never used as pipeline input.
    labels = pd.DataFrame([
        {"file": "orders.csv", "example_issue": "negative amount, missing customer_id, duplicate order_id"},
        {"file": "inventory.csv", "example_issue": "negative stock quantity, malformed timestamp"},
        {"file": "payments.csv", "example_issue": "order_id not present in published orders target"},
    ])
    labels.to_csv(output_dir / "expected_issue_labels.csv", index=False)
    print(f"Generated reproducible sample files in: {output_dir}")
    print("Seed:", seed, "| Requested base order rows:", rows)
    if queue_for_ingestion:
        # Queue each generated set under unique batch names so the running watcher
        # sees a new file event even when the generator overwrites data/input/*.csv.
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        for name in ("orders.csv", "inventory.csv", "payments.csv"):
            source = output_dir / name
            destination = INCOMING_DIR / f"{source.stem}__generated_{batch_id}{source.suffix}"
            shutil.copy2(source, destination)
            print(f"Queued for automatic ingestion: {destination.name}")
        print("The folder watcher will process these files automatically if it is running.")
    else:
        print("Automatic queueing disabled; files remain in the output directory.")
    print("Load orders before payments because payments validate order references.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate reproducible synthetic pipeline input files.")
    parser.add_argument("--rows", type=int, default=100, help="Number of base order rows (use 100000 for a larger run)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=INPUT_DIR)
    parser.add_argument("--no-queue", action="store_true", help="Generate files without queueing them for automatic ingestion")
    args = parser.parse_args()
    generate_samples(args.output_dir, args.rows, args.seed, queue_for_ingestion=not args.no_queue)
