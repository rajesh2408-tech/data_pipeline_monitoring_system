# Data Pipeline Monitoring and Freshness Alert System

A local Data Engineering project that ingests CSV datasets, validates schemas and business rules, quarantines invalid records, publishes valid rows to SQLite, tracks run metrics, monitors arrival/completion SLAs and freshness, and exposes results through a Streamlit dashboard.

## Technology

- Python 3.10+
- Streamlit for the local dashboard
- pandas for CSV processing
- SQLite for durable local metadata and target tables
- pytest for automated tests

No cloud account or machine-learning model is required.

## Architecture

```text
Scheduled source / bundled sample seed / incoming folder
   |
   v
Ingestion -> Schema validation -> Row/business-rule validation
                                    |                 |
                                    v                 v
                              Valid records      Quarantine + reasons
                                    |
                                    v
                              SQLite targets
                                    |
                                    v
                Run history + arrival monitoring + freshness
                                    |
                                    v
                          Streamlit dashboard + alerts
```

The monitoring logic is separate from the ingestion logic. A file's observed upload time is not necessarily its original source arrival time; this local demo uses the upload time as the observed arrival timestamp. A production integration should record a trusted source-arrival timestamp from the producer or orchestrator.

## Dataset schemas

### `orders.csv`

| Column | Type | Rules |
|---|---|---|
| `order_id` | string | Required, unique business key |
| `customer_id` | string | Required, non-empty |
| `amount` | decimal | Required, numeric, >= 0 |
| `order_timestamp` | ISO-8601 timestamp | Required, parseable |

### `inventory.csv`

| Column | Type | Rules |
|---|---|---|
| `product_id` | string | Required, unique business key |
| `stock_quantity` | integer | Required, non-negative integer |
| `updated_at` | ISO-8601 timestamp | Required, parseable |

### `payments.csv`

| Column | Type | Rules |
|---|---|---|
| `payment_id` | string | Required, unique business key |
| `order_id` | string | Required; must exist in published `orders` |
| `amount` | decimal | Required, numeric, >= 0 |
| `payment_status` | string | One of `paid`, `pending`, `failed`, `refunded` |
| `payment_timestamp` | ISO-8601 timestamp | Required, parseable |

The business grain is one row per order, one row per product inventory snapshot key (the demo keeps one current row per product), and one row per payment. For a production inventory history, use a composite key or append-only event table to preserve each snapshot.

## IDs and naming

- Dataset names, tables, and columns use `snake_case`.
- Each pipeline execution has a UUID `run_id`.
- Business keys such as `order_id`, `product_id`, and `payment_id` remain stable.
- Timestamps are stored as ISO-8601 strings with timezone offsets where available.
- Every target and quarantine record records `source_file` and `run_id`.

## Default monitoring rules

The starter configuration in `src/database.py` is:
- orders: expected arrival 09:00, completion by 09:15, freshness threshold 60 minutes
- inventory: expected arrival 09:15, completion by 09:30, freshness threshold 120 minutes
- payments: expected arrival 09:30, completion by 09:45, freshness threshold 60 minutes

These are illustrative local-time defaults. Adjust them to the business schedule. Monitoring evaluates today's schedule. For production, store a timezone explicitly and use an orchestrator/source event to report arrival and completion accurately.

Freshness is calculated as the evaluation time minus the maximum timestamp in the target dataset. Arrival delay and processing duration are separate metrics.

## Setup on macOS

Open Terminal and move into the extracted project folder.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Generate sample files:

```bash
python scripts/generate_data.py --rows 100 --seed 42
```

For a larger local test:

```bash
python scripts/generate_data.py --rows 100000 --seed 42
```

**Automatic re-generation and ingestion:** when the folder watcher is running, every run of `generate_data.py` now queues the newly generated `orders`, `inventory`, and `payments` CSVs into `data/incoming/` under unique batch filenames. You do not need to manually copy files. The watcher processes them automatically. Existing business keys are skipped by the idempotency rules, while newly generated keys can be published. With the same seed, increasing rows from 100 to 100000 adds the higher-numbered keys; it does not replace existing records. Use `--no-queue` if you only want to generate files in `data/input/`. Always keep the watcher running in one terminal while generating data from another.

Start the dashboard:

```bash
streamlit run app.py
```

Streamlit prints a local URL, usually `http://localhost:8501`. Open that URL in your browser.

Run the tests:

```bash
python -m pytest -q
```

## Suggested demo order

1. Generate the sample files.
2. Open the dashboard.
3. Select **Upload & Run**.
4. Upload `orders.csv` and run it first.
5. Upload `inventory.csv`.
6. Upload `payments.csv` after orders, because payment order IDs are checked against published orders.
7. Open **Data Quality & Alerts** to inspect quarantine reasons and reconciliation.
8. Open **Run History** and **Published Data** to review persisted outputs.
9. Upload the same file again to demonstrate idempotency: existing business keys are skipped rather than inserted again.

The generator intentionally adds labelled examples of invalid values and duplicates. `expected_issue_labels.csv` is separate from the input datasets and is not consumed by the pipeline.

## Operational behavior

- Schema mismatch: the run is marked failed and no rows from that batch are published.
- Invalid row: the row is stored in `quarantined_records` with its source row number and rejection reason.
- Duplicate key inside a batch: the first valid occurrence is published; subsequent occurrences are quarantined.
- Re-upload of an existing key: the row is skipped, so target keys do not duplicate.
- Empty batch: succeeds with zero input rows if the required headers are present.
- Invalid CSV syntax: records a failed run with the parse error.
- Referential integrity: payments are checked against already-published orders, so load orders before payments.
- Partial processing failure: SQLite transactions protect the batch's database changes; the run record is updated with a failure reason where possible. Retry after correcting the cause.
- Backfill: upload historical CSVs with their original event timestamps. Target keys are unique, so already-published keys are skipped; this starter version does not implement change-data-capture/upserts.

## Output and reconciliation

The dashboard exposes:
- published target rows
- quarantined records and rejection reasons
- run status, input/output/reject/skip counts, duration, and failure reason
- arrival, completion, and freshness statuses
- alerts with recommended actions
- reconciliation: `input_rows = published_rows + rejected_rows + skipped_existing_rows`

An uploaded file's receipt time is the local application's observed upload time. Do not interpret it as the producer's original arrival time in a production system without a trusted source timestamp.

## Performance measurement and optimization

Each run records elapsed processing time using `time.perf_counter()`. The data generator supports 100,000+ rows with a fixed random seed. For larger production loads, a reasonable next optimization is batched inserts (`executemany`) and chunked CSV reading instead of row-by-row inserts.

## Design trade-offs and limitations

- Streamlit was selected to keep the focus on data engineering rather than separate frontend/backend work.
- SQLite is suitable for a reproducible local demonstration; concurrent production workloads may require PostgreSQL or a cloud warehouse.
- Rules are deterministic; no ML model is trained or needed.
- The demo uses daily local-time schedules and evaluates current-day status. A production system should support explicit time zones, holidays, schedule calendars, late-data grace periods, and orchestration events.
- The alert table deduplicates a given alert type per dataset per day. A production alert lifecycle should also support resolution/clearing.
- `customer_id` is required but this starter project does not include a customer master dataset, so customer reference validation is not implemented. Payment-to-order referential integrity is implemented.
- Inventory stores the current row per product. If historical snapshots matter, use an event/history table rather than overwriting.
- The app is intended for local use and does not include authentication or authorization.

## Folder structure

```text
app.py
src/
  database.py
  pipeline.py
  monitoring.py
scripts/
  generate_data.py
tests/
  test_pipeline.py
data/
  input/
  output/
  quarantine/
reports/
README.md
requirements.txt
```

## Automatic local-folder ingestion (new)

The project now includes a separate background folder watcher. It watches `data/incoming/`, waits until each CSV has stopped changing, detects the dataset from the filename, processes it through the same validation/publishing code, moves successful runs to `data/processed/`, and moves pipeline-level failures to `data/failed/`. It refreshes SLA monitoring on a timer even when no files arrive, so missing-arrival alerts can be generated without opening the dashboard.

### Start the local demonstration

Open **Terminal window 1** in the project folder and activate your environment:

```bash
source .venv/bin/activate
python scripts/watch_folder.py
```

if it not working use
```bash
source .venv/bin/activate
python3 scripts/watch_folder.py
```


Keep this terminal running. It checks for files every 5 seconds and waits for a file to be unchanged for 2 seconds before reading it. Monitoring alerts are refreshed every 30 seconds by default. You can tune these values, for example:

```bash
python scripts/watch_folder.py --poll-seconds 3 --stable-seconds 2 --monitor-seconds 15
```

Open **Terminal window 2** in the same project folder and start the dashboard:

```bash
source .venv/bin/activate
streamlit run app.py
```

### Start the no-manual-upload local demo

On the first watcher start, the project automatically copies the bundled `orders.csv`, `inventory.csv`, and `payments.csv` samples from `data/input/` into `data/incoming/` once. The watcher then detects stable files, processes them in dependency order, validates them, publishes valid rows, quarantines invalid rows, and moves the source files to `data/processed/` or `data/failed/`. You do not need to manually upload or copy the three sample files for the first demo.

1. Start the watcher in Terminal window 1:

   ```bash
   source .venv/bin/activate
   python scripts/watch_folder.py
   ```

2. Start the dashboard in Terminal window 2:

   ```bash
   source .venv/bin/activate
   streamlit run app.py
   ```

3. Check **Current dataset status**, **Recent alerts**, **Recent pipeline runs**, **Published Data**, and **Data Quality & Alerts**.

The bundled sample seed runs only once, to avoid reprocessing the same demo files on every restart. To reset the demo seed, stop the watcher, remove `data/.sample_data_seeded`, and move or clear the files in `data/processed/` and `data/incoming/` as appropriate. Business-key idempotency prevents duplicate target rows, but a reset can create additional run records.

For new data, an upstream source or scheduled job must deliver files to `data/incoming/`. Supported names are `orders.csv`, `inventory.csv`, `payments.csv`, or batch names such as `orders__batch_002.csv`. Process orders before payments because payment validation checks the published orders table. This local project does not connect to an external production source by itself.

### Folder rules and behavior

- Incoming folder: `data/incoming/`
- Successfully processed files: `data/processed/`
- Files with a pipeline-level failure: `data/failed/`
- Watcher log: `logs/folder_watcher.log`
- SQLite database: `data/pipeline_monitor.db`
- Supported filenames: `orders.csv`, `inventory.csv`, `payments.csv`, or names like `orders__batch_002.csv`.
- CSVs with row-level validation errors can still have a successful pipeline run; rejected rows are stored in quarantine and the source file moves to `processed/` so the same bad batch does not retry forever.
- The watcher is a polling process. It must remain running for automatic ingestion and scheduled monitoring. The bundled sample files are auto-queued once on first startup; future real-world arrivals still need a configured upstream source to place files in the incoming folder. Stop it with `Control+C`.
- This is a local demonstration, not a production orchestrator. A production system would typically use managed scheduling/eventing, durable retries, operational alert delivery, and source-provided arrival timestamps.
