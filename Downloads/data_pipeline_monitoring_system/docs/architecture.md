# Architecture

## Flow

1. `scripts/generate_data.py` creates synthetic CSV batches in `data/input/` and, by default, copies each batch to `data/incoming/` with unique batch filenames.
2. `scripts/watch_folder.py` polls the incoming folder, waits for stable files, and processes dependencies in this order: orders, inventory, payments.
3. `src/pipeline.py` validates required columns and row-level business rules. Invalid rows are written to `quarantined_records`; valid rows are written to SQLite target tables. Existing primary keys are skipped.
4. `src/database.py` persists pipeline runs, arrival events, scheduled load ledger, alerts, quarantine records, and target data.
5. `src/monitoring.py` evaluates schedule-specific arrival/completion status and target-data freshness and inserts deduplicated actionable alerts.
6. `app.py` displays monitoring, run history, data quality, alerts, and published data in Streamlit.

## SLA assignment

A rolling eight-day schedule ledger is maintained for each dataset. Arrival events are matched to the oldest unfulfilled scheduled load whose expected arrival deadline has passed. This supports a prior-day load arriving the next morning; for example, an expected arrival at 09:00 on 28 October and arrival at 07:00 on 29 October is 1,320 minutes late. The demo uses the machine's local timezone; production deployments should configure an explicit business timezone and trusted source-arrival event.

## Idempotency

Every generated invocation receives a batch token embedded in business keys and filenames. Reprocessing the same CSVs therefore skips existing keys, while a newly generated invocation produces new keys. This is batch-level idempotency, not source-level deduplication across independently regenerated business events.

## Local demo boundaries

SQLite and a local filesystem are used; no cloud source, scheduler, authentication, or production notification provider is connected. The first watcher startup may queue bundled samples once for the demo. Observed file receipt time is not proof of upstream source arrival time.
