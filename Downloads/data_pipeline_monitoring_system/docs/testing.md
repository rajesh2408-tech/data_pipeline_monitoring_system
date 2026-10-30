# Testing

Run from the project root after installing `requirements.txt`:

```bash
python -m pytest -q
```

The automated suite covers valid publishing, idempotent reprocessing, empty batches, missing required values, duplicate keys, schema mismatch, invalid business values/timestamps, payment-to-order referential integrity, reconciliation, missing-load detection, on-time arrival, previous-day late arrival, watcher filename/stability behavior, one-time sample seeding, and a generator-to-watcher integration flow.

Reconciliation invariant for each run:

`input_rows = published_rows + rejected_rows + skipped_existing_rows`

The integration test uses a temporary SQLite database and temporary folders so it does not modify the developer's local project data.
