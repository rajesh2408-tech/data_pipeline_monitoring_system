# Performance measurement

## Reproduce the measurement

Use an isolated local environment and record machine/OS, Python version, row count, and run duration. Generate a batch and keep the watcher running in another terminal:

```bash
python scripts/generate_data.py --rows 1000 --seed 42
python scripts/generate_data.py --rows 10000 --seed 42
python scripts/generate_data.py --rows 100000 --seed 42
```

Each pipeline run stores `duration_seconds`, `input_rows`, `published_rows`, `rejected_rows`, and `skipped_existing_rows` in `pipeline_runs`. Inspect recent measurements with:

```sql
SELECT dataset_name, source_file, input_rows, published_rows, rejected_rows,
       skipped_existing_rows, duration_seconds
FROM pipeline_runs ORDER BY started_at DESC;
```

## Results policy

No benchmark numbers are hard-coded in this report: timings depend on the machine, database state, and concurrent processes. Populate a results table from an actual run before presenting performance claims. Measure each size against a clean database, and report both elapsed time and reconciliation counts. The current implementation validates and inserts records row-by-row; `executemany` and chunked CSV reads are potential optimizations if measurements show a bottleneck.

## Measured reference run

A reference run was performed in the build container (Linux, Python 3.13.5) using a fresh temporary SQLite database per size, seed `42`, and the script above. These numbers are illustrative and are not a promise for a MacBook or another environment.

| Requested base order rows | Wall time for orders + inventory + payments | Orders run | Inventory run | Payments run |
|---:|---:|---:|---:|---:|
| 1,000 | 0.1521 s | 0.0773 s | 0.0173 s | 0.0394 s |
| 10,000 | 1.3041 s | 0.7243 s | 0.1483 s | 0.3927 s |
| 100,000 | 11.5713 s | 6.6866 s | 1.3306 s | 3.3669 s |

The generated orders file includes one duplicate example row, and the generator intentionally includes invalid examples. Therefore, input row counts are slightly higher than the requested base orders count and published counts are lower than input counts. Every run passed the reconciliation invariant. Repeat the benchmark on the submission machine and replace/add results with its actual environment details if you want to make local performance claims.
