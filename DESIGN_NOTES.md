# Design and trade-off note

## Decisions
1. Streamlit keeps the UI in Python and is fast to run locally.
2. SQLite gives durable run history, target data, quarantine records, and alerts without requiring a server.
3. Deterministic rules make SLA and data-quality outcomes explainable and testable.
4. The business key in each target table enforces uniqueness; repeated uploads skip keys already published.
5. Quarantine retains source filename, row number, record JSON, run ID, and rejection reason.

## Important semantics
- Arrival time is the time the local app receives the upload. Production should capture source/orchestrator event time.
- Freshness uses the latest event/update timestamp in the published target.
- Arrival SLA and completion SLA are independent.
- A successful run can still have rejected rows. Run status describes processing execution; row counts describe data quality.

## Limitations
- Daily schedule defaults are illustrative and use local time.
- Alert rows are deduplicated per dataset/type/day but are not automatically resolved.
- No UI for editing schedules yet; values can be updated in SQLite or `DATASET_CONFIG`.
- The current inventory target keeps one row per product, not full history.
- No authentication, distributed orchestration, cloud storage, or ML is included.
