from __future__ import annotations

from pathlib import Path
import sys
import tempfile

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.database import DEFAULT_DB, SCHEMAS, get_config, init_db, read_query, read_table
from src.pipeline import process_csv_file
from src.monitoring import refresh_alerts, evaluate_monitoring

DB_PATH = ROOT / DEFAULT_DB
init_db(DB_PATH)

def process_csv_file_from_upload(dataset_name, uploaded_file, db_path):
    # Store upload temporarily so the same parsing and run path is used for UI and CLI.
    suffix = Path(uploaded_file.name).suffix or ".csv"
    with tempfile.NamedTemporaryFile(mode="wb", suffix=suffix, delete=False) as temp:
        temp.write(uploaded_file.getvalue())
        temp_path = Path(temp.name)
    try:
        return process_csv_file(dataset_name, temp_path, db_path)
    finally:
        temp_path.unlink(missing_ok=True)


st.set_page_config(page_title="Data Pipeline Monitor", page_icon="🛠️", layout="wide")
st.title("🛠️ Data Pipeline Monitoring & Freshness")
st.caption("Monitor expected arrivals, completion SLAs, data quality, and pipeline execution.")

with st.sidebar:
    st.header("Navigation")
    page = st.radio("Choose a page", [
        "Overview", "Upload & Run", "Dataset Configuration",
        "Run History", "Data Quality & Alerts", "Published Data", "Automatic Ingestion"
    ])
    st.divider()
    st.caption("Local demo · Python + pandas + SQLite + Streamlit")
    if st.button("Refresh monitoring alerts", use_container_width=True):
        refresh_alerts(DB_PATH)
        st.success("Monitoring evaluated. Alert list refreshed.")

if page == "Overview":
    findings = refresh_alerts(DB_PATH)
    status_df = pd.DataFrame(findings)
    runs = read_query("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 10", db_path=DB_PATH)
    alerts = read_query("SELECT * FROM alerts ORDER BY detected_at DESC", db_path=DB_PATH)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Datasets monitored", len(findings))
    c2.metric("On-time arrivals", sum(x["arrival_status"] == "On time" for x in findings))
    c3.metric("Late arrivals", sum(x["arrival_status"] == "Late" for x in findings))
    c4.metric("Missing loads", sum(x["arrival_status"] == "Missing" for x in findings))

    st.subheader("Current dataset status")
    show_cols = [
        "dataset_name", "expected_arrival", "arrival_status", "arrival_delay_minutes",
        "completion_status", "freshness_minutes", "freshness_status"
    ]
    st.dataframe(status_df[show_cols], use_container_width=True, hide_index=True)
    st.caption("Arrival is based on the observed upload time in this local demo. Configure real source arrival timestamps for production use.")

    st.subheader("Recent alerts")
    if alerts.empty:
        st.info("No alerts have been recorded yet.")
    else:
        st.dataframe(alerts[["detected_at", "dataset_name", "severity", "alert_type", "message", "recommended_action"]].head(20),
                     use_container_width=True, hide_index=True)

    st.subheader("Recent pipeline runs")
    if runs.empty:
        st.info("No pipeline runs yet. Use Upload & Run to process a CSV.")
    else:
        st.dataframe(runs, use_container_width=True, hide_index=True)

elif page == "Upload & Run":
    st.subheader("Upload a dataset")
    st.write("Upload one CSV at a time. For referential integrity, process **orders before payments**.")
    dataset_name = st.selectbox("Dataset", list(SCHEMAS.keys()))
    uploaded = st.file_uploader("Choose a CSV file", type=["csv"])
    st.caption("Expected columns: " + ", ".join(SCHEMAS[dataset_name]["columns"]))

    if uploaded is not None:
        try:
            preview = pd.read_csv(uploaded)
            st.write("Input preview")
            st.dataframe(preview.head(10), use_container_width=True, hide_index=True)
            st.write(f"Rows detected: **{len(preview):,}**")
            if st.button("Validate and run pipeline", type="primary"):
                result = process_csv_file_from_upload(dataset_name, uploaded, DB_PATH)
                st.session_state["last_result"] = result
                if result["status"] == "success":
                    st.success("Pipeline run finished.")
                else:
                    st.error("Pipeline run failed: " + str(result["failure_reason"]))
                st.json(result)
                st.info("Re-uploading existing keys is safe: already-published keys are skipped.")
        except Exception as exc:
            st.error(f"Could not read this CSV: {exc}")

    st.divider()
    st.subheader("Generate demo input")
    st.write("Use the project script to create sample files with intentional data-quality issues.")
    st.code("python scripts/generate_data.py --rows 100 --seed 42", language="bash")
    st.code("python scripts/generate_data.py --rows 100000 --seed 42", language="bash")

elif page == "Dataset Configuration":
    st.subheader("Configured schedules and rules")
    configs = pd.DataFrame(get_config(DB_PATH))
    st.dataframe(configs, use_container_width=True, hide_index=True)
    st.info("Schedules are starter defaults in local time. They are stored in SQLite and can be made editable in a future enhancement.")
    st.markdown("""
**Freshness definition:** current time minus the latest timestamp in the target table.
**Arrival definition:** time the file is received by this local application.
**Completion SLA:** completion time compared with the configured daily completion deadline.
""")

elif page == "Run History":
    st.subheader("Pipeline execution history")
    runs = read_query("SELECT * FROM pipeline_runs ORDER BY started_at DESC", db_path=DB_PATH)
    if runs.empty:
        st.info("No runs recorded yet.")
    else:
        st.dataframe(runs, use_container_width=True, hide_index=True)
        selected = st.selectbox("Inspect run ID", runs["run_id"].tolist())
        detail = runs[runs["run_id"] == selected].iloc[0].to_dict()
        st.json(detail)
        if detail.get("failure_reason"):
            st.error(detail["failure_reason"])

elif page == "Data Quality & Alerts":
    st.subheader("Alerts")
    refresh_alerts(DB_PATH)
    alerts = read_query("SELECT * FROM alerts ORDER BY detected_at DESC", db_path=DB_PATH)
    if alerts.empty:
        st.info("No alerts recorded.")
    else:
        st.dataframe(alerts, use_container_width=True, hide_index=True)
        csv_bytes = alerts.to_csv(index=False).encode("utf-8")
        st.download_button("Download alerts CSV", csv_bytes, "alerts_report.csv", "text/csv")

    st.subheader("Quarantined records")
    quarantine = read_query("SELECT * FROM quarantined_records ORDER BY quarantine_id DESC", db_path=DB_PATH)
    if quarantine.empty:
        st.info("No rejected records yet.")
    else:
        st.dataframe(quarantine, use_container_width=True, hide_index=True)
        st.download_button("Download quarantine CSV", quarantine.to_csv(index=False).encode("utf-8"),
                           "quarantine_report.csv", "text/csv")

    st.subheader("Reconciliation")
    recon = read_query("""
        SELECT r.run_id, r.dataset_name, r.input_rows, r.published_rows,
               r.rejected_rows, r.skipped_existing_rows, r.status,
               (r.published_rows + r.rejected_rows + r.skipped_existing_rows) AS accounted_rows
        FROM pipeline_runs r ORDER BY r.started_at DESC
    """, db_path=DB_PATH)
    if recon.empty:
        st.info("Run the pipeline to see reconciliation results.")
    else:
        recon["reconciles"] = recon["input_rows"] == recon["accounted_rows"]
        st.dataframe(recon, use_container_width=True, hide_index=True)

elif page == "Published Data":
    st.subheader("Published target records")
    target_dataset = st.selectbox("Choose target table", list(SCHEMAS.keys()))
    target = read_table(target_dataset, DB_PATH)
    st.metric("Published records", f"{len(target):,}")
    st.dataframe(target, use_container_width=True, hide_index=True)
    if not target.empty:
        st.download_button(f"Download {target_dataset} target CSV",
                           target.to_csv(index=False).encode("utf-8"),
                           f"{target_dataset}_target.csv", "text/csv")


elif page == "Automatic Ingestion":
    st.subheader("Automatic local-folder ingestion")
    incoming_dir = ROOT / "data" / "incoming"
    processed_dir = ROOT / "data" / "processed"
    failed_dir = ROOT / "data" / "failed"
    for folder in (incoming_dir, processed_dir, failed_dir):
        folder.mkdir(parents=True, exist_ok=True)
    incoming_files = sorted(p.name for p in incoming_dir.glob("*.csv"))
    processed_files = sorted(p.name for p in processed_dir.glob("*.csv"))
    failed_files = sorted(p.name for p in failed_dir.glob("*.csv"))
    a, b, c = st.columns(3)
    a.metric("Waiting in incoming", len(incoming_files))
    b.metric("Processed files", len(processed_files))
    c.metric("Failed files", len(failed_files))
    st.write("**Incoming folder:** `data/incoming/`")
    st.write("**Processed folder:** `data/processed/`")
    st.write("**Failed folder:** `data/failed/`")
    st.markdown("""
Start the watcher in a separate Terminal window from the project folder:

```bash
source .venv/bin/activate
python scripts/watch_folder.py
```

Then copy a CSV into `data/incoming/` from another Terminal window:

```bash
cp data/input/orders.csv data/incoming/orders.csv
```

The watcher detects stable files, processes them automatically, archives the source file, and refreshes SLA monitoring periodically. Keep the watcher Terminal running. Stop it with `Control+C`.

**Filename rule:** `orders.csv`, `inventory.csv`, `payments.csv`, or a batch name such as `orders__batch_002.csv`. Process orders before payments.
""")
    st.subheader("Files currently waiting")
    if incoming_files:
        st.write(incoming_files)
    else:
        st.info("No CSV files are waiting in the incoming folder.")
    with st.expander("Recent processed files"):
        st.write(processed_files[-20:] if processed_files else "No processed files yet.")
    with st.expander("Files requiring investigation"):
        st.write(failed_files[-20:] if failed_files else "No failed files.")
