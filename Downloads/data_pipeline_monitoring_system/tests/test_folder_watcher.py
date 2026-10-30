from pathlib import Path

from scripts.watch_folder import dataset_from_filename, is_stable


def test_dataset_name_from_supported_filename():
    assert dataset_from_filename(Path("orders.csv")) == "orders"
    assert dataset_from_filename(Path("payments__batch_002.csv")) == "payments"
    assert dataset_from_filename(Path("other.csv")) is None


def test_file_must_be_stable_before_processing(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,customer_id,amount,order_timestamp\n")
    observed = {}
    assert is_stable(path, observed, stable_seconds=10) is False
    # Simulate the stability interval having elapsed.
    size, modified, _ = observed[str(path)]
    observed[str(path)] = (size, modified, 0.0)
    assert is_stable(path, observed, stable_seconds=0) is True


def test_bundled_samples_are_queued_once_without_manual_copy(tmp_path, monkeypatch):
    import scripts.watch_folder as watcher

    sample_dir = tmp_path / "input"
    incoming_dir = tmp_path / "incoming"
    sample_dir.mkdir()
    for name in ("orders.csv", "inventory.csv", "payments.csv"):
        (sample_dir / name).write_text("sample,data\n1,2\n")

    marker = tmp_path / ".sample_data_seeded"
    monkeypatch.setattr(watcher, "SAMPLE_INPUT", sample_dir)
    monkeypatch.setattr(watcher, "INCOMING", incoming_dir)
    monkeypatch.setattr(watcher, "SAMPLE_SEED_MARKER", marker)

    assert watcher.seed_sample_files_once() == 3
    assert sorted(p.name for p in incoming_dir.glob("*.csv")) == ["inventory.csv", "orders.csv", "payments.csv"]
    assert watcher.seed_sample_files_once() == 0
