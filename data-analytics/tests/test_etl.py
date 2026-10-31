
import subprocess
import sys
import json
import shutil
import tempfile
import importlib.util
from pathlib import Path

import pyarrow.parquet as pq


BASE_DIR = Path(__file__).resolve().parent.parent
ETL_SCRIPT = BASE_DIR / "glue" / "cold_chain_etl.py"
OUTPUT_DIR = BASE_DIR / "output"


def load_etl_module(module_name):
    spec = importlib.util.spec_from_file_location(
        module_name, ETL_SCRIPT
    )
    etl = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(etl)
    return etl


def test_no_duplicate_events():
    for _ in range(2):
        subprocess.run(
            [sys.executable, str(ETL_SCRIPT)],
            check=True
        )

    files = list(OUTPUT_DIR.rglob("*.parquet"))
    assert files, "No Parquet files were generated"

    events = []
    for file in files:
        events.extend(pq.ParquetFile(file).read().to_pylist())

    event_ids = [event["event_id"] for event in events]

    assert len(event_ids) == len(set(event_ids)), (
        "Duplicate event IDs found!"
    )

    print("PASS: No duplicate events found!")


def test_temperature_breach_flag():
    files = list(OUTPUT_DIR.rglob("*.parquet"))
    assert files, "No Parquet files found"

    events = []
    for file in files:
        events.extend(pq.ParquetFile(file).read().to_pylist())

    for event in events:
        expected = float(event["temperature"]) > 5.0

        assert event["temperature_breach"] == expected, (
            f"Incorrect breach flag for {event['event_id']}"
        )

    print("PASS: Temperature breach flags are correct!")


def test_event_time_partitioning():
    test_events = [{
        "event_id": "LATE-001",
        "sensor_id": "SENSOR-TEST",
        "truck_id": "TRUCK-TEST",
        "temperature": 9.0,
        "event_time": "2026-10-27T10:00:00Z",
        "arrival_time": "2026-10-31T10:00:00Z"
    }]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        test_input = temp_path / "sample_events.json"
        test_input.write_text(
            json.dumps(test_events),
            encoding="utf-8"
        )

        etl = load_etl_module("cold_chain_etl_late")

        etl.INPUT_FILE = test_input
        etl.OUTPUT_DIR = temp_path / "output"

        etl.transform_events()

        expected_file = (
            temp_path / "output"
            / "year=2026"
            / "month=10"
            / "day=27"
            / "events.parquet"
        )

        assert expected_file.exists(), (
            "Late event was not saved under its event_time partition"
        )

        events = pq.ParquetFile(expected_file).read().to_pylist()

        assert len(events) == 1
        assert events[0]["event_id"] == "LATE-001"
        assert events[0]["temperature_breach"] is True

        print("PASS: Late event was processed using event_time.")


def test_replay_after_failed_transformation():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        project_dir = temp_root / "data-analytics"
        glue_dir = project_dir / "glue"
        glue_dir.mkdir(parents=True)

        shutil.copy2(
            ETL_SCRIPT,
            glue_dir / "cold_chain_etl.py"
        )

        raw_copy = project_dir / "sample_events.json"
        shutil.copy2(
            BASE_DIR / "sample_events.json",
            raw_copy
        )

        output_path = project_dir / "output"
        output_path.write_text(
            "temporary obstruction",
            encoding="utf-8"
        )

        failed_run = subprocess.run(
            [sys.executable, str(glue_dir / "cold_chain_etl.py")],
            capture_output=True,
            text=True
        )

        assert failed_run.returncode != 0
        assert raw_copy.exists(), "Raw data was lost after failure"

        output_path.unlink()

        replay_run = subprocess.run(
            [sys.executable, str(glue_dir / "cold_chain_etl.py")],
            capture_output=True,
            text=True
        )

        assert replay_run.returncode == 0, replay_run.stderr

        parquet_files = list(output_path.rglob("*.parquet"))
        assert parquet_files, "Replay did not generate Parquet files"

        replayed_events = []

        for file in parquet_files:
            replayed_events.extend(
                pq.ParquetFile(file).read().to_pylist()
            )

        assert len(replayed_events) == 3
        assert raw_copy.exists()

        print("PASS: Raw data survived failure and replay succeeded.")


def test_schema_evolution_extra_field():
    event = {
        "event_id": "SCHEMA-001",
        "sensor_id": "SENSOR-003",
        "truck_id": "TRUCK-03",
        "temperature": 4.5,
        "event_time": "2026-10-30T12:00:00Z",
        "battery_level": 95
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        test_input = temp_path / "sample_events.json"
        test_input.write_text(
            json.dumps([event]),
            encoding="utf-8"
        )

        etl = load_etl_module("cold_chain_etl_schema")

        etl.INPUT_FILE = test_input
        etl.OUTPUT_DIR = temp_path / "output"

        etl.transform_events()

        output_file = (
            temp_path / "output"
            / "year=2026"
            / "month=10"
            / "day=30"
            / "events.parquet"
        )

        assert output_file.exists()

        events = pq.ParquetFile(output_file).read().to_pylist()

        assert len(events) == 1
        assert events[0]["event_id"] == "SCHEMA-001"
        assert events[0]["battery_level"] == 95
        assert events[0]["temperature"] == 4.5

        print("PASS: Schema evolution extra field handled by ETL.")


def test_duplicate_event_id_is_not_written_twice():
    test_event = {
        "event_id": "DUPLICATE-001",
        "sensor_id": "SENSOR-TEST",
        "truck_id": "TRUCK-TEST",
        "temperature": 4.5,
        "event_time": "2026-10-27T10:00:00Z"
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        test_input = temp_path / "sample_events.json"
        test_input.write_text(
            json.dumps([test_event]),
            encoding="utf-8"
        )

        etl = load_etl_module("cold_chain_etl_duplicate")

        etl.INPUT_FILE = test_input
        etl.OUTPUT_DIR = temp_path / "output"

        # Process the same event twice
        etl.transform_events()
        etl.transform_events()

        output_file = (
            temp_path / "output"
            / "year=2026"
            / "month=10"
            / "day=27"
            / "events.parquet"
        )

        events = pq.ParquetFile(output_file).read().to_pylist()

        assert len(events) == 1
        assert events[0]["event_id"] == "DUPLICATE-001"

        print("PASS: Duplicate event ID was not written twice.")


if __name__ == "__main__":
    test_no_duplicate_events()
    test_temperature_breach_flag()
    test_event_time_partitioning()
    test_replay_after_failed_transformation()
    test_schema_evolution_extra_field()
    test_duplicate_event_id_is_not_written_twice()
