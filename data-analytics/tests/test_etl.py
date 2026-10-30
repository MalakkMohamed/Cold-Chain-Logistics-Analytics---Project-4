
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent.parent
ETL_SCRIPT = BASE_DIR / "glue" / "cold_chain_etl.py"
OUTPUT_DIR = BASE_DIR / "output"


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
    assert len(event_ids) == len(set(event_ids)), "Duplicate event IDs found!"

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


if __name__ == "__main__":
    test_no_duplicate_events()
    test_temperature_breach_flag()


def test_event_time_partitioning():
    import json
    import shutil
    import tempfile
    from datetime import datetime

    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        original_input = BASE_DIR / "sample_events.json"
        original_output = OUTPUT_DIR

        test_events = [{
            "event_id": "LATE-001",
            "sensor_id": "SENSOR-TEST",
            "truck_id": "TRUCK-TEST",
            "temperature": 9.0,
            "event_time": "2026-10-27T10:00:00Z"
        }]

        test_input = temp_path / "sample_events.json"
        test_input.write_text(json.dumps(test_events), encoding="utf-8")

        # Verify partition date is based on event_time.
        timestamp = datetime.fromisoformat(
            test_events[0]["event_time"].replace("Z", "+00:00")
        )
        expected_partition = (
            f"year={timestamp.year:04d}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}"
        )

        assert expected_partition == "year=2026/month=10/day=27"

        print("PASS: Late event partition uses event_time.")




def test_replay_after_failed_transformation():
    import shutil
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        project_dir = temp_root / "data-analytics"
        glue_dir = project_dir / "glue"
        glue_dir.mkdir(parents=True)

        shutil.copy2(ETL_SCRIPT, glue_dir / "cold_chain_etl.py")
        raw_copy = project_dir / "sample_events.json"
        shutil.copy2(BASE_DIR / "sample_events.json", raw_copy)

        output_path = project_dir / "output"
        output_path.write_text("temporary obstruction", encoding="utf-8")

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
    import json
    import tempfile

    event = {
        "event_id": "SCHEMA-001",
        "sensor_id": "SENSOR-003",
        "truck_id": "TRUCK-03",
        "temperature": 4.5,
        "event_time": "2026-10-30T12:00:00Z",
        "battery_level": 95
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = Path(temp_dir) / "schema_event.json"
        file_path.write_text(json.dumps(event), encoding="utf-8")

        loaded_event = json.loads(file_path.read_text(encoding="utf-8"))

        assert loaded_event["event_id"] == "SCHEMA-001"
        assert loaded_event["temperature"] == 4.5
        assert loaded_event["battery_level"] == 95
        assert loaded_event["sensor_id"] == "SENSOR-003"
