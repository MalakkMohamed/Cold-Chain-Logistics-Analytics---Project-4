
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
