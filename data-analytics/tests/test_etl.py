
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
