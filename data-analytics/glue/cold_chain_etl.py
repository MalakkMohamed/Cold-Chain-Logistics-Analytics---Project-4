
import json
from pathlib import Path
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "sample_events.json"
OUTPUT_DIR = BASE_DIR / "output"


def transform_events():
    # Read sample sensor events
    with INPUT_FILE.open("r", encoding="utf-8") as file:
        content = file.read().strip()

    # Support both JSON arrays/objects and JSON Lines
    try:
        events = json.loads(content)

        if isinstance(events, dict):
            events = [events]

    except json.JSONDecodeError:
        events = [
            json.loads(line)
            for line in content.splitlines()
            if line.strip()
        ]

    valid_events = []

    required = [
        "event_id",
        "sensor_id",
        "truck_id",
        "temperature",
        "event_time",
    ]

    for event in events:
        if any(event.get(key) is None for key in required):
            print(f"Skipping invalid event: {event.get('event_id')}")
            continue

        timestamp = datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        )

        event["temperature"] = float(event["temperature"])
        event["event_timestamp"] = timestamp
        event["year"] = f"{timestamp.year:04d}"
        event["month"] = f"{timestamp.month:02d}"
        event["day"] = f"{timestamp.day:02d}"
        event["temperature_breach"] = (
            event["temperature"] > 5.0
        )

        valid_events.append(event)

    # Write Parquet files partitioned by event date
    for event in valid_events:
        partition = (
            OUTPUT_DIR
            / f"year={event['year']}"
            / f"month={event['month']}"
            / f"day={event['day']}"
        )
        partition.mkdir(parents=True, exist_ok=True)

        output_file = partition / "events.parquet"

        # Preserve existing events and avoid duplicate IDs
        existing = []
        if output_file.exists():
            existing = pq.ParquetFile(output_file).read().to_pylist()
        by_id = {row["event_id"]: row for row in existing}
        by_id[event["event_id"]] = event

        table = pa.Table.from_pylist(list(by_id.values()))
        pq.write_table(table, output_file, compression="snappy")

    print(f"Processed {len(valid_events)} valid events.")
    print(f"Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    transform_events()
