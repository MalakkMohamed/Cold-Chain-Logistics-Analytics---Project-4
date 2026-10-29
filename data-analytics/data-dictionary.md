# Cold-Chain Logistics — Data Dictionary

## Overview
This dataset contains temperature and location readings collected from sensors installed in refrigerated delivery trucks.

## Fields

| Field | Data Type | Description |
|---|---|---|
| event_id | String | Unique identifier for each sensor event |
| sensor_id | String | Identifier of the sensor that generated the event |
| truck_id | String | Identifier of the delivery truck |
| temperature | Number | Temperature reading in degrees Celsius |
| latitude | Number | Latitude of the truck's location |
| longitude | Number | Longitude of the truck's location |
| event_time | ISO 8601 Timestamp | Time when the sensor event occurred |
| event_timestamp | Timestamp | Parsed event time used during processing |
| year | String | Year partition derived from the event time |
| month | String | Month partition derived from the event time |
| day | String | Day partition derived from the event time |
| temperature_breach | Boolean | True when temperature exceeds the example threshold of 5°C |

## Data Processing
- Input format: JSON
- Output format: Apache Parquet
- Compression: Snappy
- Partitioning: year/month/day
- Example breach threshold: temperature greater than 5°C

**Note:** The 5°C threshold is illustrative and must be confirmed against the project's actual cold-chain requirements before production use.
