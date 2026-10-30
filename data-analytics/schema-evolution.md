# Schema Evolution Decision

## Purpose
Sensor events may contain new fields as the cold-chain platform evolves.

## Example
A future event may include an additional field:
- battery_level: sensor battery percentage.

## Decision
Use an additive schema evolution approach:
1. Keep existing field names and data types stable.
2. Allow new optional fields to be introduced.
3. Do not rename or remove existing fields without a migration plan.
4. Validate required fields before processing.
5. Update the Glue Data Catalog schema when new fields are added.
6. Test compatibility with old and new event formats before deployment.

## Compatibility
- Older events may not contain newly introduced optional fields.
- New events may contain additional fields.
- Consumers should handle missing optional fields safely.

## Current Implementation Status
The local schema test verifies that an additional JSON field can be read.
Full Parquet and AWS Glue Catalog schema evolution has not yet been tested.
