# Incident Runbook - Missing Sensor Data

## Incident
A sensor stops sending temperature readings.

## Detection
1. Check the latest event timestamp for the sensor.
2. Compare it with the expected one-minute reporting interval.
3. Identify sensors with no recent events.

## Investigation
1. Verify the sensor ID and truck ID.
2. Check the Kinesis stream for incoming records.
3. Review Lambda and CloudWatch logs for errors.
4. Check connectivity and sensor power status with the operations team.

## Response
1. Notify the operations team about the missing sensor.
2. Record the sensor ID, truck ID, and last event timestamp.
3. Investigate connectivity or hardware issues.
4. Restore the sensor connection.
5. Confirm that new events are received.

## Recovery
1. Retrieve available buffered or retained raw events.
2. Replay missing events through the processing pipeline.
3. Verify the resulting records in the curated data.

## Evidence and Escalation
Record the incident time, affected sensor, investigation results, recovery actions, and resolution time.
Escalate unresolved hardware or connectivity issues to the responsible operations team.

## Current Implementation Status
The missing-sensor Athena query is prepared but has not been executed against a deployed Glue table.
