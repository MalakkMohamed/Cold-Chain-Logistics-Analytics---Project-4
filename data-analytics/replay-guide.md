# Replay and Recovery Guide

## Purpose
This guide explains how to recover raw sensor events after an ETL failure.

## Recovery Procedure
1. Keep the original raw JSON files unchanged in the S3 Raw zone.
2. Identify the failed processing date and affected input files.
3. Check the ETL or AWS Glue logs to identify the failure.
4. Fix the transformation issue before replaying.
5. Re-run the ETL job using the retained raw data.
6. Verify the generated Parquet files and record counts.
7. Confirm that duplicate event IDs have not been introduced.

## Late Events
Events should be partitioned using their event timestamp, not their arrival time. A late event must be written to its correct year/month/day partition.

## Duplicate Events
Use event_id as the event identifier. The production pipeline should implement an idempotent write strategy before replaying data.

## Current Test Evidence
The local replay test simulates a failed transformation, confirms that raw data remains available, and verifies that a subsequent run succeeds.

## AWS Deployment Status
The local replay test passed. AWS Glue replay has not yet been executed because Glue job creation is blocked by account permissions.
