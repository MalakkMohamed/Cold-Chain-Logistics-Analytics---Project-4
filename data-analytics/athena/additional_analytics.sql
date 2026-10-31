-- 1. Daily temperature comparison by truck
SELECT
    year,
    month,
    day,
    truck_id,
    COUNT(*) AS total_readings,
    ROUND(AVG(temperature), 2) AS average_temperature,
    MAX(temperature) AS maximum_temperature
FROM cold_chain_events
GROUP BY year, month, day, truck_id
ORDER BY year, month, day, truck_id;


-- 2. Daily temperature breach counts by truck
SELECT
    year,
    month,
    day,
    truck_id,
    COUNT(*) AS breach_count
FROM cold_chain_events
WHERE temperature > 5.0
GROUP BY year, month, day, truck_id
ORDER BY year, month, day, truck_id;


-- 3. Missing or inactive sensors
-- Requires a registered_sensors table containing sensor_id.
SELECT
    s.sensor_id,
    MAX(e.event_time) AS last_seen
FROM registered_sensors s
LEFT JOIN cold_chain_events e
    ON s.sensor_id = e.sensor_id
GROUP BY s.sensor_id
HAVING MAX(e.event_time) IS NULL
    OR MAX(e.event_time) < date_add('minute', -5, current_timestamp);
