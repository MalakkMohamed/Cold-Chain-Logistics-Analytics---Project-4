
SELECT
    truck_id,
    COUNT(*) AS total_readings,
    ROUND(AVG(temperature), 2) AS average_temperature,
    MIN(temperature) AS minimum_temperature,
    MAX(temperature) AS maximum_temperature
FROM cold_chain_events
GROUP BY truck_id
ORDER BY truck_id;
