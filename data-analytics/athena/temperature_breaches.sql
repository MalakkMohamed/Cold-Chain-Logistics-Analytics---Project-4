SELECT
    truck_id,
    COUNT(*) AS total_readings,
    SUM(
        CASE
            WHEN temperature > 5.0 THEN 1
            ELSE 0
        END
    ) AS breach_count
FROM cold_chain_events
GROUP BY truck_id
ORDER BY truck_id;