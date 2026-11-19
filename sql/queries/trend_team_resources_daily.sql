-- Trend for Q2: resources of team Y per UTC day.
-- Parameters: team_id, period_start, period_end.
--
-- query_count, cpu_seconds and physical_input_bytes are additive: summing the days gives the period total.
-- physical_input_bytes covers only the queries that reported it; queries_without_input_bytes says how many did not.
-- max_query_peak_memory_bytes is not: take the maximum over the days.
SELECT
    query_date,
    count(*)                         AS query_count,
    count_if(query_state = 'FAILED') AS failed_query_count,
    sum(cpu_time_seconds)            AS cpu_seconds,
    sum(physical_input_bytes)        AS physical_input_bytes,
    count_if(physical_input_bytes IS NULL) AS queries_without_input_bytes,
    max(peak_user_memory_bytes)      AS max_query_peak_memory_bytes
FROM v_query_team
WHERE team_id = ?
  AND end_time >= ?
  AND end_time < ?
GROUP BY query_date
ORDER BY query_date;
