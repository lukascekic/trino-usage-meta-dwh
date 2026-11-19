-- Q2. How many resources did team Y consume during a given period?
--
-- Parameters: team_id, period_start (inclusive), period_end (exclusive) - timestamps with time zone.
--
-- Every completed query of the team counts - failed ones and ones that read no table included - because
-- the cluster spent those resources either way. The bridge table is not involved, so a query that touched
-- several tables is counted exactly once.
-- A team with no query in the period gets query_count = 0 and NULL sums (SQL's sum over no rows).
SELECT
    count(*)                               AS query_count,
    count_if(query_state = 'FAILED')       AS failed_query_count,
    sum(cpu_time_seconds)                  AS cpu_seconds,
    sum(physical_input_bytes)              AS physical_input_bytes,
    count_if(physical_input_bytes IS NULL) AS queries_without_input_bytes,
    max(peak_user_memory_bytes)            AS max_query_peak_memory_bytes
FROM v_query_team
WHERE team_id = ?
  AND end_time >= ?
  AND end_time < ?;
