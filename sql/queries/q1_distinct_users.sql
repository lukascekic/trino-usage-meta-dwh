-- Q1. How many distinct users queried table X during a given period?
--
-- Parameters: cluster_id, catalog_name, schema_name, table_name,
--             period_start, period_end (the same two values again, for partition pruning),
--             period_start (inclusive), period_end (exclusive) - timestamps with time zone.
--
-- "Queried" = a FINISHED query that READ the table (directly or through a view).
-- A query is counted in the period in which it ENDED.
-- Distinct users are counted over the whole period; they must never be obtained by adding up per-day counts.
SELECT count(DISTINCT f.user_name) AS distinct_users
FROM fact_query AS f
JOIN bridge_query_table AS b
    ON b.cluster_id = f.cluster_id AND b.query_id = f.query_id
WHERE b.cluster_id = ?
  AND b.catalog_name = ?
  AND b.schema_name = ?
  AND b.table_name = ?
  AND b.access_type = 'READ'
  -- Redundant with the end_time filter below, but it lets Trino skip bridge partitions outside the period.
  AND b.query_date BETWEEN CAST(? AT TIME ZONE 'UTC' AS date) AND CAST(? AT TIME ZONE 'UTC' AS date)
  AND f.query_state = 'FINISHED'
  AND f.end_time >= ?
  AND f.end_time < ?;
