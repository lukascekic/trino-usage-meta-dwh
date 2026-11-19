-- Trend for Q1: distinct users of table X per UTC day.
-- Parameters: cluster_id, catalog_name, schema_name, table_name, period_start, period_end, period_start, period_end.
--
-- Each row is correct for its own day. The rows must NOT be added up to get a figure for the whole period:
-- a user active on three days would be counted three times. Use q1_distinct_users.sql for a period.
SELECT
    f.query_date,
    count(DISTINCT f.user_name) AS distinct_users,
    count(*)                    AS queries
FROM fact_query AS f
JOIN bridge_query_table AS b
    ON b.cluster_id = f.cluster_id AND b.query_id = f.query_id
WHERE b.cluster_id = ?
  AND b.catalog_name = ?
  AND b.schema_name = ?
  AND b.table_name = ?
  AND b.access_type = 'READ'
  AND b.query_date BETWEEN CAST(? AT TIME ZONE 'UTC' AS date) AND CAST(? AT TIME ZONE 'UTC' AS date)  -- partition pruning only
  AND f.query_state = 'FINISHED'
  AND f.end_time >= ?
  AND f.end_time < ?
GROUP BY f.query_date
ORDER BY f.query_date;
