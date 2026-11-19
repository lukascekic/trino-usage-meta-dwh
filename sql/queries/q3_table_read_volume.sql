-- Q3. How much data was read from each table during a given period?
--
-- Parameters: cluster_id, period_start, period_end (again, for partition pruning),
--             period_start (inclusive), period_end (exclusive) - timestamps with time zone.
--
-- Bytes and rows are what Trino reported reading from each table, per query, summed over the query's scans.
-- Like team resources (Q2), this counts every completed query, failed ones included: the scan happened.
-- A query that did not report a measure for a table is counted in queries_without_input_bytes / _rows, and
-- the sums cover only the queries that did report it. CPU per table does not exist: Trino measures CPU per
-- query.
SELECT
    b.catalog_name,
    b.schema_name,
    b.table_name,
    count(*)                                 AS queries,
    sum(b.physical_input_bytes)              AS physical_input_bytes,
    sum(b.physical_input_rows)               AS physical_input_rows,
    count_if(b.physical_input_bytes IS NULL) AS queries_without_input_bytes,
    count_if(b.physical_input_rows IS NULL)  AS queries_without_input_rows
FROM bridge_query_table AS b
JOIN fact_query AS f
    ON f.cluster_id = b.cluster_id AND f.query_id = b.query_id
WHERE b.cluster_id = ?
  AND b.access_type = 'READ'
  AND b.query_date BETWEEN CAST(? AT TIME ZONE 'UTC' AS date) AND CAST(? AT TIME ZONE 'UTC' AS date)
  AND f.end_time >= ?
  AND f.end_time < ?
GROUP BY b.catalog_name, b.schema_name, b.table_name
ORDER BY physical_input_bytes DESC NULLS LAST, b.catalog_name, b.schema_name, b.table_name;
