-- Invariants of the model. Any row returned is a violation. Run after every load and on demand
-- (`python -m pipeline check`).
SELECT 'fact_query: duplicate key ' || cluster_id || '/' || query_id AS problem
FROM fact_query
GROUP BY cluster_id, query_id
HAVING count(*) > 1
UNION ALL
SELECT 'bridge_query_table: duplicate key ' || query_id || ' ' || access_type || ' ' || catalog_name || '.' || schema_name || '.' || table_name
FROM bridge_query_table
GROUP BY cluster_id, query_id, catalog_name, schema_name, table_name, access_type
HAVING count(*) > 1
UNION ALL
SELECT 'bridge_query_table: row without its query ' || b.cluster_id || '/' || b.query_id
FROM bridge_query_table AS b
LEFT JOIN fact_query AS f ON f.cluster_id = b.cluster_id AND f.query_id = b.query_id
WHERE f.query_id IS NULL
UNION ALL
SELECT 'bridge_query_table: table missing from dim_table ' || b.catalog_name || '.' || b.schema_name || '.' || b.table_name
FROM bridge_query_table AS b
LEFT JOIN dim_table AS d
    ON  d.cluster_id = b.cluster_id AND d.catalog_name = b.catalog_name
    AND d.schema_name = b.schema_name AND d.table_name = b.table_name
WHERE d.table_name IS NULL
UNION ALL
-- Conservation: team attribution must neither add nor drop a query. This is checked on KEYS, not by
-- comparing two sums of floating-point measures: the same doubles added in a different order can differ in
-- the last bits, and that would fail a perfectly good load. If every query appears in v_query_team exactly
-- once, every measure is carried over row by row and all totals are preserved by construction.
SELECT 'v_query_team: query attributed to more than one team ' || cluster_id || '/' || query_id
FROM v_query_team
GROUP BY cluster_id, query_id
HAVING count(*) > 1
UNION ALL
SELECT 'v_query_team: ' || CAST(v.n AS varchar) || ' rows, but fact_query has ' || CAST(f.n AS varchar)
FROM (SELECT count(*) AS n FROM v_query_team) AS v
CROSS JOIN (SELECT count(*) AS n FROM fact_query) AS f
WHERE v.n <> f.n;
