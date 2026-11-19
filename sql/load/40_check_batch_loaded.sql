-- Completeness gate: runs after the writes, before the batch is marked SUCCEEDED.
-- Every query and every query-table relationship of the staged batch must now be in the model.
SELECT 'query not loaded: ' || c.cluster_id || '/' || c.query_id AS problem
FROM stg_query_content AS c
LEFT JOIN fact_query AS f ON f.cluster_id = c.cluster_id AND f.query_id = c.query_id
WHERE c.delivery_rank = 1 AND f.query_id IS NULL
UNION ALL
SELECT 'relationship not loaded: ' || s.query_id || ' ' || s.access_type || ' ' || s.catalog_name || '.' || s.schema_name || '.' || s.table_name
FROM stg_query_table AS s
JOIN stg_query_content AS c ON c.delivery_id = s.delivery_id AND c.delivery_rank = 1
LEFT JOIN bridge_query_table AS b
    ON  b.cluster_id = s.cluster_id AND b.query_id = s.query_id AND b.catalog_name = s.catalog_name
    AND b.schema_name = s.schema_name AND b.table_name = s.table_name AND b.access_type = s.access_type
WHERE b.query_id IS NULL;
