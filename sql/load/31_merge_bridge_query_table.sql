-- MODEL, step 2: insert query-table relationships that are not loaded yet. Running it again after a crash
-- adds whatever is still missing and nothing else.
MERGE INTO bridge_query_table AS t
USING (
    SELECT t.*
    FROM stg_query_table AS t
    JOIN stg_query_content AS c ON c.delivery_id = t.delivery_id AND c.delivery_rank = 1
) AS s
    ON  t.cluster_id = s.cluster_id AND t.query_id = s.query_id
    AND t.catalog_name = s.catalog_name AND t.schema_name = s.schema_name AND t.table_name = s.table_name
    AND t.access_type = s.access_type
WHEN NOT MATCHED THEN INSERT (
    cluster_id, query_id, catalog_name, schema_name, table_name, access_type, is_directly_referenced,
    physical_input_bytes, physical_input_rows, query_date
) VALUES (
    s.cluster_id, s.query_id, s.catalog_name, s.schema_name, s.table_name, s.access_type, s.is_directly_referenced,
    s.physical_input_bytes, s.physical_input_rows, s.query_date
);
