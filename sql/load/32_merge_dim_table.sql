-- MODEL, step 3: keep the list of known tables. MIN / MAX make the result independent of the order in
-- which batches arrive and safe to repeat.
MERGE INTO dim_table AS t
USING (
    SELECT s.cluster_id, s.catalog_name, s.schema_name, s.table_name,
           min(s.query_date) AS first_seen_date, max(s.query_date) AS last_seen_date
    FROM stg_query_table AS s
    JOIN stg_query_content AS c ON c.delivery_id = s.delivery_id AND c.delivery_rank = 1
    GROUP BY 1, 2, 3, 4
) AS s
    ON  t.cluster_id = s.cluster_id AND t.catalog_name = s.catalog_name
    AND t.schema_name = s.schema_name AND t.table_name = s.table_name
WHEN MATCHED THEN UPDATE SET
    first_seen_date = least(t.first_seen_date, s.first_seen_date),
    last_seen_date = greatest(t.last_seen_date, s.last_seen_date)
WHEN NOT MATCHED THEN INSERT (cluster_id, catalog_name, schema_name, table_name, first_seen_date, last_seen_date)
VALUES (s.cluster_id, s.catalog_name, s.schema_name, s.table_name, s.first_seen_date, s.last_seen_date);
