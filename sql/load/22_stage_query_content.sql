-- READING LAYER, step 4: decide what "the same event" means.
--
-- content_hash covers everything the model takes from an event: every typed column of the query AND the
-- complete set of tables it touched, with the bytes and rows read from each. It does NOT cover transport data (delivery_id, batch_id, received_at),
-- the order of keys in the JSON text, or parts of the event the model never reads. Therefore:
--   * the same event delivered again, in any batch or re-serialized          -> same hash -> ignored;
--   * a "second version" of a query with a different measure OR table list   -> different hash -> conflict
--     (this includes the bytes or rows read from one table, even when the query total is unchanged).
CREATE OR REPLACE TABLE stg_query_content AS
WITH relations AS (
    SELECT
        delivery_id,
        array_sort(array_agg(json_format(CAST(
            ROW(catalog_name, schema_name, table_name, access_type, is_directly_referenced,
                physical_input_bytes, physical_input_rows) AS JSON)))) AS relation_set
    FROM stg_query_table
    GROUP BY delivery_id
),
hashed AS (
    SELECT
        q.*,
        to_hex(sha256(to_utf8(json_format(CAST(ROW(
            q.cluster_id, q.query_id, q.user_name, q.principal, q.original_user, q.source, q.client_tags,
            q.resource_group, q.query_type, q.update_type, q.query_state, q.error_name, q.error_type,
            CAST(q.create_time AS varchar), CAST(q.execution_start_time AS varchar), CAST(q.end_time AS varchar),
            q.cpu_time_seconds, q.failed_cpu_time_seconds, q.wall_time_seconds, q.queued_time_seconds,
            q.physical_input_bytes, q.physical_input_rows, q.peak_user_memory_bytes, q.written_bytes,
            q.server_version,
            COALESCE(r.relation_set, CAST(ARRAY[] AS array(varchar)))
        ) AS JSON))))) AS content_hash
    FROM stg_query AS q
    LEFT JOIN relations AS r ON r.delivery_id = q.delivery_id
)
SELECT
    h.*,
    -- Identical redeliveries inside one batch: the first one is loaded.
    row_number() OVER (PARTITION BY cluster_id, query_id ORDER BY delivery_id) AS delivery_rank
FROM hashed AS h;
