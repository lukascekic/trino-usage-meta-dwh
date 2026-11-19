-- MODEL, step 1: insert completed queries that are not loaded yet. A query that is already loaded is left
-- untouched (23_find_conflicts.sql has already proven it is identical), so loading a batch again - after a
-- crash or by mistake - changes nothing.
MERGE INTO fact_query AS t
USING (SELECT * FROM stg_query_content WHERE delivery_rank = 1) AS s
    ON t.cluster_id = s.cluster_id AND t.query_id = s.query_id
WHEN NOT MATCHED THEN INSERT (
    cluster_id, query_id, user_name, principal, original_user, source, client_tags, resource_group,
    query_type, update_type, query_state, error_name, error_type,
    create_time, execution_start_time, end_time, query_date,
    cpu_time_seconds, failed_cpu_time_seconds, wall_time_seconds, queued_time_seconds,
    physical_input_bytes, physical_input_rows, peak_user_memory_bytes, written_bytes,
    server_version, content_hash, loaded_batch_id, loaded_at
) VALUES (
    s.cluster_id, s.query_id, s.user_name, s.principal, s.original_user, s.source, s.client_tags, s.resource_group,
    s.query_type, s.update_type, s.query_state, s.error_name, s.error_type,
    s.create_time, s.execution_start_time, s.end_time, s.query_date,
    s.cpu_time_seconds, s.failed_cpu_time_seconds, s.wall_time_seconds, s.queued_time_seconds,
    s.physical_input_bytes, s.physical_input_rows, s.peak_user_memory_bytes, s.written_bytes,
    s.server_version, s.content_hash, s.batch_id, current_timestamp
);
