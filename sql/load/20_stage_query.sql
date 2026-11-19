-- READING LAYER, step 2: typed projection of one delivery batch, one row per delivered event.
-- Parameters: batch_id, and its first and last received_at. Together with 10_validate_batch.sql and 21_stage_query_table.sql, this is the only place that knows the JSON shape.
--
-- Wire format seen on Trino 484: durations are JSON numbers in SECONDS; sizes are byte counts; timestamps
-- are ISO-8601 text with NANOSECOND precision; an optional key without a value is simply absent (-> NULL).
--
-- Timestamps: Iceberg (format v2) stores microseconds. The value is TRUNCATED to microseconds on the text,
-- before parsing - never rounded. Rounding would move 23:59:59.999999900 into the next day and could move
-- a query across a reporting-period or team-membership boundary.
CREATE OR REPLACE TABLE stg_query AS
WITH src AS (
    SELECT delivery_id, batch_id, cluster_id, json_parse(event_json) AS e
    FROM {{raw_schema}}.query_events
    WHERE batch_id = ?
      AND received_at BETWEEN ? AND ?  -- the batch's own raw partitions only
),
typed AS (
    SELECT
        delivery_id,
        batch_id,
        cluster_id,
        json_extract_scalar(e, '$.metadata.queryId') AS query_id,
        json_extract_scalar(e, '$.context.user') AS user_name,
        json_extract_scalar(e, '$.context.principal') AS principal,
        json_extract_scalar(e, '$.context.originalUser') AS original_user,
        json_extract_scalar(e, '$.context.source') AS source,
        CAST(json_extract(e, '$.context.clientTags') AS array(varchar)) AS client_tags,
        array_join(CAST(json_extract(e, '$.context.resourceGroupId') AS array(varchar)), '.') AS resource_group,
        json_extract_scalar(e, '$.context.queryType') AS query_type,
        json_extract_scalar(e, '$.metadata.updateType') AS update_type,
        json_extract_scalar(e, '$.metadata.queryState') AS query_state,
        json_extract_scalar(e, '$.failureInfo.errorCode.name') AS error_name,
        json_extract_scalar(e, '$.failureInfo.errorCode.type') AS error_type,
        CAST(from_iso8601_timestamp_nanos(regexp_replace(json_extract_scalar(e, '$.createTime'), '(\.\d{6})\d+', '$1'))
             AS timestamp(6) with time zone) AT TIME ZONE 'UTC' AS create_time,
        CAST(from_iso8601_timestamp_nanos(regexp_replace(json_extract_scalar(e, '$.executionStartTime'), '(\.\d{6})\d+', '$1'))
             AS timestamp(6) with time zone) AT TIME ZONE 'UTC' AS execution_start_time,
        CAST(from_iso8601_timestamp_nanos(regexp_replace(json_extract_scalar(e, '$.endTime'), '(\.\d{6})\d+', '$1'))
             AS timestamp(6) with time zone) AT TIME ZONE 'UTC' AS end_time,
        CAST(json_extract_scalar(e, '$.statistics.cpuTime') AS double) AS cpu_time_seconds,
        CAST(json_extract_scalar(e, '$.statistics.failedCpuTime') AS double) AS failed_cpu_time_seconds,
        CAST(json_extract_scalar(e, '$.statistics.wallTime') AS double) AS wall_time_seconds,
        CAST(json_extract_scalar(e, '$.statistics.queuedTime') AS double) AS queued_time_seconds,
        CAST(json_extract_scalar(e, '$.statistics.physicalInputBytes') AS bigint) AS physical_input_bytes,
        CAST(json_extract_scalar(e, '$.statistics.physicalInputRows') AS bigint) AS physical_input_rows,
        CAST(json_extract_scalar(e, '$.statistics.peakUserMemoryBytes') AS bigint) AS peak_user_memory_bytes,
        CAST(json_extract_scalar(e, '$.statistics.writtenBytes') AS bigint) AS written_bytes,
        json_extract_scalar(e, '$.context.serverVersion') AS server_version
    FROM src
)
SELECT
    t.*,
    CAST(end_time AS date) AS query_date
FROM typed AS t;
