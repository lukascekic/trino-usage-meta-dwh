-- READING LAYER, step 1: validate one delivery batch. Parameters: batch_id, and its first and last received_at.
-- Returns one row per problem. Any row returned rejects the WHOLE batch: nothing is loaded from it.
-- A delivered event is never silently dropped or silently turned into "a query without tables".
--
-- Shape contract (observed on real Trino 484 events, see fixtures/captured/). For every field the model reads:
--   * a REQUIRED field must be present with the expected JSON type;
--   * an OPTIONAL field may be ABSENT (it becomes NULL), but if it is PRESENT it must have the expected
--     JSON type. "Absent" and "present but damaged" are different things and are never both turned into NULL;
--   * the JSON type itself is checked: the number 17 is not the text "17";
--   * ioMetadata.inputs must be an array. An EMPTY array is valid (a query that read no table), a missing
--     array is not.
-- Fields the model does not read (query plan, operator statistics, ...) are not inspected.
WITH
    FUNCTION is_absent(j json) RETURNS boolean
        RETURN COALESCE(json_format(j) = 'null', true),
    FUNCTION is_text(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(j), '"'), false),
    FUNCTION is_nonempty_text(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(j), '"') AND json_format(j) <> '""', false),
    FUNCTION is_number(j json) RETURNS boolean
        RETURN COALESCE(regexp_like(json_format(j), '^-?[0-9]'), false),
    FUNCTION is_whole_number(j json) RETURNS boolean
        RETURN COALESCE(regexp_like(json_format(j), '^-?[0-9]+$') AND TRY_CAST(json_format(j) AS bigint) IS NOT NULL, false),
    FUNCTION is_object(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(j), '{'), false),
    FUNCTION is_boolean(j json) RETURNS boolean
        RETURN COALESCE(json_format(j) IN ('true', 'false'), false),
    FUNCTION is_timestamp(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(j), '"')
                        AND TRY(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$'))) IS NOT NULL, false),
    -- the text ends in Z or a numeric offset after the time of day
    FUNCTION has_time_zone(j json) RETURNS boolean
        RETURN COALESCE(regexp_like(json_extract_scalar(j, '$'), 'T[0-9:.,]+(Z|[+-][0-9]{2}(:?[0-9]{2})?)$'), false),
    FUNCTION is_text_array(j json) RETURNS boolean
        RETURN COALESCE(all_match(TRY_CAST(j AS array(json)), x -> COALESCE(starts_with(json_format(x), '"'), false)), false),
    -- catalog / schema / table of one ioMetadata entry
    FUNCTION has_identity(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(json_extract(j, '$.catalogName')), '"') AND json_format(json_extract(j, '$.catalogName')) <> '""'
                    AND starts_with(json_format(json_extract(j, '$.schema')), '"') AND json_format(json_extract(j, '$.schema')) <> '""'
                    AND starts_with(json_format(json_extract(j, '$.table')), '"') AND json_format(json_extract(j, '$.table')) <> '""', false),
    -- one metadata.tables entry: catalog / schema / table and a boolean directlyReferenced
    FUNCTION is_table_reference(j json) RETURNS boolean
        RETURN COALESCE(starts_with(json_format(json_extract(j, '$.catalog')), '"') AND json_format(json_extract(j, '$.catalog')) <> '""'
                    AND starts_with(json_format(json_extract(j, '$.schema')), '"') AND json_format(json_extract(j, '$.schema')) <> '""'
                    AND starts_with(json_format(json_extract(j, '$.table')), '"') AND json_format(json_extract(j, '$.table')) <> '""'
                    AND json_format(json_extract(j, '$.directlyReferenced')) IN ('true', 'false'), false)
WITH src AS (
    SELECT delivery_id, TRY(json_parse(event_json)) AS e
    FROM {{raw_schema}}.query_events
    WHERE batch_id = ?
      AND received_at BETWEEN ? AND ?  -- the batch's own raw partitions only
),
parts AS (
    SELECT
        delivery_id,
        e,
        json_extract_scalar(e, '$.context.serverVersion') AS server_version,
        json_extract(e, '$.ioMetadata.inputs') AS inputs,
        json_extract(e, '$.ioMetadata.output') AS output,
        json_extract(e, '$.metadata.tables') AS tables
    FROM src
)
SELECT p.delivery_id, x.problem
FROM parts AS p
CROSS JOIN UNNEST(ARRAY[
    IF(e IS NULL, 'event_json is not valid JSON'),

    -- required
    IF(e IS NOT NULL AND NOT is_nonempty_text(json_extract(e, '$.metadata.queryId')), 'metadata.queryId is missing or not text'),
    IF(e IS NOT NULL AND NOT is_nonempty_text(json_extract(e, '$.metadata.queryState')), 'metadata.queryState is missing or not text'),
    IF(e IS NOT NULL AND NOT is_nonempty_text(json_extract(e, '$.context.user')), 'context.user is missing or not text'),
    IF(e IS NOT NULL AND NOT is_nonempty_text(json_extract(e, '$.context.serverVersion')), 'context.serverVersion is missing or not text'),
    -- Versions whose event shape has been verified. Add a version here only after checking its events.
    IF(server_version <> '' AND NOT contains(ARRAY['484'], server_version), 'unsupported serverVersion ' || server_version),
    IF(e IS NOT NULL AND NOT is_timestamp(json_extract(e, '$.createTime')), 'createTime is missing or unreadable'),
    IF(e IS NOT NULL AND NOT is_timestamp(json_extract(e, '$.endTime')), 'endTime is missing or unreadable'),
    IF(e IS NOT NULL AND NOT is_number(json_extract(e, '$.statistics.cpuTime')), 'statistics.cpuTime is missing or not a number'),
    -- A completed query has one of these two states; anything else is not a QueryCompletedEvent.
    IF(is_nonempty_text(json_extract(e, '$.metadata.queryState'))
       AND NOT contains(ARRAY['FINISHED', 'FAILED'], json_extract_scalar(e, '$.metadata.queryState')),
       'metadata.queryState is ' || json_extract_scalar(e, '$.metadata.queryState') || ', expected FINISHED or FAILED'),
    -- Without a zone, Trino reads the time in the session's zone, so the period of a query would depend on
    -- who runs the load.
    IF(any_match(
           ARRAY[json_extract(e, '$.createTime'), json_extract(e, '$.executionStartTime'), json_extract(e, '$.endTime')],
           j -> is_timestamp(j) AND NOT has_time_zone(j)),
       'a timestamp has no time zone (createTime, executionStartTime or endTime)'),

    -- optional: absent is fine, present must have the right type
    IF(NOT is_absent(json_extract(e, '$.executionStartTime')) AND NOT is_timestamp(json_extract(e, '$.executionStartTime')),
       'executionStartTime is present but unreadable'),
    -- An optional PARENT of the wrong type would make its children look absent, so parents are checked first.
    IF(NOT is_absent(json_extract(e, '$.failureInfo')) AND NOT is_object(json_extract(e, '$.failureInfo')),
       'failureInfo is present but not an object'),
    IF(is_object(json_extract(e, '$.failureInfo')) AND NOT is_absent(json_extract(e, '$.failureInfo.errorCode'))
       AND NOT is_object(json_extract(e, '$.failureInfo.errorCode')),
       'failureInfo.errorCode is present but not an object'),
    IF(any_match(
           ARRAY[json_extract(e, '$.context.principal'), json_extract(e, '$.context.originalUser'),
                 json_extract(e, '$.context.source'), json_extract(e, '$.context.queryType'),
                 json_extract(e, '$.metadata.updateType'),
                 json_extract(e, '$.failureInfo.errorCode.name'), json_extract(e, '$.failureInfo.errorCode.type')],
           j -> NOT is_absent(j) AND NOT is_text(j)),
       'an optional text field is present but not text (principal, originalUser, source, queryType, updateType or failureInfo.errorCode)'),
    IF(any_match(
           ARRAY[json_extract(e, '$.context.clientTags'), json_extract(e, '$.context.resourceGroupId')],
           j -> NOT is_absent(j) AND NOT is_text_array(j)),
       'context.clientTags or context.resourceGroupId is present but not an array of text'),
    IF(any_match(
           ARRAY[json_extract(e, '$.statistics.failedCpuTime'), json_extract(e, '$.statistics.wallTime'),
                 json_extract(e, '$.statistics.queuedTime'), json_extract(e, '$.statistics.physicalInputBytes'),
                 json_extract(e, '$.statistics.physicalInputRows'), json_extract(e, '$.statistics.peakUserMemoryBytes'),
                 json_extract(e, '$.statistics.writtenBytes')],
           j -> NOT is_absent(j) AND NOT is_number(j)),
       'an optional statistic is present but not a number'),
    IF(any_match(
           ARRAY[json_extract(e, '$.statistics.physicalInputBytes'), json_extract(e, '$.statistics.physicalInputRows'),
                 json_extract(e, '$.statistics.peakUserMemoryBytes'), json_extract(e, '$.statistics.writtenBytes')],
           j -> is_number(j) AND NOT is_whole_number(j)),
       'a byte or row count is not a whole number within the 64-bit range'),

    -- plausibility: measures cannot be negative, and a query cannot end before it was submitted
    IF(any_match(
           ARRAY[json_extract(e, '$.statistics.cpuTime'), json_extract(e, '$.statistics.failedCpuTime'),
                 json_extract(e, '$.statistics.wallTime'), json_extract(e, '$.statistics.queuedTime'),
                 json_extract(e, '$.statistics.physicalInputBytes'), json_extract(e, '$.statistics.physicalInputRows'),
                 json_extract(e, '$.statistics.peakUserMemoryBytes'), json_extract(e, '$.statistics.writtenBytes')],
           j -> is_number(j) AND TRY_CAST(json_format(j) AS double) < 0),
       'a statistic is negative'),
    IF(is_timestamp(json_extract(e, '$.createTime')) AND is_timestamp(json_extract(e, '$.endTime'))
       AND from_iso8601_timestamp_nanos(json_extract_scalar(e, '$.endTime'))
           < from_iso8601_timestamp_nanos(json_extract_scalar(e, '$.createTime')),
       'endTime is before createTime'),

    -- tables the query touched
    IF(e IS NOT NULL AND TRY_CAST(inputs AS array(json)) IS NULL, 'ioMetadata.inputs is missing or not an array'),
    IF(any_match(TRY_CAST(inputs AS array(json)), i -> NOT has_identity(i)),
       'an ioMetadata.inputs entry lacks catalogName, schema or table'),
    IF(any_match(TRY_CAST(inputs AS array(json)), i ->
           (NOT is_absent(json_extract(i, '$.physicalInputBytes')) AND NOT is_whole_number(json_extract(i, '$.physicalInputBytes')))
        OR (NOT is_absent(json_extract(i, '$.physicalInputRows')) AND NOT is_whole_number(json_extract(i, '$.physicalInputRows')))),
       'an ioMetadata.inputs byte or row count is present but not a whole number within the 64-bit range'),
    IF(any_match(TRY_CAST(inputs AS array(json)), i ->
           TRY_CAST(json_format(json_extract(i, '$.physicalInputBytes')) AS double) < 0
        OR TRY_CAST(json_format(json_extract(i, '$.physicalInputRows')) AS double) < 0),
       'an ioMetadata.inputs byte or row count is negative'),
    -- Per-table sums are stored as bigint, so the sum over all scans of ONE table must fit. Only values that
    -- are already valid bigints are added (anything else is reported above), in decimal(38, 0): up to 10^19
    -- valid values can be added there without overflow, so the check itself cannot fail.
    IF(any_match(TRY_CAST(inputs AS array(json)), i ->
           reduce(filter(TRY_CAST(inputs AS array(json)), k ->
                      json_extract_scalar(k, '$.catalogName') = json_extract_scalar(i, '$.catalogName')
                  AND json_extract_scalar(k, '$.schema') = json_extract_scalar(i, '$.schema')
                  AND json_extract_scalar(k, '$.table') = json_extract_scalar(i, '$.table')),
                  CAST(0 AS decimal(38, 0)),
                  (acc, k) -> acc + COALESCE(CAST(TRY_CAST(json_format(json_extract(k, '$.physicalInputBytes')) AS bigint) AS decimal(38, 0)), 0),
                  acc -> acc) > DECIMAL '9223372036854775807'
        OR reduce(filter(TRY_CAST(inputs AS array(json)), k ->
                      json_extract_scalar(k, '$.catalogName') = json_extract_scalar(i, '$.catalogName')
                  AND json_extract_scalar(k, '$.schema') = json_extract_scalar(i, '$.schema')
                  AND json_extract_scalar(k, '$.table') = json_extract_scalar(i, '$.table')),
                  CAST(0 AS decimal(38, 0)),
                  (acc, k) -> acc + COALESCE(CAST(TRY_CAST(json_format(json_extract(k, '$.physicalInputRows')) AS bigint) AS decimal(38, 0)), 0),
                  acc -> acc) > DECIMAL '9223372036854775807'),
       'the byte or row counts of one table in ioMetadata.inputs add up to more than a 64-bit integer'),
    IF(NOT is_absent(output) AND NOT has_identity(output), 'ioMetadata.output lacks catalogName, schema or table'),
    IF(NOT is_absent(tables) AND TRY_CAST(tables AS array(json)) IS NULL, 'metadata.tables is present but not an array'),
    IF(any_match(TRY_CAST(tables AS array(json)), t -> NOT is_table_reference(t)),
       'a metadata.tables entry lacks catalog, schema, table or a boolean directlyReferenced')
]) AS x(problem)
WHERE x.problem IS NOT NULL
ORDER BY p.delivery_id, x.problem;
