-- READING LAYER, step 3: the tables each delivered event touched. Parameters: batch_id, and its first and last received_at.
--   READ  = every distinct table in ioMetadata.inputs. Trino lists a table once per scan, so a self-join
--           lists it twice - as two identical entries. The scans are collapsed to one row by GROUP BY, and the
--           bytes and rows of ALL scans are summed first: both scans read the data. A measure is NULL for the
--           table if any of its scans did not report it.
--   WRITE = the table in ioMetadata.output, if any.
-- A table may get both rows in one query (INSERT INTO t SELECT ... FROM t; MERGE reads its target).
-- Nothing is inferred from the SQL text, and no table is excluded: the bridge reports what Trino reported.
CREATE OR REPLACE TABLE stg_query_table AS
WITH src AS (
    SELECT delivery_id, cluster_id, json_parse(event_json) AS e
    FROM {{raw_schema}}.query_events
    WHERE batch_id = ?
      AND received_at BETWEEN ? AND ?  -- the batch's own raw partitions only
),
reads AS (
    SELECT
        s.delivery_id,
        json_extract_scalar(i, '$.catalogName') AS catalog_name,
        json_extract_scalar(i, '$.schema') AS schema_name,
        json_extract_scalar(i, '$.table') AS table_name,
        'READ' AS access_type,
        IF(bool_or(json_extract_scalar(i, '$.physicalInputBytes') IS NULL), NULL,
           sum(CAST(json_extract_scalar(i, '$.physicalInputBytes') AS bigint))) AS physical_input_bytes,
        IF(bool_or(json_extract_scalar(i, '$.physicalInputRows') IS NULL), NULL,
           sum(CAST(json_extract_scalar(i, '$.physicalInputRows') AS bigint))) AS physical_input_rows
    FROM src AS s
    CROSS JOIN UNNEST(CAST(json_extract(s.e, '$.ioMetadata.inputs') AS array(json))) AS u(i)
    GROUP BY 1, 2, 3, 4
),
writes AS (
    SELECT
        delivery_id,
        json_extract_scalar(e, '$.ioMetadata.output.catalogName') AS catalog_name,
        json_extract_scalar(e, '$.ioMetadata.output.schema') AS schema_name,
        json_extract_scalar(e, '$.ioMetadata.output.table') AS table_name,
        'WRITE' AS access_type,
        CAST(NULL AS bigint) AS physical_input_bytes,
        CAST(NULL AS bigint) AS physical_input_rows
    FROM src
    WHERE json_extract_scalar(e, '$.ioMetadata.output.table') IS NOT NULL
),
-- metadata.tables says whether a table was named in the query text or reached through a view.
referenced AS (
    SELECT
        s.delivery_id,
        json_extract_scalar(t, '$.catalog') AS catalog_name,
        json_extract_scalar(t, '$.schema') AS schema_name,
        json_extract_scalar(t, '$.table') AS table_name,
        bool_or(CAST(json_extract_scalar(t, '$.directlyReferenced') AS boolean)) AS is_directly_referenced
    FROM src AS s
    CROSS JOIN UNNEST(CAST(json_extract(s.e, '$.metadata.tables') AS array(json))) AS u(t)
    GROUP BY 1, 2, 3, 4
)
SELECT
    a.delivery_id,
    q.cluster_id,
    q.query_id,
    a.catalog_name,
    a.schema_name,
    a.table_name,
    a.access_type,
    r.is_directly_referenced,
    a.physical_input_bytes,
    a.physical_input_rows,
    q.query_date
FROM (SELECT * FROM reads UNION ALL SELECT * FROM writes) AS a
JOIN stg_query AS q
    ON q.delivery_id = a.delivery_id
LEFT JOIN referenced AS r
    ON  r.delivery_id = a.delivery_id
    AND r.catalog_name = a.catalog_name AND r.schema_name = a.schema_name AND r.table_name = a.table_name;
