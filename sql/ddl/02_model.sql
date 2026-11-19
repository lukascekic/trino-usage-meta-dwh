-- META DWH MODEL.
-- Keys are logical contracts enforced by the load (Iceberg does not enforce uniqueness).

CREATE SCHEMA IF NOT EXISTS {{dwh_schema}};

CREATE TABLE IF NOT EXISTS fact_query (
    cluster_id              varchar NOT NULL COMMENT 'Key part. Trino cluster the query ran on.',
    query_id                varchar NOT NULL COMMENT 'Key part. Trino query id.',
    user_name               varchar NOT NULL COMMENT 'context.user - the identity the query ran as. "The user" in every usage statistic. Includes service accounts.',
    principal               varchar COMMENT 'context.principal - the authenticated principal; differs from user_name under impersonation.',
    original_user           varchar COMMENT 'context.originalUser.',
    source                  varchar COMMENT 'context.source - client-declared tool name. Self-reported.',
    client_tags             array(varchar) COMMENT 'context.clientTags. Self-reported.',
    resource_group          varchar COMMENT 'context.resourceGroupId joined with dots.',
    query_type              varchar COMMENT 'SELECT, INSERT, MERGE, DATA_DEFINITION, ... Absent for queries that failed to parse.',
    update_type             varchar COMMENT 'metadata.updateType, e.g. INSERT, MERGE, CREATE TABLE.',
    query_state             varchar NOT NULL COMMENT 'FINISHED or FAILED.',
    error_name              varchar COMMENT 'failureInfo.errorCode.name for failed queries.',
    error_type              varchar COMMENT 'failureInfo.errorCode.type for failed queries, e.g. USER_ERROR.',
    create_time             timestamp(6) with time zone NOT NULL COMMENT 'When the query was submitted. Decides which team the query belongs to.',
    execution_start_time    timestamp(6) with time zone COMMENT 'When execution started.',
    end_time                timestamp(6) with time zone NOT NULL COMMENT 'When the query ended. Decides which reporting period the query belongs to.',
    query_date              date NOT NULL COMMENT 'UTC calendar date of end_time. Copied to bridge_query_table, which is partitioned by it.',
    cpu_time_seconds        double NOT NULL COMMENT 'statistics.cpuTime. ADDITIVE across queries. Headline resource metric.',
    failed_cpu_time_seconds double COMMENT 'statistics.failedCpuTime. Do NOT add to cpu_time_seconds.',
    wall_time_seconds       double COMMENT 'statistics.wallTime. Elapsed time, not resource consumption.',
    queued_time_seconds     double COMMENT 'statistics.queuedTime.',
    physical_input_bytes    bigint COMMENT 'statistics.physicalInputBytes. ADDITIVE across queries.',
    physical_input_rows     bigint COMMENT 'statistics.physicalInputRows. ADDITIVE across queries.',
    peak_user_memory_bytes  bigint COMMENT 'statistics.peakUserMemoryBytes. NOT additive: aggregate with MAX only.',
    written_bytes           bigint COMMENT 'statistics.writtenBytes. ADDITIVE across queries.',
    server_version          varchar NOT NULL COMMENT 'context.serverVersion - the Trino version that produced the event.',
    content_hash            varchar NOT NULL COMMENT 'Hash of everything the model took from the event (all columns above plus the set of tables touched). Tells a harmless redelivery from a conflicting version of the same query.',
    loaded_batch_id         varchar NOT NULL COMMENT 'Audit: delivery batch that first loaded this row. Depends on load order.',
    loaded_at               timestamp(6) with time zone NOT NULL COMMENT 'Audit: when the row was loaded.'
)
COMMENT 'One row per completed Trino query (finished or failed). Grain: query. Key: (cluster_id, query_id). The only table with query-level measures (CPU, memory, totals). Sum them from this table or v_query_team alone: after a join to bridge_query_table every query appears once per table, and the sums would be multiplied.'
-- Partitioned by the day of end_time, the column every period filter uses, so a period query reads only
-- the partitions of that period.
WITH (partitioning = ARRAY['day(end_time)']);

CREATE TABLE IF NOT EXISTS bridge_query_table (
    cluster_id             varchar NOT NULL COMMENT 'Key part. With query_id references fact_query.',
    query_id               varchar NOT NULL COMMENT 'Key part.',
    catalog_name           varchar NOT NULL COMMENT 'Key part. Table identity is catalog + schema + table, exactly as Trino reported it.',
    schema_name            varchar NOT NULL COMMENT 'Key part.',
    table_name             varchar NOT NULL COMMENT 'Key part.',
    access_type            varchar NOT NULL COMMENT 'Key part. READ = table is among the query inputs (ioMetadata.inputs). WRITE = table is the query output (ioMetadata.output). A table can have both in one query.',
    is_directly_referenced boolean COMMENT 'true = named in the query text; false = reached only through a view; NULL = not listed in metadata.tables.',
    physical_input_bytes   bigint COMMENT 'READ only: bytes Trino reported reading from this table in this query, summed over every scan of it (a self-join scans twice). NULL if a scan did not report it, and for WRITE. ADDITIVE across queries and tables.',
    physical_input_rows    bigint COMMENT 'READ only: rows Trino reported reading from this table, summed over every scan; not the number of distinct rows. NULL if a scan did not report it, and for WRITE. ADDITIVE.',
    query_date             date NOT NULL COMMENT 'Copy of fact_query.query_date (UTC date of the query end). Partition column: filter on it to skip days outside a period.'
)
COMMENT 'Which tables each query touched. Grain: query x table x access type. Holds only what Trino measures per table (bytes and rows read). CPU and memory exist only per query and live in fact_query, so summing them through this table is always wrong.'
WITH (partitioning = ARRAY['query_date']);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id   varchar NOT NULL COMMENT 'Key. Stable team identifier. UNASSIGNED is reserved for queries whose user has no team.',
    team_name varchar NOT NULL COMMENT 'Display name.'
)
COMMENT 'Teams. Grain: team. Additional metadata, maintained outside Trino (org directory export).';

CREATE TABLE IF NOT EXISTS user_team_history (
    user_name  varchar NOT NULL COMMENT 'Key part. Matches fact_query.user_name.',
    team_id    varchar NOT NULL COMMENT 'References dim_team.',
    valid_from timestamp(6) with time zone NOT NULL COMMENT 'Key part. Start of membership, inclusive, UTC.',
    valid_to   timestamp(6) with time zone COMMENT 'End of membership, exclusive, UTC. NULL = still a member.'
)
COMMENT 'Which team each user belonged to, and when. Grain: user x membership interval. Intervals of one user never overlap, so a query maps to at most one team. Additional metadata (org directory export).';

CREATE TABLE IF NOT EXISTS dim_table (
    cluster_id      varchar NOT NULL COMMENT 'Key part.',
    catalog_name    varchar NOT NULL COMMENT 'Key part.',
    schema_name     varchar NOT NULL COMMENT 'Key part.',
    table_name      varchar NOT NULL COMMENT 'Key part. Identity is the reported name: a renamed table is a new table.',
    first_seen_date date NOT NULL COMMENT 'Earliest query_date on which a query touched the table.',
    last_seen_date  date NOT NULL COMMENT 'Latest query_date on which a query touched the table.'
)
COMMENT 'Every table that appeared in at least one query. Grain: table. Derived from bridge_query_table, so a table nobody ever queried is not here.';

CREATE TABLE IF NOT EXISTS etl_batch (
    batch_id               varchar NOT NULL COMMENT 'Key. Delivery batch from raw.query_events.',
    status                 varchar NOT NULL COMMENT 'STARTED, SUCCEEDED or FAILED. Only SUCCEEDED batches are complete in the model.',
    started_at             timestamp(6) with time zone NOT NULL COMMENT 'Start of the latest attempt.',
    finished_at            timestamp(6) with time zone COMMENT 'End of the latest attempt; NULL while STARTED (also after a crash).',
    messages               bigint COMMENT 'Delivered messages in the batch.',
    queries_new            bigint COMMENT 'Queries of this batch that no other batch had loaded before (an interrupted earlier attempt of the same batch does not count as another batch).',
    queries_already_loaded bigint COMMENT 'Queries of this batch that another batch had already loaded (redeliveries).',
    error                  varchar COMMENT 'Why the batch was rejected, when FAILED.'
)
COMMENT 'Load log. Grain: delivery batch. A batch is loaded again from the start until it is SUCCEEDED.';
