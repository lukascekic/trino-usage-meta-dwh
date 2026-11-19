-- RAW LAYER (assumed input).
-- The task statement says raw Trino events already land in Iceberg tables. This repository assumes the
-- simplest shape that statement allows: one append-only table holding each delivered QueryCompletedEvent
-- as untouched JSON, plus the transport columns a collector adds. If the real layout differs, only the
-- reading layer (sql/load/10_*, 20_*, 21_*) has to change.

CREATE SCHEMA IF NOT EXISTS {{raw_schema}};

CREATE TABLE IF NOT EXISTS {{raw_schema}}.query_events (
    delivery_id varchar NOT NULL COMMENT 'Unique per delivered message. A redelivered event gets a new delivery_id.',
    batch_id    varchar NOT NULL COMMENT 'Closed, immutable delivery batch the message belongs to. Unit of loading.',
    cluster_id  varchar NOT NULL COMMENT 'Trino cluster the event came from; added by the collector.',
    received_at timestamp(6) with time zone NOT NULL COMMENT 'When the collector received the message (not when the query ran).',
    event_json  varchar NOT NULL COMMENT 'The complete QueryCompletedEvent exactly as Trino emitted it.'
)
COMMENT 'Raw Trino query-completed events, append-only, one row per delivered message. Grain: delivery.'
WITH (partitioning = ARRAY['day(received_at)']);
