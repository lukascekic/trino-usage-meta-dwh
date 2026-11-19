-- Which delivery batches still have to be loaded, in the order they were received.
-- Batches are chosen by the DELIVERY log, never by query time, so an event that arrives days late is
-- still picked up. A batch that was started but not finished (crash) or was rejected is pending again.
SELECT r.batch_id
FROM {{raw_schema}}.query_events AS r
LEFT JOIN etl_batch AS b
    ON b.batch_id = r.batch_id AND b.status = 'SUCCEEDED'
WHERE b.batch_id IS NULL
GROUP BY r.batch_id
ORDER BY min(r.received_at), r.batch_id;
