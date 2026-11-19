-- Conflict gate: runs before anything is written. Any row returned rejects the whole batch.
-- A completed query is final. Two messages for the same query with different content mean the collection
-- pipeline is broken; picking one of them (for example "the latest wins") would hide that, and arrival
-- order is not a version number.
SELECT c.delivery_id,
       'conflicts with another message of this batch for query ' || c.cluster_id || '/' || c.query_id AS problem
FROM stg_query_content AS c
JOIN (
    SELECT cluster_id, query_id
    FROM stg_query_content
    GROUP BY cluster_id, query_id
    HAVING count(DISTINCT content_hash) > 1
) AS d
    ON d.cluster_id = c.cluster_id AND d.query_id = c.query_id
UNION ALL
SELECT c.delivery_id,
       'conflicts with the already loaded version of query ' || c.cluster_id || '/' || c.query_id
FROM stg_query_content AS c
JOIN fact_query AS f
    ON f.cluster_id = c.cluster_id AND f.query_id = c.query_id
WHERE f.content_hash <> c.content_hash
ORDER BY 1, 2;
