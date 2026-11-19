-- Team attribution, in exactly one place.
-- A query belongs to the team its user was a member of when the query was SUBMITTED (create_time).
-- Users without a membership row for that moment fall into UNASSIGNED, so no consumption is ever dropped:
-- the view has exactly the same rows as fact_query.
CREATE OR REPLACE VIEW v_query_team AS
SELECT
    f.*,
    COALESCE(h.team_id, 'UNASSIGNED') AS team_id
FROM fact_query AS f
LEFT JOIN user_team_history AS h
    ON  h.user_name = f.user_name
    AND f.create_time >= h.valid_from
    AND (h.valid_to IS NULL OR f.create_time < h.valid_to);
