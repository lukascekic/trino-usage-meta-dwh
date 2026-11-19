-- Publish a new version of the team reference data that 01_check_reference.sql has accepted in the staging
-- tables stg_dim_team and stg_user_team_history.
-- Each table is brought to the new version with ONE MERGE, which is one Iceberg commit: rows that are in the
-- new version are inserted or updated, rows that are not are deleted. A reader therefore sees either the old
-- or the new memberships, never a table that is empty or half replaced. The two tables are two commits;
-- v_query_team reads only user_team_history, so team attribution never sees the moment in between.

MERGE INTO dim_team AS t
USING (
    SELECT team_id, team_name, false AS obsolete
    FROM stg_dim_team
    UNION ALL
    SELECT d.team_id, d.team_name, true
    FROM dim_team AS d
    WHERE NOT EXISTS (SELECT 1 FROM stg_dim_team AS s WHERE s.team_id = d.team_id)
) AS s
ON t.team_id = s.team_id
WHEN MATCHED AND s.obsolete THEN DELETE
WHEN MATCHED THEN UPDATE SET team_name = s.team_name
WHEN NOT MATCHED THEN INSERT (team_id, team_name) VALUES (s.team_id, s.team_name);

MERGE INTO user_team_history AS t
USING (
    SELECT user_name, team_id, valid_from, valid_to, false AS obsolete
    FROM stg_user_team_history
    UNION ALL
    SELECT h.user_name, h.team_id, h.valid_from, h.valid_to, true
    FROM user_team_history AS h
    WHERE NOT EXISTS (
        SELECT 1 FROM stg_user_team_history AS s WHERE s.user_name = h.user_name AND s.valid_from = h.valid_from
    )
) AS s
ON t.user_name = s.user_name AND t.valid_from = s.valid_from
WHEN MATCHED AND s.obsolete THEN DELETE
WHEN MATCHED THEN UPDATE SET team_id = s.team_id, valid_to = s.valid_to
WHEN NOT MATCHED THEN INSERT (user_name, team_id, valid_from, valid_to)
    VALUES (s.user_name, s.team_id, s.valid_from, s.valid_to);
