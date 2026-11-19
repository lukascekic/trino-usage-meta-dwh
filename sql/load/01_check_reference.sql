-- Reference-data gate. Any row returned rejects the team data.
-- Runs on a new version in the staging tables before it replaces the published one, and on the published
-- tables before every load. {{teams}} and {{memberships}} name the tables being checked.
-- Overlapping membership intervals would attach one query to two teams and double its resources.
SELECT 'overlapping membership: ' || a.user_name || ' from ' || CAST(a.valid_from AS varchar) || ' and from ' || CAST(b.valid_from AS varchar) AS problem
FROM {{memberships}} AS a
JOIN {{memberships}} AS b
    ON  a.user_name = b.user_name
    AND a.valid_from < b.valid_from
    AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
UNION ALL
SELECT 'duplicate membership start: ' || user_name || ' from ' || CAST(valid_from AS varchar)
FROM {{memberships}}
GROUP BY user_name, valid_from
HAVING count(*) > 1
UNION ALL
SELECT 'empty or negative membership interval: ' || user_name || ' from ' || CAST(valid_from AS varchar)
FROM {{memberships}}
WHERE valid_to <= valid_from
UNION ALL
SELECT 'membership refers to unknown team: ' || h.user_name || ' -> ' || h.team_id
FROM {{memberships}} AS h
LEFT JOIN {{teams}} AS t ON t.team_id = h.team_id
WHERE t.team_id IS NULL
UNION ALL
SELECT 'duplicate team: ' || team_id
FROM {{teams}}
GROUP BY team_id
HAVING count(*) > 1
UNION ALL
SELECT 'reserved team UNASSIGNED is missing from dim_team'
WHERE NOT EXISTS (SELECT 1 FROM {{teams}} WHERE team_id = 'UNASSIGNED');
