-- select all episodes that have broadcasts on <site_id> this week
drop table if exists cpnm_deletes;
-- @bind: insert_values
CREATE TABLE cpnm_deletes (
    id char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci PRIMARY KEY
) ENGINE = MEMORY
AS
SELECT DISTINCT e.id
FROM entries b JOIN entries e ON e.id = b.parent_id
WHERE b.type = 'broadcast'
  AND b.deleted_at IS NULL
  AND b.dates->>'$.start' BETWEEN ? AND ?
  AND b.site_id = ?
  AND e.type = 'episode'
  AND e.deleted_at IS NULL
;
-- @bind: delete those broadcasts
DELETE b
FROM entries b JOIN cpnm_deletes c ON c.id = b.parent_id
WHERE b.type = 'broadcast'
  AND b.site_id = ?
;
-- delete taxonomables of episodes no longer referencing any active broadcasts
DELETE txb
FROM taxonomables txb JOIN cpnm_deletes c ON c.id = txb.taxonomable_id
                      JOIN entries e ON e.id = c.id
WHERE txb.taxonomable_type = 'episode'
  AND e.type = 'episode'
  AND NOT EXISTS (SELECT 1
                  FROM entries b
                  WHERE b.active_broadcast_episode_id = e.active_episode_id)
;
-- delete the episodes no longer referencing any active broadcasts
DELETE e
FROM entries e JOIN cpnm_deletes c ON c.id = e.id
               LEFT JOIN entries b ON b.active_broadcast_episode_id = e.active_episode_id
WHERE e.type = 'episode'
  AND b.id IS NULL
;
