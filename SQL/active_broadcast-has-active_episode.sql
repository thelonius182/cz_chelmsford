-- Active broadcasts without an active episode 
drop temporary table orphan_broadcasts;
-- create temporary table orphan_broadcasts as
SELECT
    b.id AS bc_id,
    b.parent_id AS bc_parent_id,
    b.deleted_at as bc_deleted_at,
    e.type AS bc_parent_type,
    e.deleted_at AS parent_deleted_at
FROM entries AS b
LEFT JOIN entries AS e
       ON e.id = b.parent_id
WHERE b.type = 'broadcast'
  AND b.deleted_at IS NULL
  AND (
       b.parent_id IS NULL
       OR e.id IS NULL
       OR e.type <> 'episode'
       OR e.deleted_at IS NOT NULL
  )
;
delete from entries where `type` = 'broadcast' and id in
(select bc_id from orphan_broadcasts)
;
-- Active episodes must have some parent_id.
-- This does NOT require that parent to be a program.
-- create temporary table orphan_episode as
SELECT
    id AS episode_id,
    parent_id
FROM entries
WHERE type = 'episode'
  AND deleted_at IS NULL
  AND parent_id IS NULL
;
delete from entries where `type` = 'episode' and id in
(select episode_id from orphan_episode)
;
ALTER TABLE entries
  ADD COLUMN active_episode_id char(36)
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    GENERATED ALWAYS AS (
      CASE
        WHEN type = 'episode'
         AND deleted_at IS NULL
        THEN id
        ELSE NULL
      END
    ) STORED,

  ADD UNIQUE KEY entries_active_episode_id_uq (active_episode_id),

  ADD COLUMN active_broadcast_episode_id char(36)
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    GENERATED ALWAYS AS (
      CASE
        WHEN type = 'broadcast'
         AND deleted_at IS NULL
        THEN parent_id
        ELSE NULL
      END
    ) STORED,

  ADD KEY entries_active_broadcast_episode_idx
    (active_broadcast_episode_id),

  ADD CONSTRAINT entries_chk_active_episode_has_parent
    CHECK (
      type <> 'episode'
      OR deleted_at IS NOT NULL
      OR parent_id IS NOT NULL
    ),

  ADD CONSTRAINT entries_chk_active_broadcast_has_parent
    CHECK (
      type <> 'broadcast'
      OR deleted_at IS NOT NULL
      OR parent_id IS NOT NULL
    )
;
ALTER TABLE entries
  ADD CONSTRAINT entries_active_broadcast_episode_fk
    FOREIGN KEY (active_broadcast_episode_id)
    REFERENCES entries (active_episode_id)
;