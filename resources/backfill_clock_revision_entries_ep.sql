-- Episodes first (to satisfy the FK)
INSERT INTO clock_revision_entries (
  `id`,
  `type`,
  `surtitle`,
  `title`,
  `subtitle`,
  `slug`,
  `description`,
  `content`,
  `blocks`,
  `sections`,
  `dates`,
  `attributes`,
  `properties`,
  `seo`,
  `playlist`,
  `duration`,
  `order`,
  `parent_id`,
  `image_id`,
  `media_id`,
  `site_id`,
  `user_id`,
  `legacy_id`,
  `legacy_type`,
  `legacy_data`,
  `published_at`,
  `created_at`,
  `updated_at`,
  `deleted_at`
)
WITH selected_broadcasts AS (
  SELECT distinct parent_id
  FROM entries
  WHERE type = 'broadcast'
    AND deleted_at IS NULL
    AND site_id = 1
    AND dates->>'$.start' >= ? AND dates->>'$.start' < ?
)
SELECT 
  e.`id`,
  e.`type`,
  e.`surtitle`,
  e.`title`,
  e.`subtitle`,
  e.`slug`,
  e.`description`,
  e.`content`,
  e.`blocks`,
  e.`sections`,
  e.`dates`,
  e.`attributes`,
  e.`properties`,
  e.`seo`,
  e.`playlist`,
  e.`duration`,
  e.`order`,
  e.`parent_id`,
  e.`image_id`,
  e.`media_id`,
  e.`site_id`,
  e.`user_id`,
  e.`legacy_id`,
  e.`legacy_type`,
  e.`legacy_data`,
  e.`published_at`,
  e.`created_at`,
  e.`updated_at`,
  e.`deleted_at`
FROM entries e
JOIN selected_broadcasts b
  ON b.parent_id = e.id
WHERE e.type = 'episode'
  AND e.deleted_at IS NULL
;
