select * from test_cpnm_latest.entries
where parent_id = '1f8fc8bf-9438-4587-bcbe-a04c68303fae'
  and type = 'broadcast'
  and deleted_at is null
;
SELECT
  id,
  site_id,
  `type`,
  deleted_at,
  JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) AS raw_start,
  CAST(JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) AS DATETIME) AS parsed_start
FROM entries
WHERE `type` = 'broadcast'
  AND deleted_at IS NULL
  AND (
    JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) IS NULL
    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) AS DATETIME) IS NULL
  )
;
  -- needs to be empty:
SELECT
  site_id,
  CAST(dates->>'$.start' AS DATETIME) AS parsed_start,
  COUNT(*) AS n
FROM entries
WHERE `type` = 'broadcast'
  AND deleted_at IS NULL
GROUP BY site_id, parsed_start
HAVING COUNT(*) > 1
;
-- check duplicates
SELECT
  e.id,
  e.site_id,
  e.type,
  e.deleted_at,
  JSON_UNQUOTE(JSON_EXTRACT(e.dates, '$.start')) AS raw_start,
  CAST(JSON_UNQUOTE(JSON_EXTRACT(e.dates, '$.start')) AS DATETIME) AS parsed_start,
  e.title_nl,
  e.title_en,
  e.created_at,
  e.updated_at
FROM entries e
JOIN (
  SELECT
    site_id,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS DATETIME) AS parsed_start
  FROM entries
  WHERE type = 'broadcast'
    AND deleted_at IS NULL
  GROUP BY site_id, parsed_start
  HAVING COUNT(*) > 1
) d
  ON d.site_id = e.site_id
 AND d.parsed_start = CAST(JSON_UNQUOTE(JSON_EXTRACT(e.dates, '$.start')) AS DATETIME)
WHERE e.type = 'broadcast'
  AND e.deleted_at IS NULL
ORDER BY e.site_id, parsed_start, e.created_at, e.id
;
-- drop temporary table clean_up_2026;
-- create temporary table clean_up_2026 as
-- drop temporary table clean_up_LT_2026;
create temporary table clean_up_LT_2026 as
-- drop temporary table clean_up_last_batch;
-- create temporary table clean_up_last_batch as
WITH ranked AS (
  SELECT
    id,
    parent_id,
    site_id,
    title_nl,
    title_en,
    created_at,
    updated_at,
    JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS raw_start,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS DATETIME) AS parsed_start,
    ROW_NUMBER() OVER (
      PARTITION BY
        site_id,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS DATETIME)
      ORDER BY created_at ASC, id ASC
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY
        site_id,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS DATETIME)
    ) AS group_size
  FROM entries
  WHERE type = 'broadcast'
    AND deleted_at IS NULL
)
SELECT
  CASE WHEN rn = 1 THEN 'KEEP' ELSE 'DUPLICATE' END AS action,
  group_size,
  site_id,
  parsed_start,
  id,
  parent_id,
  title_nl,
  title_en,
  created_at,
  updated_at
FROM ranked
WHERE group_size > 1
ORDER BY site_id, parsed_start, rn
;
delete from entries where type = 'broadcast' and id in
(select cu.id from clean_up_2026 cu where parsed_start >= '2026-01-01 00:00:00' and action = 'DUPLICATE')
;
select c.*, 
       e.legacy_data->>'$.wp_post_id' as wp_id
from clean_up_LT_2026 c join entries e on e.id = c.parent_id
                                      and e.type = 'episode'
									  and e.deleted_at is null
where e.legacy_data->>'$.wp_post_id' = 879060
order by 2
;
create temporary table clean_up_897060 as
select c.id 
from clean_up_LT_2026 c join entries e on e.id = c.parent_id
                                      and e.type = 'episode'
									  and e.deleted_at is null
where e.legacy_data->>'$.wp_post_id' = 879060
;
delete from entries where type = 'broadcast' and id in
(select id from clean_up_897060)
;
delete from entries where type = 'broadcast' and id in
(select id from clean_up_last_batch where action = 'DUPLICATE')
;
-- FINALLY!
ALTER TABLE entries
ADD CONSTRAINT entries_chk_broadcast_start_datetime
CHECK (
  `type` <> 'broadcast'
  OR `deleted_at` IS NOT NULL
  OR (
    JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) IS NOT NULL
    AND CAST(JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) AS DATETIME) IS NOT NULL
  )
)
;
CREATE UNIQUE INDEX entries_unique_broadcast_start_per_site
ON entries (
  `site_id`,
  (CASE
     WHEN `type` = 'broadcast'
      AND `deleted_at` IS NULL
     THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(`dates`, '$.start')) AS DATETIME)
     ELSE NULL
   END)
);