with broadcasts as (
    select cast(b.dates->>'$.start' as datetime) as bc_start,
           cast(b.dates->>'$.end'   as datetime) as bc_stop
    from entries b join entries e on e.id = b.parent_id
                                 and e.type = 'episode'
                                 and e.deleted_at is null
    where cast(b.dates->>'$.start' as datetime) between ? and ?
      and b.type = 'broadcast'
      and b.site_id = 1
      and b.deleted_at is null
),
ordered as (
    select bc_start,
           bc_stop,
           lead(bc_start) over (order by bc_start) as next_bc_start
    from broadcasts
)
select bc_start,
       bc_stop,
       next_bc_start
from ordered
where next_bc_start is not null
  and bc_stop != next_bc_start
order by bc_start
;
