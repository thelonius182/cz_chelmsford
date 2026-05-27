with params as (select ? as bc_start,
                       ? as tgt_site_id
), ds1 as (
select cast(b.dates->>'$.start' as datetime) as bc_start,
	   cast(b.dates->>'$.end'   as datetime) as bc_stop,
	   e.title_nl as bc_name,
     e.id as ep_id,
     b.id as bc_id,
     e.parent_id as pgm_id_prod,
     case when JSON_TYPE(JSON_EXTRACT(e.content, '$.nl.content')) = 'ARRAY'
           AND JSON_LENGTH(JSON_EXTRACT(e.content, '$.nl.content')) > 0
          then 'Y' else 'N' end as ep_has_content
from entries b join params p
               join entries e on e.id = b.parent_id
							               and e.deleted_at is null
							               and e.type = 'episode'
							               -- and e.site_id = ?
where cast(b.dates->>'$.start' as datetime) >= p.bc_start
	and b.deleted_at is null
	and b.type = 'broadcast'
	and b.site_id = p.tgt_site_id
), ds2 as (
select ep_id, min(cast(b.dates->>'$.start' as datetime)) as first_bc
from ds1 join params p
         join entries b on b.parent_id = ep_id
	                     and b.deleted_at is null
	                     and b.type = 'broadcast'
					             and b.site_id = p.tgt_site_id
group by ep_id
), ds3 as (
select ep_id, b.id as first_bc_id
from ds2 join params p
         join entries b on b.parent_id = ep_id
	                     and b.deleted_at is null
	                     and b.type = 'broadcast'
					             and b.site_id = p.tgt_site_id
                       and cast(b.dates->>'$.start' as datetime) = first_bc
)
select ds1.bc_start,
       ds1.bc_stop,
       ds1.bc_name,
       ds1.pgm_id_prod,
       ds1.ep_id,
       ds1.ep_has_content,
       ds1.bc_id,
       case when ds1.bc_start = ds2.first_bc then null else ds2.first_bc    end as bc_rp_source_start,
       case when ds1.bc_start = ds2.first_bc then null else ds3.first_bc_id end as bc_rp_source_id
from ds1 join ds2 on ds2.ep_id = ds1.ep_id
         join ds3 on ds3.ep_id = ds1.ep_id     
order by 1;