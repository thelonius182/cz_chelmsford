select b.dates->>'$.start' as bc_start,
       e.id as ep_id,
	     replace(e.title_nl, '&amp;', '&') as ep_title,
	     b.duration as duur,
	     k.ep_catkey as playlist,
	     (select min(dates->>'$.start') from entries 
	                      where type = 'broadcast' 
	                        and deleted_at is null 
	                        and parent_id = e.id
	     ) as min_bc_start
from entries e join entries b on b.parent_id = e.id 
								             and b.deleted_at is null 
								             and b.type = 'broadcast'
					     left join episode_catlg_keys k on k.ep_id = e.id 
where e.deleted_at is null
  and e.type = 'episode'
  and b.dates->>'$.start' between ? and ?
  and b.site_id = ?
order by 1
;
