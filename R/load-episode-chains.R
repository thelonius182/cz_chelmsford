# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Shared script. Required objects from global env:
# - GD already authenticated
# - connection to cpnm-db: con
#
# Creates:
# - episode_chains
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

# load episode chain labels ----
# NB - `wp_title` in this sheet should be full NL-title (in lowercase)
raw_chains <- read_sheet(ss = config$gws_episode_chains, sheet = "data")
clean_chains <- raw_chains |> filter(episode_chain != "#NONE#")

# retrieve cpnm items ----
qry <- "select id, 
               type, 
               title_nl, 
               case when JSON_TYPE(JSON_EXTRACT(content, '$.nl.content')) = 'ARRAY'
                         AND JSON_LENGTH(JSON_EXTRACT(content, '$.nl.content')) > 0
                    then 'Y' else 'N' end as has_content
        from entries
        where deleted_at is null 
          and type = 'episode';"
db_cpnm_items_raw_epi <- dbGetQuery(con, qry) 

qry <- "select parent_id, 
               cast(dates->>'$.start' as datetime) as dttm_start, 
               cast(dates->>'$.end'   as datetime) as dttm_stop
        from entries
        where deleted_at is null 
          -- and site_id = 1
          and type = 'broadcast';"
db_cpnm_items_raw_bc <- dbGetQuery(con, qry) 

cpnm_epi_bc <- db_cpnm_items_raw_epi |> inner_join(db_cpnm_items_raw_bc, by = join_by(id == parent_id)) |> 
  mutate(across(where(~ inherits(.x, "POSIXct")), ~ force_tz(.x, tzone = "Europe/Amsterdam")),
         title_nl = str_replace(title_nl, "&amp;", "&"),
         title_nl = str_to_lower(title_nl)) |> 
  filter(str_length(str_trim(title_nl, side = "both")) > 0) |> 
  # filter(str_length(str_trim(title_nl, side = "both")) > 0  & dttm_start >= max_ts_to_load - days(200)) |> 
  add_bc_cols(ts_col = dttm_start) |> 
  mutate(slot_key = paste0(slot_key, "-", bc_week_of_month)) |> 
  select(ep_id = id, dttm_start, dttm_stop, slot_key, title_nl, has_content) |> 
  arrange(ep_id, dttm_start, slot_key) |> 
  group_by(ep_id) |> mutate(rn = row_number()) |> ungroup() |> 
  filter(rn == 1) |> 
  select(-rn)
  
# split chain sets ----
#   to simplify joining cpnm_epi_bc
chains_a <- clean_chains |> 
  filter(!is.na(wp_slot))
joined_slots_a <- cpnm_epi_bc |> 
  inner_join(chains_a, by = join_by(title_nl == wp_title, slot_key == wp_slot)) 
  # filter(dttm_start < max_ts_to_load)
chains_b <- clean_chains |> 
  filter(is.na(wp_slot))
joined_slots_b <- cpnm_epi_bc |> 
  inner_join(chains_b, by = join_by(title_nl == wp_title)) 
  # filter(dttm_start < max_ts_to_load)

# combine both sets again ----
episode_chains <- joined_slots_a |> 
  bind_rows(joined_slots_b) |> 
  arrange(episode_chain, desc(dttm_start)) |> 
  mutate(is_replay = F) |> 
  select(slot = dttm_start, slot_key, is_replay, episode_entry_id = ep_id, episode_chain, has_content)
