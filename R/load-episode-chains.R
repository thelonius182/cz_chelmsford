# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Shared script. Required objects from global env:
# - max_ts_to_load
# - connection to cpnm-db: con
#
# Creates:
# - episode_chains
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

# load episode chain labels ----
with_drive_quiet(
  # . trigger GD-auth
  drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
)

path_chains_cz <- "/home/lon/R_projects/cz_chelmsford/resources/chains_cz.xlsx"
with_drive_quiet(
  drive_download(file = cz_get_url("chains_cz"), overwrite = T, path = path_chains_cz)
)
raw_chains <- cz_extract_sheet(path_chains_cz, sheet_name = "wp_chains")
clean_chains <- raw_chains |> filter(episode_chain != "#NONE#")

# retrieve cpnm items ----
qry <- "select id, 
               parent_id, 
               type, 
               title_nl, 
               case when JSON_TYPE(JSON_EXTRACT(content, '$.nl.content')) = 'ARRAY'
                         AND JSON_LENGTH(JSON_EXTRACT(content, '$.nl.content')) > 0
                    then 'Y' else 'N' end as has_content,
               cast(dates->>'$.start' as datetime) as dttm_start, 
               cast(dates->>'$.end'   as datetime) as dttm_stop
        from entries
        where deleted_at is null 
          and site_id = 1
          and type in ('broadcast', 'episode');"
db_cpnm_items_raw <- dbGetQuery(con, qry) 
cpnm_items <- db_cpnm_items_raw |> mutate(dttm_start = force_tz(dttm_start, tzone = "Europe/Amsterdam"),
                                          dttm_stop  = force_tz(dttm_stop,  tzone = "Europe/Amsterdam"),
                                          title_nl = str_to_lower(title_nl),
                                          title_nl = str_replace(title_nl, "&amp;", "&"),
                                          dttm_start = if_else(type == "episode" & !is.na(dttm_start), NA_POSIXct_, dttm_start),
                                          dttm_stop  = if_else(type == "episode" & !is.na(dttm_start), NA_POSIXct_, dttm_stop))

# prep broadcasts ----
cpnm_broadcasts <- cpnm_items |> 
  filter(type == "broadcast" & dttm_start >= max_ts_to_load - days(200)) |> 
  add_bc_cols(ts_col = dttm_start) |> 
  mutate(slot_key = paste0(slot_key, "-", bc_week_of_month)) |> 
  select(parent_id, dttm_start, dttm_stop, slot_key, title_nl) |> 
  arrange(parent_id, dttm_start, slot_key) |> 
  group_by(parent_id) |> mutate(rn = row_number()) |> ungroup() |> 
  filter(rn == 1) |> select(-rn)
  
# prep episodes ----
cpnm_episodes <- cpnm_items |> 
  filter(type == "episode") |> 
  select(id, title_nl, has_content) |> 
  arrange(title_nl)

# combine bc+epi ----
cpnm_epi_bc <- cpnm_episodes |> 
  inner_join(cpnm_broadcasts, by = join_by(title_nl, id == parent_id))

# split chain sets ----
#   to simplify joining cpnm_epi_bc
chains_a <- clean_chains |> 
  filter(!is.na(wp_slot))
joined_slots_a <- cpnm_epi_bc |> 
  inner_join(chains_a, by = join_by(title_nl == wp_title, slot_key == wp_slot)) |> 
  filter(dttm_start < max_ts_to_load)
chains_b <- clean_chains |> 
  filter(is.na(wp_slot))
joined_slots_b <- cpnm_epi_bc |> 
  inner_join(chains_b, by = join_by(title_nl == wp_title)) |> 
  filter(dttm_start < max_ts_to_load)

# combine both sets again ----
episode_chains <- joined_slots_a |> 
  bind_rows(joined_slots_b) |> 
  arrange(episode_chain, dttm_start) |> 
  mutate(is_replay = F) |> 
  select(slot = dttm_start, slot_key, is_replay, episode_entry_id = id, episode_chain)
