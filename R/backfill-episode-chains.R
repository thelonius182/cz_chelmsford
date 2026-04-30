
# downloads GD ----
# . trigger GD-auth
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# . get episode chain labels from GD
path_chains_cz <- "/home/lon/R_projects/cz_chelmsford/resources/chains_cz.xlsx"
drive_download(file = cz_get_url("chains_cz"), overwrite = T, path = path_chains_cz)
# df_raw_nipper_fresh <- cz_extract_sheet(path_chains_cz, sheet_name = "nipper_fresh")
# df_nipper_fresh <- df_raw_nipper_fresh |> mutate(wp_title = str_to_lower(wp_title))
df_raw_chains <- cz_extract_sheet(path_chains_cz, sheet_name = "wp_chains")
df_clean_chains <- df_raw_chains |> filter(episode_chain != "#NONE#")

# rerun query on Nipper first
wp_slot_alloc_raw <- read_tsv(file = "resources/clockfactory_slot_allocation.tsv", col_types = "cccc")

qry <- "select id, parent_id, type, title_nl, 
               case when JSON_TYPE(JSON_EXTRACT(content, '$.nl.content')) = 'ARRAY'
                         AND JSON_LENGTH(JSON_EXTRACT(content, '$.nl.content')) > 0
                    then 'Y' else 'N' end as has_content,
               cast(dates->>'$.start' as datetime) as dttm_start, 
               cast(dates->>'$.end'   as datetime) as dttm_stop
        from entries 
        where deleted_at is null 
          and site_id = 1
          and type in ('broadcast', 'episode')
;"
db_cpnm_items_raw <- dbGetQuery(con, qry) 
df_cpnm_items <- db_cpnm_items_raw |> mutate(dttm_start = force_tz(dttm_start, tzone = "Europe/Amsterdam"),
                                             dttm_stop  = force_tz(dttm_stop,  tzone = "Europe/Amsterdam"),
                                             title_nl = str_to_lower(title_nl),
                                             dttm_start = if_else(type == "episode" & !is.na(dttm_start), NA_POSIXct_, dttm_start),
                                             dttm_stop  = if_else(type == "episode" & !is.na(dttm_start), NA_POSIXct_, dttm_stop))

df_cpnm_broadcasts <- df_cpnm_items |> 
  filter(type == "broadcast" & dttm_start >= ymd_hms("2023-12-07 13:00:00", tz = "Europe/Amsterdam", quiet = T)) |> 
  add_bc_cols(ts_col = dttm_start) |> mutate(slot_key = paste0(slot_key, "-", bc_week_of_month)) |> 
  select(parent_id, dttm_start, slot_key, title_nl) |> arrange(parent_id, dttm_start, slot_key) |> 
  group_by(parent_id) |> mutate(rn = row_number()) |> ungroup() |> filter(rn == 1) |> select(-rn)
  
df_cpnm_episodes <- df_cpnm_items |> filter(type == "episode") |> select(id, title_nl, has_content) |> arrange(title_nl)

df_cpnm_epi_bc <- df_cpnm_episodes |> inner_join(df_cpnm_broadcasts, by = join_by(title_nl, id == parent_id))

# split chain tibbles in conditional (a) and unconditional ones (b), to simplify joining with slots
df_chains_a <- df_clean_chains |> filter(!is.na(wp_slot))
df_joined_slots_a <- df_cpnm_epi_bc |> inner_join(df_chains_a, by = join_by(title_nl == wp_title, slot_key == wp_slot)) |> 
  filter(dttm_start < stop_ts)
df_chains_b <- df_clean_chains |> filter(is.na(wp_slot))
df_joined_slots_b <- df_cpnm_epi_bc |> inner_join(df_chains_b, by = join_by(title_nl == wp_title)) |> 
  filter(dttm_start < stop_ts)

df_joined_slots_backfill <- df_joined_slots_a |> bind_rows(df_joined_slots_b) |> arrange(episode_chain, dttm_start) |> 
  select(-wp_slot)

