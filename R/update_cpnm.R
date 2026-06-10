# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = 
# Shared script in explicit env = upd_cpnm_env. Required objects from global env:
# - tib_clock:      sic
# - con:            connection to cpnm-db
# - episode_chains: sic
# - lacie_chains:   sic
# - log_slug:       name/slug of logfile 
# - cur_site:       sic
# - TZ_AM:          timezone Europe/Amsterdan
#
# Creates:
# - n_new_episodes_fresh
# - n_new_episodes_replay
# - tib_clock_upd
# - lacie_chains_upd
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = 

# > add new to database ----
# do new ones first, so they can become part of their episode chains before replays are selected
new_episodes_fresh <- tib_clock |> filter(!is_replay & is.na(episode_entry_id))

# format creation date the way the database expects it: as UTC
ts_now = now()
# ts_now = ymd_hms("2026-05-16 17:39:43")
# use identical `created_at` timestamps for all rows, making it easy to select them as a set
fmt_created_at = format(ts_now, tz = "UTC", usetz = FALSE)

if (nrow(new_episodes_fresh) > 0) {
  flog.info(str_glue("adding newest clock items to database, using `created_at` = {fmt_created_at} UTC"), name = log_slug)
  
  func_result <- clock2db(pm_clock_tib = new_episodes_fresh,
                          pm_created_at = ts_now,
                          pm_site = cur_site,
                          pm_db = con)
  
  # . update clock tibble rows ----
  qry <- glue_sql(
    "select cast(b.dates->>'$.start' as datetime) as slot,
            e.id as episode_entry_id
     from entries b join entries e on e.id = b.parent_id
                                  and e.deleted_at is null
                                  and e.type = 'episode'
     where b.deleted_at is null
       and b.type = 'broadcast'
       and b.site_id = {cur_site}
       and b.created_at = {fmt_created_at}
     order by 1;", .con = con)
  db_new_items <- dbGetQuery(con, qry) |> mutate(slot = force_tz(slot, tzone = TZ_AM))
  tib_clock <- tib_clock |> rows_update(db_new_items, by = "slot")
}

# > add replays to database ----
# . prep episode chains ----
episode_chains <- episode_chains |>
  bind_rows(tib_clock) |> 
  filter(!is_replay) |> 
  select(episode_chain, ec_slot = slot, episode_entry_id) |> 
  arrange(episode_chain, desc(ec_slot)) |> 
  distinct()

# . prep replays ----
new_episodes_replay <- tib_clock |> 
  select(-genre, -ty_genre_id) |> 
  distinct() |> 
  filter(is_replay & is.na(episode_entry_id)) 

# . lookup replays ----
n <- nrow(new_episodes_replay)

if (n > 0) {
  flog.info(str_glue("adding replay clock items to database, using `created_at` = {fmt_created_at} UTC"), name = log_slug)
  # prep vectors for tibble used later in 'update the clock'
  prep_slot <- new_episodes_replay$slot
  prep_episode_entry_id <- character(n)
  
  for (rn in seq_len(n)) {
    
    prep_episode_entry_id[rn] <- 
      if (cur_site == SITE$WORLD_OF_JAZZ && new_episodes_replay$prod_type[rn] == PROD_TYPE$LACIE) {
        la_list <- fetch_lacie(pm_chain = new_episodes_replay$episode_chain[rn],
                               pm_lacie_chains = lacie_chains)
        lacie_chains <- la_list[[1]]
        la_list[[2]] |> pull(ep_id)
      } else {
        lookup_replay(pm_chains = episode_chains,
                      pm_cur_chain = new_episodes_replay$episode_chain[rn],
                      pm_replay_target_slot = new_episodes_replay$slot[rn],
                      pm_bc_type = new_episodes_replay$uitzendtype[rn], # live/semi-live
                      pm_nipperstudio = new_episodes_replay$nipper_mogelijk[rn],
                      pm_replay_week_start = bc_week_start(pm_slot_start = new_episodes_replay$slot[rn], 
                                                           pm_site_id = cur_site))
        
      }
    
    # log as "not found"
    if (prep_episode_entry_id[rn] == "NOT FOUND") {
      v1 <- new_episodes_replay$titel_NL[rn]
      v2 <- new_episodes_replay$slot[rn]
      v3 <- new_episodes_replay$episode_chain[rn]
      v4 <- new_episodes_replay$slot_key[rn]
      flog.error(str_glue("no replay found for {v4} = {v2}, chain {v3}, {v1}"), name = log_slug)
    } else {
      # add broadcast to the episode to be replayed
      ins_result <- cpnm_bc_ins(pm_pgm_id = new_episodes_replay$pgm_id[rn],
                                pm_epi_id = prep_episode_entry_id[rn],
                                pm_site_id = cur_site,
                                pm_bc_start = new_episodes_replay$slot[rn],
                                pm_bc_minutes = new_episodes_replay$slot_minutes[rn],
                                pm_created_at = ts_now,
                                pm_cpnm_db = con)
    }
  }
  
  # . update the clock ----
  replay_updates <- tibble(slot = prep_slot, episode_entry_id = prep_episode_entry_id) |>
    left_join(episode_chains, by = join_by(episode_entry_id)) |>
    select(slot, episode_entry_id, replay_source_slot = ec_slot)
  
  # dummy_replay_slot <- ymd_hms("1958-12-25 13:00:00", tz = TZ_AM, quiet = T)
  tib_clock <- tib_clock |>
    # mutate(replay_source_slot = if_else(is_replay, dummy_replay_slot, NA_POSIXct_)) |>
    mutate(replay_source_slot = as.POSIXct(NA, tz = "Europe/Amsterdam")) |>
    rows_update(replay_updates, by = "slot") |>
    select(1:6, replay_source_slot, everything())
}

result <- list(
  n_new_episodes_fresh  = nrow(new_episodes_fresh),
  n_new_episodes_replay = nrow(new_episodes_replay),
  tib_clock_upd = tib_clock,
  lacie_chains_upd = lacie_chains
)
