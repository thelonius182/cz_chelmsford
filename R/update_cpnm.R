# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = 
# Shared script in explicit env = upd_cpnm_env. Required objects from global env:
# - tib_clock:      sic
# - con:            connection to cpnm-db
# - episode_chains: sic
# - log_slug:       name/slug of logfile 
# - cur_site:       sic
# - TZ_AM:          timezone Europe/Amsterdan
#
# Creates:
# - n_new_episodes_fresh
# - n_new_episodes_replay
# - tib_clock_upd
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = 

# > add new to database ----
# do new ones first, so they can become part of their episode chains before replays are selected
new_episodes_fresh <- tib_clock |> filter(!is_replay & is.na(episode_entry_id))

# format creation date the way the database expects it: as UTC
ts_now = now()
fmt_created_at = format(ts_now, tz = "UTC", usetz = FALSE)

if (nrow(new_episodes_fresh) > 0) {
  flog.info(str_glue("adding fresh clock items to database, using `created_at` = {fmt_created_at} UTC"), name = log_slug)
  
  # use identical `created_at` timestamps for all rows, making it easy to select them as a set
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
  bind_rows(tib_clock, envir = upd_cpnm_env) |> 
  filter(!is_replay) |> 
  select(episode_chain, slot, episode_entry_id) |> 
  arrange(episode_chain, desc(slot)) |> 
  distinct()

# . prep replays ----
new_episodes_replay <- tib_clock |> 
  select(-genre, -ty_genre_id) |> 
  distinct() |> 
  filter(is_replay & is.na(episode_entry_id)) 

# . lookup replays ----
n <- nrow(new_episodes_replay)

if (n > 0) {
  flog.info("adding replays to database, same `created_at`", name = log_slug)
  
  # prep vectors for tibble used later in 'update the clock'
  slot <- new_episodes_replay$slot
  episode_entry_id <- character(n)
  
  for (rn in seq_len(n)) {
    episode_entry_id[rn] <- lookup_replay(pm_chains = episode_chains.2,
                                          pm_cur_chain = new_episodes_replay$episode_chain[rn],
                                          pm_replay_target_slot = new_episodes_replay$slot[rn],
                                          pm_bc_type = new_episodes_replay$uitzendtype[rn],
                                          pm_nipperstudio = new_episodes_replay$nipper_mogelijk[rn],
                                          pm_start_of_week = start_ts)
    # log as "not found"
    if (episode_entry_id[rn] == "NOT FOUND") {
      v1 <- new_episodes_replay$titel_NL[rn]
      v2 <- new_episodes_replay$slot[rn]
      v3 = new_episodes_replay$episode_chain[rn]
      flog.error(str_glue("no replay found for {v1}, target slot {v2}, of chain {v3}"), name = log_slug)
    } else {
      # add broadcast to the episode to be replayed
      ins_result <- cpnm_bc_ins(pm_pgm_id = new_episodes_replay$pgm_id[rn],
                                pm_epi_id = episode_entry_id[rn],
                                pm_site_id = cur_site,
                                pm_bc_start = new_episodes_replay$slot[rn],
                                pm_bc_minutes = new_episodes_replay$slot_minutes[rn],
                                pm_created_at = ts_now,
                                pm_cpnm_db = con)
    }
  }
  
  # . update the clock ----
  replay_updates <- tibble(slot = slot, episode_entry_id = episode_entry_id) |>
    left_join(episode_chains.2, by = join_by(episode_entry_id)) |>
    select(slot = slot.x, episode_entry_id, replay_source_slot = slot.y)
  
  dummy_replay_slot <- ymd_hms("1958-12-25 13:00:00", tz = TZ_AM, quiet = T)
  tib_clock <- tib_clock |>
    mutate(replay_source_slot = if_else(is_replay, dummy_replay_slot, NA_POSIXct_)) |>
    rows_update(replay_updates, by = "slot") |>
    select(1:6, replay_source_slot, everything())
}

result <- list(
  n_new_episodes_fresh = nrow(new_episodes_fresh),
  n_new_episodes_replay = nrow(new_episodes_replay),
  tib_clock_upd <- tib_clock
)
