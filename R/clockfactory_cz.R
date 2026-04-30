# - - - - - - - - - - - - -
# Build CZ programme clock
# - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI, digest,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
apf <- flog.appender(appender.file(config$log_appender_file_cz), "clof")
flog.info("\n = = = = = =  Building a programme clock for CZ = = = = = =", name = "clof")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")

SITE <- list(
  CONCERTZENDER = 1L,
  WORLD_OF_JAZZ = 2L
)

STEP <- list(
  FACTORY = 10L,
  EDITOR  = 20L,
  DESK    = 30L,
  PLAYOUT = 99L
)

step_lookup <- tibble(
  step = c(10L, 20L, 30L, 99L),
  step_name = c("FACTORY", "EDITOR", "DESK", "PLAYOUT")
)

source("R/custom_functions.R", encoding = "UTF-8")
# connect to CPNM-database ----
source("R/cpnm_db_setup.R", encoding = "UTF-8")  

# > Main Control Loop ----
repeat {
  # Find start of week to build ----
  clock_start_utc <- start_of_cz_week(pm_cpnm_db = con)
  tz_am <- "Europe/Amsterdam"
  start_ts <- force_tz(clock_start_utc$next_week_start, tzone = tz_am)
  stop_ts   <- start_ts + days(7L)
  flog.info(str_glue("Week: Thursday {logfmt_ts(start_ts)} to Thursday {logfmt_ts(stop_ts)}"), name = "clof")
  
  # clockprofile from GD ----
  # . trigger GD-auth
  drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
  path_clockprofile_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
  drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockprofile_cz)
  
  # clockcatalogue from GD ----
  path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
  drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)
  
  # sheets as df ----
  df_clockprofile_cz_raw <- cz_extract_sheet(path_clockprofile_cz, sheet_name = "cz-data")
  df_clockcatalogue_raw <- cz_extract_sheet(path_clockcatalogue, sheet_name = "gids-info")
  df_clockcatalogue <- df_clockcatalogue_raw |> rename(catalg_key = `key-modelrooster`,
                                                       titel_NL = `titel-NL`,
                                                       titel_EN = `titel-EN`,
                                                       productie = `productie-1-taak`,
                                                       redacteurs = `productie-1-mdw`,
                                                       genre_1 = `genre-1-NL`,
                                                       genre_2 = `genre-2-NL`,
                                                       intro_NL = `std.samenvatting-NL`,
                                                       intro_EN = `std.samenvatting-EN`,
                                                       afbeelding = feat_img_ids,
                                                       episode_chain = `episode-chain`) |> 
    mutate(catalg_key = str_replace(catalg_key, "_(ma|di|wo|do|vr|za|zo)", ""))
  
  # tidy clockprofile ----
  df_clockprofile_cz_raw <- df_clockprofile_cz_raw |> rename(slot_key = slot) |> 
    mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key)
  
  mk_weekly <- df_clockprofile_cz_raw |> filter(!is.na(wekelijks)) |> select(1:6) |> 
    rename(catalg_key = wekelijks, prod_type = te)
  mk_biweekly<- df_clockprofile_cz_raw |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
    rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
  mk_week.1 <- df_clockprofile_cz_raw |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
    mutate(block = 1L)
  mk_week.2 <- df_clockprofile_cz_raw |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
    mutate(block = 2L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
  mk_week.3 <- df_clockprofile_cz_raw |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
    mutate(block = 3L)
  mk_week.4 <- df_clockprofile_cz_raw |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
    mutate(block = 4L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
  mk_week.5 <- df_clockprofile_cz_raw |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
    mutate(block = 5L)
  
  # build a clock ----
  bc_week_ts <- tibble(ts = seq(from = start_ts, to = stop_ts - hours(1), by = "hour"))
  
  df_calendar <- add_bc_cols(bc_week_ts, ts) |> 
    mutate(cycle = if_else(bc_day_label == "do" & bc_hour_start == 13, week_label(ts), NA_character_)) |> 
    fill(cycle, .direction = "down") |> select(slot = ts, slot_key, block = bc_week_of_month, cycle)
  
  df_clock_cz_weekly <- df_calendar |> inner_join(mk_weekly, by = join_by(slot_key))
  
  df_clock_cz_biweekly <- df_calendar |> inner_join(mk_biweekly, by = join_by(slot_key, cycle))
  
  mk_5_weeks <- mk_week.1 |> bind_rows(mk_week.2) |>
    bind_rows(mk_week.3) |>
    bind_rows(mk_week.4) |>
    bind_rows(mk_week.5)
  
  df_clock_cz_5_weeks <- df_calendar |> inner_join(mk_5_weeks, by = join_by(slot_key, block))
  
  df_clock_cz_cur <- df_clock_cz_weekly |> bind_rows(df_clock_cz_biweekly) |> bind_rows(df_clock_cz_5_weeks) |> arrange(slot) |> 
    left_join(df_clockcatalogue, by = join_by(catalg_key)) |> 
    mutate(hh = if_else(prod_type == "h", "H", hh),
           prod_type = if_else(prod_type == "h", "u", prod_type)) |> 
    mutate(is_replay = if_else(is.na(hh), FALSE, TRUE)) |> 
    select(-hh, -mac, -`dummy-1`, -slug) |> 
    pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  
  # validate clock length ----
  cur_clock_minutes <- df_clock_cz_cur |> select(slot, slot_minutes) |> distinct() |> summarise(total = sum(slot_minutes))
  
  if (cur_clock_minutes$total != 10080L) {
    flog.error("invalid clock length (1): expected 10080 minutes, but got {cur_clock_minutes}; quiting this job.", name = "clof")
    break
  }
  
  # validate catalogue ----
  missing <- df_clock_cz_cur |> filter(is.na(titel_NL)) |> nrow()
  
  if (missing > 0) {
    flog.error("clock catalogue is incomplete (title); quiting this job.", name = "clof")
    break
  }
  
  missing <- df_clock_cz_cur |> filter(is.na(episode_chain)) |> nrow()
  
  if (missing > 0) {
    flog.error("clock catalogue is incomplete (episode chain); quiting this job.", name = "clof")
    break
  }
  
  # genres ----
  query <- "select name->>'$.nl' as genre_NL, 
                   id as ty_genre_id 
            from taxonomies 
            where type = 'genre' 
              and deleted_at is null
              and site_id in (1, 2)
              and name->>'$.nl' not in ('Algemeen', 'World of Jazz')
            order by 1
            ;"
  
  ty_genres <- dbGetQuery(con, query)
  df_clock_cz_cur.1 <- df_clock_cz_cur |> left_join(ty_genres, by = join_by(genre == genre_NL))
  missing <- df_clock_cz_cur.1 |> filter(is.na(ty_genre_id)) |> nrow()
  
  if (missing > 0) {
    flog.error("genres missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # editors ----
  query <- "select name->>'$.nl' as editor_name, 
                   id as ty_editor_id
            from taxonomies 
            where type = 'colofon'
              and deleted_at is null
              and site_id in (1, 2)
              and name->>'$.en' is not null
            order by 1
            ;"
  ty_editors <- dbGetQuery(con, query)
  df_clock_cz_cur.2 <- df_clock_cz_cur.1 |> left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  
  # . check complete ----
  missing <- df_clock_cz_cur.2 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct() |> nrow()
  
  if (missing > 0) {
    flog.error("editors missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # images ----
  df_afb <- df_clock_cz_cur.2 |> filter(!is.na(afbeelding)) |> select(afbeelding) |> distinct() |> 
    mutate(afbeelding = as.integer(afbeelding), image_id = NA_character_) |> arrange(afbeelding)
  
  for (rn in seq_len(nrow(df_afb))) {
    df_image <- cpnm_img_get(pm_img_id = df_afb$afbeelding[rn], pm_cpnm_db = con)
    
    if (is.na(df_image$id)) {
      next
    }
    
    df_afb$image_id[rn] <- df_image$id
  }
  
  # . check complete ----
  missing <- df_afb |> filter(is.na(image_id)) |> nrow()
  
  if (missing > 0) {
    flog.error("images are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # . append ----
  df_clock_cz_cur.3 <- df_clock_cz_cur.2 |> left_join(df_afb, by = join_by(afbeelding))
  
  # programs ----
  query <- "WITH counts AS (
                SELECT LOWER(p.title_nl) AS titel_nl_lc,
                       p.id AS pgm_id,
                       COUNT(b.id) AS n_bcs
                FROM entries p LEFT JOIN entries e ON e.parent_id = p.id
                                             and e.type = 'episode'
                                             AND e.deleted_at IS NULL
                               LEFT JOIN entries b ON b.parent_id = e.id
                                             and b.type = 'broadcast'
                                             AND b.deleted_at IS NULL
                WHERE p.type = 'program'
                  AND p.deleted_at IS NULL
                  AND p.site_id IN (1, 2)
                GROUP BY LOWER(p.title_nl),
                         p.id
            ), ranked AS (SELECT titel_nl_lc,
                                 pgm_id,
                                 n_bcs,
                                 ROW_NUMBER() OVER (PARTITION BY titel_nl_lc
                                                    ORDER BY n_bcs DESC, pgm_id) AS rn
                          FROM counts
            )
            SELECT titel_nl_lc, pgm_id FROM ranked WHERE rn = 1 ORDER BY titel_nl_lc;"
  program_titles <- dbGetQuery(con, query)
  
  df_clock_cz_cur.4 <- df_clock_cz_cur.3 |> left_join(program_titles, by = join_by("titel_nl_lc"))
  missing <- df_clock_cz_cur.4 |> filter(is.na(pgm_id)) |> nrow()
  
  # . check complete ----
  if (missing > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = "clof")
    break
  }
  
  # . check length ----
  cur_clock_minutes <- df_clock_cz_cur.4 |> distinct(slot, slot_minutes) |> summarise(total = sum(slot_minutes))
  
  if (cur_clock_minutes$total != 10080L) {
    flog.error(str_glue("invalid clock length (2): expected 10080 minutes, but got {cur_clock_minutes$total}; quiting this job."), 
               name = "clof")
    break
  }
  
  # build clock history ----
  source("R/backfill-episode-chains.R", encoding = "UTF-8")  
  
  df_clock_cz_his <- df_joined_slots_backfill |> 
    mutate(slot_minutes = int_length(int = interval(start = dttm_start, end = dttm_stop)) / 60L) |> 
    select(slot = dttm_start, slot_key, slot_minutes, episode_chain, episode_entry_id = id) |> 
    left_join(df_clockcatalogue, by = join_by(episode_chain)) |> select(-`dummy-1`, -slug, -genre_1, -genre_2)
  
  # merge cur/his-clocks ----
  df_clock_cz <- df_clock_cz_cur.4 |> bind_rows(df_clock_cz_his) |> arrange(slot) |> 
    group_by(slot) |> mutate(rn = row_number()) |> ungroup() |> 
    select(1:5, is_replay, rn, episode_entry_id, episode_chain, everything()) |> filter(rn == 1L) |> select(-rn) |> 
    mutate(is_replay = if_else(!is.na(episode_entry_id), FALSE, is_replay))
  
  # update locked slots ----
  df_locked <- locked_slots(pm_start = start_ts, pm_stop = stop_ts, pm_site = SITE$CONCERTZENDER, pm_db = con) |>
    mutate(slot = force_tz(slot, tzone = tz_am))
  df_clock_cz.1 <- df_clock_cz |> rows_update(df_locked, by = "slot")
  
  # > add new to database ----
  # do new ones first, to make sure that episodes replaying early, so this week, can be found
  df_new_episodes_fresh <- df_clock_cz.1 |> filter(!is_replay & is.na(episode_entry_id)) |> 
    # don't override existing slots
    anti_join(df_locked, by = join_by(slot))
  
  # prepped value for creation date the way the database stores it: with timezone UTC
  ts_now = now()
  fmt_created_at = format(ts_now, tz = "UTC", usetz = FALSE)
  
  if (nrow(df_new_episodes_fresh) > 0) {
    flog.info(str_glue("adding fresh clock items to database, using `created_at` = {fmt_created_at} UTC"), name = "clof")
    
    # insert everything with identical `created_at` timestamps, making it a set
    func_result <- clock2db(pm_clock_tib = df_new_episodes_fresh, 
                            pm_created_at = ts_now, 
                            pm_site = SITE$CONCERTZENDER, 
                            pm_db = con)
    
    # . update clock tibble rows ----
    # test: fmt_created_at <- "2026-04-24 18:42:00"
    qry <- glue_sql(
      "select cast(b.dates->>'$.start' as datetime) as slot,
       e.id as episode_entry_id
     from entries b join entries e on e.id = b.parent_id
                                  and e.deleted_at is null
                                  and e.type = 'episode'
     where b.deleted_at is null
       and b.type = 'broadcast'
       and b.site_id = 1
       and b.created_at = {fmt_created_at}
     order by 1;", .con = con)
    db_new_items <- dbGetQuery(con, qry) |> mutate(slot = force_tz(slot, tzone = tz_am))
    df_clock_cz.2 <- df_clock_cz.1 |> rows_update(db_new_items, by = "slot")
  } else {
    flog.info("no fresh clock items to add to the database", name = "clof")
    df_clock_cz.2 <- df_clock_cz.1
  }
  
  # > add replays to database ----
  # . prep episode chains ----
  df_chains <- df_clock_cz.2 |> filter(!is_replay) |> select(episode_chain, slot, episode_entry_id) |> 
    arrange(episode_chain, desc(slot))
  
  # . prep replays ----
  df_new_episodes_replay <- df_clock_cz.2 |> filter(is_replay & is.na(episode_entry_id)) |> 
    mutate(replay_offset = case_when(nipper_mogelijk != "N" ~ 11L,  # NipperStudio replays broadcasts 6 months ago
                                     uitzendtype == "live"  ~ 1L,   # live broadcasts should replay the second to last
                                     TRUE                   ~ 0L)   # semi-live broadcasts can replay the last one
    ) |> anti_join(df_locked, by = join_by(slot))
  
  # . lookup replays ----
  n <- nrow(df_new_episodes_replay)
  
  if (n > 0) {
    flog.info(str_glue("adding replay clock items to database, using `created_at` = {fmt_created_at} UTC"), 
              name = "clof")
    
    # prep vectors for tibble used later in 'update the clock'
    slot <- df_new_episodes_replay$slot
    episode_entry_id <- character(n)
  
    for (rn in seq_len(n)) {
      episode_entry_id[rn] <- lookup_replay(pm_chains = df_chains,
                                            pm_cur_chain = df_new_episodes_replay$episode_chain[rn],
                                            pm_replay_slot = df_new_episodes_replay$slot[rn],
                                            pm_offset = df_new_episodes_replay$replay_offset[rn])
      # log not finding an episode to be replayed
      if (is.na(episode_entry_id[rn]) || episode_entry_id[rn] == "BLANK") {
        v1 <- df_new_episodes_replay$titel_NL[rn]
        v2 <- df_new_episodes_replay$slot[rn]
        v3 = df_new_episodes_replay$episode_chain[rn]
        flog.info(str_glue("no replay found for {v1}, slot {v2}, chain {v3}"), name = "clof")
      } else {
        # add broadcast to the episode to be replayed
        ins_result <- cpnm_bc_ins(pm_pgm_id = df_new_episodes_replay$pgm_id[rn],
                                  pm_epi_id = episode_entry_id,
                                  pm_site_id = SITE$CONCERTZENDER,
                                  pm_bc_start = df_new_episodes_replay$slot[rn],
                                  pm_bc_minutes = df_new_episodes_replay$slot_minutes[rn],
                                  pm_created_at = ts_now,
                                  pm_cpnm_db = con)
      }
    }
    
    # update the clock
    replay_updates <- tibble(slot = slot, episode_entry_id = episode_entry_id)
    df_clock_cz.3 <- df_clock_cz.2 |> rows_update(replay_updates, by = "slot")
    
  } else {
    flog.info("no replay clock items to add to the database", name = "clof")
    df_clock_cz.3 <- df_clock_cz.2
  }
  
  # . persist clock ----
  write_rds(df_clock_cz.3, file = config$clock_home)
  
  # . check completeness ----
  fmt_start_ts_m10 = fmt_ts(start_ts - minutes(10L))
  fmt_stop_ts_m10 = fmt_ts(stop_ts - minutes(10L))
  
  qry_gaps <- glue_sql("
            with broadcasts as (
                select cast(b.dates->>'$.start' as datetime) as bc_start,
                       cast(b.dates->>'$.end'   as datetime) as bc_stop,
                       e.title_nl
                from entries b join entries e on e.id = b.parent_id
                                             and e.deleted_at is null
                                             and e.type = 'episode'
                where cast(b.dates->>'$.start' as datetime) between {fmt_start_ts_m10} and {fmt_stop_ts_m10}
                  and b.deleted_at is null
                  and b.type = 'broadcast'
                  and b.site_id = 1
            ),
            ordered as (
                select bc_start,
                       bc_stop,
                       lead(bc_start) over (order by bc_start) as next_bc_start,
                       title_nl
                from broadcasts
            )
            select bc_start,
                   bc_stop,
                   next_bc_start,
                   title_nl
            from ordered
            where next_bc_start is not null
              and bc_stop != next_bc_start
            order by bc_start;", .con = con)
  db_gaps <- dbGetQuery(con, qry_gaps)

  if (nrow(db_gaps) > 0) {
    flog.error("Detected %s gaps", nrow(db_gaps), name = "clof")
    log_tibble(x = db_gaps |> select(bc_start, title_nl, everything()), 
               label = "Gaps are AFTER each of these broadcasts", 
               n = 10, width = 180)
    break
  }
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("job finished", name = "clof")
