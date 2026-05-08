# - - - - - - - - - - - - -
# Rebuild program clock CZ
# - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI, digest, optparse,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
apf <- flog.appender(appender.file(config$log_appender_file_cz), "clof")
source("R/custom_functions.R", encoding = "UTF-8")

# Where to start rebuilding?
rebuild_start_chr <-
  if (interactive()) {
    ask_rebuild_date()
  } else {
    opts <- parse_rebuild_options()
    opts$date
  }

rebuild_start <- as.Date(rebuild_start_chr)

if (is.na(rebuild_start)) {
  stop("Invalid date. Please use yyyy-mm-dd.", call. = FALSE)
}

rebuild_start <- ymd(rebuild_start_chr, tz = "Europe/Amsterdam", quiet = T) 

if (is.na(rebuild_start)) {
  cat(rebuild_start_chr)
  stop("invalid date")
}

flog.info("\n = = = = = =  Rebuilding program clock CZ = = = = = =", name = "clof")

TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)
BUILD_TYPE <- list(PROD = 1L, REV  = 2L)
DAYS_OF_WEEK <- list(MONDAY = 1L, THURSDAY = 4L)
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
cur_build_type <- BUILD_TYPE$REV

# Build clock set FS----
# clock_set_fs <- dir_ls(path = path_dir(config$clock_home_cz), type = "file", regexp = "_cz_") |> 
#   tibble(file = _) |>
#   mutate(file_date = str_extract(path_file(file), "\\d{8}(?=[.]RDS$)") |> ymd(tz = TZ_AM),
#          file_start = file_date + hours(13L), file_end = file_start + days(7L)) |>
#   filter(file_end > rebuild_start) |> arrange(file_start) |> pull(file) |>
#   map(\(file) {
#     readRDS(file) |> as_tibble() |> mutate(source_file = path_file(file))
#   }) |> bind_rows()

# connect to CPNM-database ----
source("R/cpnm_db_setup.R", encoding = "UTF-8")  

# Build production clock set ----
rebuild_start13 <- rebuild_start
hour(rebuild_start13) <- 13L
rebuild_start13_utc <- force_tz(rebuild_start13, tzone = "UTC")
prod_clock_set_db_raw <- dbGetQuery(conn = con, statement = read_file("resources/load-production-clockset.sql"), 
                                    params = list(rebuild_start13_utc))
prod_clock_set_db <- prod_clock_set_db_raw |> 
  mutate(across(where(~ inherits(.x, "POSIXct")), ~ force_tz(.x, tzone = "Europe/Amsterdam")))

# clock_set_compare <- clock_set_db |> left_join(clock_set_fs, by = join_by(bc_start == slot)) 
# clock_diffs <- clock_set_compare |> filter(ep_id != episode_entry_id | is.na(slot_key))

# create revision helper tables ----
sql_sts <- dbExecute(conn = con, statement = "drop table clock_revision_entries")
sql_sts <- dbExecute(conn = con, statement = read_file(file = "resources/ddl_clock_revision_entries.sql"))
sql_sts <- dbExecute(conn = con, statement = "drop table clock_revision_taxonomables")
sql_sts <- dbExecute(conn = con, statement = read_file(file = "resources/ddl_clock_revision_taxonomables.sql"))

# Pre-populate revision clock. Pre-pop is required because replays can originate from episodes before rebuild_start,
# and not having an active episode in the revision clock will block adding a broadcast (FK-constraint).
# The oldest replays required are NipperStudio replays (dating back 185 days). So no need to load more than the last 200 days.
sql_sts <- dbExecute(conn = con, statement = read_file("resources/backfill_clock_revision_entries_ep.sql"), 
                                    params = list(rebuild_start13_utc - days(200), rebuild_start13_utc))
sql_sts <- dbExecute(conn = con, statement = read_file("resources/backfill_clock_revision_entries_bc.sql"), 
                                    params = list(rebuild_start13_utc - days(200), rebuild_start13_utc))

# > Main Control Loop ----
repeat {
  flog.info(str_glue("Start = Thursday {rebuild_start_fmt}"), name = "clof")
  
  # . trigger GD-auth
  path_clockprofile_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
  path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
  with_drive_quiet(
    drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
  )
  with_drive_quiet(
    # clockprofile from GD ----
    drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockprofile_cz, )
  )
  with_drive_quiet(
    # clockcatalogue from GD ----
    drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)
  )
  
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
  bc_week_ts <- tibble(ts = seq(from = rebuild_start13, to = max(prod_clock_set_db$bc_start), by = "hour"))
  
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
  
  df_clock_cz_cur.5 <- df_clock_cz_cur.4 |> mutate(episode_entry_id = NA_character_)

  # > add new to database ----
  # do new ones first, to make sure that episodes replaying early, so this week, can be found
  df_new_episodes_fresh <- df_clock_cz_cur.5 |> filter(!is_replay & is.na(episode_entry_id))

  # prepped value for creation date the way the database stores it: with timezone UTC
  ts_now = now()
  fmt_created_at = format(ts_now, tz = "UTC", usetz = FALSE)
  
  if (nrow(df_new_episodes_fresh) > 0) {
    flog.info(str_glue("adding fresh clock items to clock_revision_entries, using `created_at` = {fmt_created_at} UTC"), name = "clof")
    
    # insert everything with identical `created_at` timestamps, making it a set
    func_result <- clock2db(pm_clock_tib = df_new_episodes_fresh, 
                            pm_created_at = ts_now, 
                            pm_site = SITE$CONCERTZENDER, 
                            pm_build_type = BUILD_TYPE$REV,
                            pm_db = con)
    
    # . update clock tibble rows ----
    qry <- glue_sql(
      "select cast(b.dates->>'$.start' as datetime) as slot,
       e.id as episode_entry_id
     from clock_revision_entries b join clock_revision_entries e on e.id = b.parent_id
                                  and e.deleted_at is null
                                  and e.type = 'episode'
     where b.deleted_at is null
       and b.type = 'broadcast'
       and b.site_id = 1
       and b.created_at = {fmt_created_at}
     order by 1;", .con = con)
    db_new_items <- dbGetQuery(con, qry) |> mutate(slot = force_tz(slot, tzone = TZ_AM))
    df_clock_cz_cur.6 <- df_clock_cz_cur.5 |> rows_update(db_new_items, by = "slot")
  } else {
    flog.error("no fresh revision_clock items found, quiting this job", name = "clof")
    break
  }
  
  # > add replays to database ----
  # . - load episode chains ----
  his_stop_ts <- rebuild_start13
  source("R/load-episode-chains.R", encoding = "UTF-8")
  
  df_chains <- df_clock_cz_cur.6 |> bind_rows(df_clock_cz_his) |> 
    filter(!is_replay) |> select(episode_chain, slot, episode_entry_id) |> 
    arrange(episode_chain, desc(slot)) |> distinct()
  
  # . prep replays ----
  df_new_episodes_replay <- df_clock_cz_cur.6 |> select(-genre, -ty_genre_id) |> distinct() |> 
    filter(is_replay & is.na(episode_entry_id))
  
  # . lookup replays ----
  n <- nrow(df_new_episodes_replay)
  
  if (n > 0) {
    flog.info("adding revision_replays, same `created_at`", name = "clof")
    
    # prep vectors for tibble used later in 'update the clock'
    slot <- df_new_episodes_replay$slot
    episode_entry_id <- character(n)
    
    for (rn in seq_len(n)) {
      episode_entry_id[rn] <- lookup_replay(pm_chains = df_chains,
                                            pm_cur_chain = df_new_episodes_replay$episode_chain[rn],
                                            pm_replay_target_slot = df_new_episodes_replay$slot[rn],
                                            pm_bc_type = df_new_episodes_replay$uitzendtype[rn],
                                            pm_nipperstudio = df_new_episodes_replay$nipper_mogelijk[rn],
                                            pm_start_of_week = start_of_week(df_new_episodes_replay$slot[rn]))
      # log as "not found"
      if (episode_entry_id[rn] == "NOT-FOUND") {
        v1 <- df_new_episodes_replay$titel_NL[rn]
        v2 <- df_new_episodes_replay$slot[rn]
        v3 = df_new_episodes_replay$episode_chain[rn]
        flog.error(str_glue("no replay found for {v1}, target slot {v2}, of chain {v3}"), name = "clof")
      } else {
        # add broadcast to the episode to be replayed
        ins_result <- cpnm_bc_ins(pm_pgm_id = df_new_episodes_replay$pgm_id[rn],
                                  pm_epi_id = episode_entry_id[rn],
                                  pm_site_id = SITE$CONCERTZENDER,
                                  pm_bc_start = df_new_episodes_replay$slot[rn],
                                  pm_bc_minutes = df_new_episodes_replay$slot_minutes[rn],
                                  pm_created_at = ts_now,
                                  pm_build_type = BUILD_TYPE$REV,
                                  pm_cpnm_db = con)
      }
    }
    
    # . update the clock ----
    replay_updates <- tibble(slot = slot, episode_entry_id = episode_entry_id) |>
      left_join(df_chains, by = join_by(episode_entry_id)) |>
      select(slot = slot.x, episode_entry_id, replay_source_slot = slot.y)
    
    dummy_replay_slot <- ymd_hms("1958-12-25 13:00:00", tz = TZ_AM, quiet = T)
    df_clock_cz_cur.7 <- df_clock_cz_cur.6 |>
      mutate(replay_source_slot = if_else(is_replay, dummy_replay_slot, NA_POSIXct_)) |>
      rows_update(replay_updates, by = "slot") |>
      select(1:6, replay_source_slot, everything())
    
  } else {
    flog.error("no replays to add, quiting this job", name = "clof")
    break
  }
  
  # find the diffs ----
  clock_diffs <- df_clock_cz_cur.7 |> left_join(prod_clock_set_db, by = join_by(slot == bc_start)) |> 
    select(slot, 
           prod_title = bc_name, prod_ep_has_content = ep_has_content, prod_replay_bc_id = bc_rp_source_id, prod_ep_id = ep_id,
           rev_title = titel_NL, rev_is_replay = is_replay, rev_ep_id = episode_entry_id) |> 
    filter(prod_title != rev_title) 
  
  # update replays in db ----
  clock_diffs_update <- clock_diffs |>
    filter(!is.na(prod_replay_bc_id)) |>
    transmute(
      entry_id = prod_replay_bc_id,
      new_parent_id = rev_ep_id
    )
  
  dbWriteTable(con, name = "clock_diffs_update", value = clock_diffs_update, temporary = TRUE, overwrite = TRUE)
  
  dbExecute(con, "UPDATE entries AS e
                  JOIN clock_diffs_update AS c
                    ON c.entry_id = e.id
                  SET e.parent_id = c.new_parent_id")
  
  dbExecute(con, "DROP TEMPORARY TABLE IF EXISTS clock_diffs_update")
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("job finished", name = "clof")
