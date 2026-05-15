# - - - - - - - - - - - - -
# Rebuild program clock CZ
# - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI, digest, optparse,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
log_slug <- "clof"
apf <- flog.appender(appender.file(config$log_appender_file_cz), log_slug)
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

flog.info("\n = = = = = =  Rebuilding program clock CZ = = = = = =", name = log_slug)

TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)
BUILD_TYPE <- list(PROD = 1L, REV  = 2L)
DAYS_OF_WEEK <- list(MONDAY = 1L, THURSDAY = 4L)
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")

# connect to CPNM-database ----
source("R/cpnm_db_setup.R", encoding = "UTF-8")

# Build production clock set ----
rebuild_start13 <- rebuild_start
hour(rebuild_start13) <- 13L
rebuild_start13_utc <- force_tz(rebuild_start13, tzone = "UTC")
prod_clock_set_db_raw <- dbGetQuery(conn = con, statement = read_file("resources/load-production-clockset.sql"),
                                    params = list(rebuild_start13_utc))
prod_clock_set_db <- prod_clock_set_db_raw |>
  mutate(across(where(~ inherits(.x, "POSIXct")), ~ force_tz(.x, tzone = "Europe/Amsterdam")),
         bc_name = str_replace(bc_name, "&amp;", "&"))

# > Main Control Loop ----
repeat {
  flog.info(str_glue("Start = Thursday {fmt_ts(rebuild_start13)}"), name = log_slug)
  
  # . trigger GD-auth
  with_drive_quiet(
    drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
  )
  path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
  with_drive_quiet(
    # clockcatalogue from GD ----
    drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)
  )
  
  # sheets as df ----
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
  
  # clockprofile from GD ----
  path_clockprofile_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
  with_drive_quiet(
    drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockprofile_cz, )
  )
  # . tidy clockprofile ----
  df_clockprofile_cz_raw <- cz_extract_sheet(path_clockprofile_cz, sheet_name = "cz-data") |> rename(slot_key = slot) |> 
    mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key)
    
  # validate slot/minutes columns in profile
  ccs <- check_circular_sequence(df_clockprofile_cz_raw |> select(slot_key, slot_minutes))
  if (nrow(ccs) > 0) {
    flog.error("clock profile has errors in slot/minutes columns; quiting this job.", name = log_slug)
    break
  }
  
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
    flog.error("clock catalogue is incomplete (title); quiting this job.", name = log_slug)
    break
  }
  
  missing <- df_clock_cz_cur |> filter(is.na(episode_chain)) |> nrow()
  
  if (missing > 0) {
    flog.error("clock catalogue is incomplete (episode chain); quiting this job.", name = log_slug)
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
    flog.error("genres missing in the taxonomy; quiting this job.", name = log_slug)
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
    flog.error("editors missing in the taxonomy; quiting this job.", name = log_slug)
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
    flog.error("images are incomplete; quiting this job.", name = log_slug)
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
    flog.error("programs missing in 'entries'; quiting this job.", name = log_slug)
    break
  }

  # combine rev/prod ----
  df_clock_cz_cur.5 <- df_clock_cz_cur.4 |> full_join(prod_clock_set_db, by = join_by(slot == bc_start))
  # get the diffs ----
  df_clock_cz_cur.6 <- df_clock_cz_cur.5 |> filter(titel_NL != bc_name | is.na(slot_key) | is.na(bc_name))
  
  # . delete diff broadcasts ----
  # these are bc's in prod-db no longer in revised clock
  bc_to_delete <- df_clock_cz_cur.6 |> filter(!is.na(bc_id)) |> transmute(bc_id_delete = bc_id)
  ts_now = now()
  fmt_action_at = format(ts_now, tz = "UTC", usetz = FALSE)
  
  if (nrow(bc_to_delete) > 0) {
    sql_sts <- dbExecute(con, "drop temporary table if exists bc_to_delete")
    sql_sts <- dbExecute(con, "create temporary table bc_to_delete (
                                 bc_id_delete char(36) character set utf8mb4 collate utf8mb4_unicode_ci)")
    sql_sts <- dbAppendTable(con, "bc_to_delete", bc_to_delete)
    sql_stmt <- glue_sql("update entries as e
                          join bc_to_delete as c on c.bc_id_delete = e.id
                          set e.deleted_at = {fmt_action_at}
                          where e.type = 'broadcast'
                            and e.deleted_at is null
                            and e.site_id = 1;", .con = con)
    sql_sts <- dbExecute(con, sql_stmt)
  }
  
  # . add diff ep's + broadcasts ----
  # these are the episodes and broadcasts missing in prod-db, both fresh and replay
  ep_bc_to_add <- df_clock_cz_cur.6 |> filter(!is.na(slot_key)) |> 
    select(slot:pgm_id) |> 
    mutate(episode_entry_id = NA_character_)
  
  # . get episode chains ----
  chain_env <- new.env(parent = globalenv())
  chain_env$max_ts_to_load <- rebuild_start13
  chain_env$con <- con
  source("R/load-episode-chains.R", local = chain_env)
  episode_chains <- chain_env$episode_chains
  
  # . update cpnm database ----
  upd_cpnm_env <- new.env(parent = globalenv())
  upd_cpnm_env$tib_clock <- ep_bc_to_add
  upd_cpnm_env$con <- con
  upd_cpnm_env$episode_chains <- episode_chains
  upd_cpnm_env$log_slug <- log_slug
  upd_cpnm_env$cur_site <- SITE$CONCERTZENDER
  upd_cpnm_env$TZ_AM <- TZ_AM
  source("R/update_cpnm.R", local = upd_cpnm_env)
  result <- upd_cpnm_env$result
  n_new_episodes_fresh  = result$n_new_episodes_fresh
  n_new_episodes_replay = result$n_new_episodes_replay
  tib_clock_upd <- result$tib_clock_upd
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("job finished", name = log_slug)
