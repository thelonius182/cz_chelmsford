# - - - - - - - - - - - - - - - - - - - - -
# (Re-)build program clock for CZ or WJ
# - - - - - - - - - - - - - - - - - - - - -

# init & cfg ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, optparse,
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite
               # openxlsx, readxl, 
)

config <- read_yaml("config.yaml")
TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)
DFT_IMG <- list(CZ = "d5c53946-16e5-11f1-94a8-3631520831df", WJ = "d7d25b88-16e5-11f1-94a8-3631520831df")
BUILD_TYPE <- list(EXTEND = 1L, REVISE  = 2L)
PROD_TYPE <- list(LACIE = "a", SEMI_LIVE = "s")
R_DAYS_OF_WEEK <- list(MONDAY = 1L, THURSDAY = 4L) # R and SQL have different conventions
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
log_slug <- "clof"
apf <- flog.appender(appender.file(config$log_appender_file), log_slug)
flog.info("\n= = = = = = broadcast clockfactory = = = = = =", name = log_slug)

source("R/custom_functions.R", encoding = "UTF-8")
source("R/cpnm_db_setup.R", encoding = "UTF-8")

# input site & build_date ----
# via RStudio both EXTEND and REVISE are possible: input site & date. Via Powershell only EXTEND will run: input just the site
if (interactive() && requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  
  # prompt: build for which site?
  site_id_chr <- rstudioapi::showPrompt(title = "Select site", message = "1: Concertzender, 2: World of Jazz")
  site_id <- as.integer(site_id_chr)
  
  # prompt: where to start (re-)building for that site?
  dft_build_from_UTC <- next_bc_week_start(pm_site_id = site_id, pm_cpnm_db = con)
  dft_build_from_NL <- force_tz(dft_build_from_UTC$value, tzone = TZ_AM)  
  build_from_chr <- rstudioapi::showPrompt(title = "Build from date/time", 
                                           message = "shown: start of next new week", 
                                           default = fmt_ts(dft_build_from_NL))
  build_from_NL <- ymd_hms(build_from_chr, quiet = T, tz = TZ_AM)
  
  if (is.na(build_from_NL)) {
    dbDisconnect(con)
    close_tunnel(tunnel)
    cat(as.character(build_from_NL))
    stop("Invalid date. Expect 'ymd hms'-format", call. = FALSE)
  }
  
  cur_build_type <- if (dft_build_from_NL == build_from_NL) BUILD_TYPE$EXTEND else BUILD_TYPE$REVISE
} else {
  # get cmd-line input
  srvjob_args <- parse_args()
  site_id <- as.integer(srvjob_args$site_id)
  build_from_UTC <- next_bc_week_start(pm_site_id = site_id, pm_cpnm_db = con)
  build_from_NL <- force_tz(build_from_UTC$value, tzone = TZ_AM)  
  cur_build_type <- BUILD_TYPE$EXTEND
}

# Build production clock set ----
prod_clock_set_db_raw <- dbGetQuery(conn = con, statement = read_file("SQL/load-production-clockset.sql"),
                                    params = list(fmt_ts(build_from_NL), site_id))
prod_clock_set_db <- prod_clock_set_db_raw |>
  mutate(across(where(~ inherits(.x, "POSIXct")), ~ force_tz(.x, tzone = TZ_AM)),
         bc_name = str_replace(bc_name, "&amp;", "&"))

# > Main Control Loop ----
# read this as DO {...} WHILE(FALSE)
repeat {
  flog_b <- names(BUILD_TYPE)[BUILD_TYPE == cur_build_type]
  flog_s <- names(SITE)[SITE == site_id]
  flog.info(str_glue("Building from Thursday {fmt_ts(build_from_NL)}, type {flog_b} for site {flog_s}"), name = log_slug)
  
  # load GD-sheets ----
  tryCatch(
    {
      # . trigger GD-auth
      options(gargle_oauth_cache = ".secrets-salsa")
      gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
      df_clockcatalogue_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
      df_clockprofile_raw <- read_sheet(ss = config$gws_clock_profiles, sheet = names(SITE)[SITE == site_id])
    },
    error = function(e1) {
      flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
      break
    })
  
  # . tidy catalogue ----
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
                                                       episode_chain = `episode-chain`) 
  
  # . tidy profile ----
  df_clockprofile <- df_clockprofile_raw |>
    rename(slot_key = slot) |> 
    mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key) |> 
    filter(!is.na(slot_minutes))
    
  # validate slot/minutes columns in clockprofile
  ccs <- check_circular_sequence(df_clockprofile |> select(slot_key, slot_minutes))
  
  if (nrow(ccs) > 0) {
    flog.error("slots/minutes in GD-clock profile are misaligned/non-circular; quiting this job.", name = log_slug)
    break
  }
  
  mk_weekly <- df_clockprofile |> filter(!is.na(wekelijks)) |> select(1:6) |> 
    rename(catalg_key = wekelijks, prod_type = te)
  mk_biweekly<- df_clockprofile |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
    rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
  mk_week.1 <- df_clockprofile |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
    mutate(block = 1L)
  mk_week.2 <- df_clockprofile |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
    mutate(block = 2L)
  mk_week.3 <- df_clockprofile |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
    mutate(block = 3L)
  mk_week.4 <- df_clockprofile |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
    mutate(block = 4L)
  mk_week.5 <- df_clockprofile |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
    mutate(block = 5L)

  # . repair 'Thema' ----
  # repair 'Thema' on CZ only, assume it's in slots 'wo20' and 'wo21' of weeks 2 and 4
  if (site_id == SITE$CONCERTZENDER) {
    mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
    mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
  }

  # . repair 'Rec/Play' ----
  # ... on WJ only, assume it's in slots 'do20' and 'do21' of weeks 2 and 4
  #                            and slots 'wo18' and 'wo19' of weeks 1 and 3
  if (site_id == SITE$WORLD_OF_JAZZ) {
    mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
    mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
    mk_week.1 <- mk_week.1 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
    mk_week.3 <- mk_week.3 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
  }
  
  # build a clock ----
  bc_week_to <- if (nrow(prod_clock_set_db) > 0) max(prod_clock_set_db$bc_start) else build_from_NL + days(7L) - minutes(10L)
  
  bc_week_ts <- tibble(ts = seq(from = build_from_NL, to = bc_week_to, by = "hour"))
  
  df_calendar <- add_bc_cols(bc_week_ts, ts) |> 
    mutate(cycle = if_else(bc_day_label == "do" & bc_hour_start == bc_week_start_hour(site_id), 
                           bc_week_label(pm_start_of_bc_week = ts, pm_site_id = site_id), 
                           NA_character_)) |> 
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
    mutate(src = if_else(prod_type == "h", "H", src),
           prod_type = if_else(prod_type == "h", "u", prod_type)) |> 
    mutate(is_replay = if_else(is.na(src), FALSE, TRUE)) |> 
    select(-src, -mac, -`dummy-1`, -slug) |> 
    pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  # validate catalogue ----
  df_missing <- df_clock_cz_cur |> filter(is.na(titel_NL))
  
  if (nrow(df_missing) > 0) {
    flog.error("clock catalogue is incomplete (title); quiting this job.", name = log_slug)
    break
  }
  
  df_missing <- df_clock_cz_cur |> filter(is.na(episode_chain))
  
  if (nrow(df_missing) > 0) {
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
  df_missing <- df_clock_cz_cur.1 |> filter(is.na(ty_genre_id))
  
  if (nrow(df_missing) > 0) {
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
  df_missing <- df_clock_cz_cur.2 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct()
  
  if (nrow(df_missing) > 0) {
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
  df_missing <- df_afb |> filter(is.na(image_id))
  
  if (nrow(df_missing) > 0) {
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
  df_missing <- df_clock_cz_cur.4 |> filter(is.na(pgm_id))
  
  # . check complete ----
  if (nrow(df_missing) > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = log_slug)
    break
  }

  # combine rev/prod ----
  df_clock_cz_cur.5 <- df_clock_cz_cur.4 |> 
    full_join(prod_clock_set_db, by = join_by(slot == bc_start))
  
  # get the diffs ----
  df_clock_cz_cur.6 <- df_clock_cz_cur.5 |> 
    filter(titel_NL != bc_name | is.na(slot_key) | is.na(bc_name)) |> 
    # leave episodes untouched that already have editor content in the database
    filter(is.na(ep_has_content) | ep_has_content == "N")
  
  # . no diffs ----
  if (nrow(df_clock_cz_cur.6) == 0) {
    flog.error(str_glue("Nothing found to {flog_bt}; quiting this job."), name = log_slug)
    break
  }
  
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
  
  # . add diff ep's + bc's ----
  # these are the episodes and broadcasts missing in prod-db, both fresh and replay
  ep_bc_to_add <- df_clock_cz_cur.6 |> filter(!is.na(slot_key)) |> 
    select(slot:pgm_id) |> 
    mutate(episode_entry_id = NA_character_)
  
  # . get episode chains ----
  chain_env <- new.env(parent = globalenv())
  chain_env$con <- con
  source("R/load-episode-chains.R", local = chain_env)
  episode_chains <- chain_env$episode_chains

  if (site_id == SITE$WORLD_OF_JAZZ) {
    
    # . check LaCie drive ----
    lacie_root <- config$lacie_root
    
    if (!dir_exists(lacie_root)) {
      flog.error(str_glue("{lacie_root} is not available; quiting this job."), name = log_slug)
      break
    }
    
    # . get LaCie chains ----
    source("R/LaCie_tools.R", encoding = "UTF-8")
    ep_lacies <- lacie_episodes(con_mysql = con)
    con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
    fs_lacies <- scan_fs(lacie_root) |> 
      mutate(bc_start_chr = separate_dt(fn)) |> 
      inner_join(ep_lacies, by = join_by(chain, bc_start_chr)) |> 
      # some broadcasts have 2 files: .wav and .aiff; exclude .wav
      group_by(episode_id) |> 
      mutate(rn = row_number()) |> 
      ungroup() |> 
      filter(rn == 1)
    sdb <- sync_db(con = con_sqlite, fs = fs_lacies)
    db_lacies <- dbGetQuery(con_sqlite, "select * from lacie_stack order by chain, pos;")
  }
  
  # . update cpnm database ----
  upd_cpnm_env <- new.env(parent = globalenv())
  upd_cpnm_env$tib_clock <- ep_bc_to_add
  upd_cpnm_env$con <- con
  upd_cpnm_env$episode_chains <- episode_chains
  upd_cpnm_env$log_slug <- log_slug
  upd_cpnm_env$cur_site <- site_id
  upd_cpnm_env$TZ_AM <- TZ_AM
  upd_cpnm_env$lacie_chains <- db_lacies
  source("R/update_cpnm.R", local = upd_cpnm_env)
  result <- upd_cpnm_env$result
  n_new_episodes_fresh <- result$n_new_episodes_fresh
  n_new_episodes_replay <- result$n_new_episodes_replay
  tib_clock_upd <- result$tib_clock_upd
  db_lacies_upd <- result$lacie_chains_upd
  
  # store modified LaCie chains ----
  dbe <- dbExecute(con_sqlite, "delete from lacie_stack")
  dba <- dbAppendTable(con_sqlite, name = "lacie_stack", value = db_lacies_upd)
  
  # . look for gaps/overlaps ----
  week_seq_err <- dbGetQuery(conn = con, statement = read_file("SQL/check_for_gaps.sql"), 
                             # params = list(fmt_ts(begin_ts), fmt_ts(end_ts))) |> 
                             params = list(fmt_ts(build_from_NL - minutes(10L)),
                                           site_id)) |> 
    mutate(across(where(~ inherits(.x, "POSIXct")), ~ force_tz(.x, tzone = TZ_AM))) |> 
    add_bc_cols(ts_col = bc_stop) |> 
    mutate(issue = if_else(next_bc_start > bc_stop, "GAP", "OVERLAP"),
           issue = str_glue("{issue} at {fmt_ts(bc_stop)}"),
           issue = paste0(issue, " (", slot_key, "-", bc_week_of_month, ")"))
  
  if (nrow(week_seq_err) > 0) {
    flog.error("Gaps/overlaps found in database.", name = log_slug)
    log_tibble(week_seq_err |> select(issue))
    flog.info("Run a revision job after fixing gaps/overlaps.", name = log_slug)
    break
  }
  
  # store an extension as .RDS too ----
  if (cur_build_type == BUILD_TYPE$EXTEND) {
    qfn_clock_rds <- paste0(config$clock_home_rds, flog_s, "-", stamp("19581225", quiet = T)(build_from_NL), ".RDS")
    write_rds(tib_clock_upd, qfn_clock_rds)
    flog.info(str_glue("Extended week is also stored as {qfn_clock_rds}."), name = log_slug)
  }
  
  # Exit from MCL
  break
}

flog.info("job finished", name = log_slug)

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
dbDisconnect(con_sqlite)
