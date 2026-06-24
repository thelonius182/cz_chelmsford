# - - - - - - - - - - - - - - - - - - - - -
# Create next Weekly Playout for WoJ
# - - - - - - - - - - - - - - - - - - - - -

# init & cfg ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite)

config <- read_yaml("config.yaml")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
log_slug <- "clof"
flap <- flog.appender(appender.file(config$log_appender_file), log_slug)
TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)

# > Main Control Loop ----
# read this as DO {...} WHILE(FALSE)
repeat {
  
  # load GD-sheet ----
  tryCatch(
    {
      # . trigger GD-auth
      options(gargle_oauth_cache = ".secrets-salsa")
      gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
      gws_plws_raw <- read_sheet(ss = config$gws_playlistweeks, sheet = "WORLD_OF_JAZZ")
      gws_clock_catalg_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
    },
    error = function(e1) {
      flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
      break
    })
  
  where_to_continue <- ymd_hm(max(gws_plws_raw$uitzending, na.rm = T), quiet = T)
  hour(where_to_continue) <- 12
  minute(where_to_continue) <- 55
  where_to_stop <- where_to_continue + days(7L)
  source("R/custom_functions.R", encoding = "UTF-8")
  source("R/cpnm_db_setup.R", encoding = "UTF-8")

  # get week from cpnm-db ----  
  db_week <- dbGetQuery(con_cpnm, statement = read_file("SQL/clockweek.sql"), 
                        params = list(where_to_continue, where_to_stop, SITE$WORLD_OF_JAZZ)
             ) |> rename(catalg_key = playlist)
  
  # . get catalogue ----
  catalg <- gws_clock_catalg_raw |> select(
    catalg_key = `key-modelrooster`,
    title = `titel-NL`,
    server = `cz-playout-mac`,
    uitzendtype,
    chain = `episode-chain`
  )
  
  catalg_woj <- catalg |> filter(str_detect(uitzendtype, "_woj") &
                                   chain != "#NONE#")
  
  catalg_lacie <- catalg |> filter(str_detect(uitzendtype, "lacie") &
                                   chain != "#NONE#")
  
  # . check LaCie drive ----
  lacie_root <- config$lacie_root
  
  if (!dir_exists(lacie_root)) {
    flog.error(str_glue("{lacie_root} is not available; quiting this job."), name = log_slug)
    break
  }
  
  # . get LaCie chains ----
  source("R/LaCie_tools.R", encoding = "UTF-8")
  ep_lacies <- lacie_episodes(con_mysql = con_cpnm)
  con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
  fs_lacies <- scan_fs(lacie_root) |> 
    mutate(bc_start_chr = separate_dt(fn)) |> 
    inner_join(ep_lacies, by = join_by(chain, bc_start_chr)) |> 
    # some broadcasts have 2 files: .wav and .aiff; exclude .wav
    group_by(episode_id) |> 
    mutate(rn = row_number()) |> 
    ungroup() |> 
    filter(rn == 1)
  sdb <- sync_db(con_sqlite = con_sqlite, fs = fs_lacies)
  db_lacies <- dbGetQuery(con_sqlite, "select * from lacie_stack order by chain, pos;")
  
  # fallback for missing catalogue keys
  # - NB: assuming clockfactory ran right before this, as this normally happens
  epi_chains <- read_rds("resources/episode_chains.RDS")
  db_week_missing <- db_week |> filter(is.na(catalg_key)) |> select(ep_id) |>
    inner_join(epi_chains, by = join_by(ep_id == episode_entry_id)) |>
    distinct() |> 
    inner_join(catalg_lacie, by = join_by(episode_chain == chain)) |>
    select(ep_id, catalg_key)
  
  if (nrow(db_week_missing) > 0) {
    db_week <- db_week |> rows_update(y = db_week_missing, by = "ep_id")
  }
  
  gws_week_lac <- db_week |> left_join(db_lacies, by = join_by(ep_id))
  
  # . tibble for GWS
  gws_week <- gws_week_lac |> 
    left_join(catalg, by = join_by(catalg_key)) |> 
    filter(is.na(uitzendtype) | uitzendtype != "non-stop") |> 
    mutate(min_bc_start = if_else(min_bc_start == bc_start, NA_character_, min_bc_start),
           source = case_when(!is.na(fn) ~ "LaCie", 
                              ep_title %in% catalg_woj$title & !is.na(min_bc_start) ~ "WoJ-pc replay",
                              ep_title %in% catalg_woj$title ~ "WoJ-pc new",
                              TRUE ~ "Uitzendmac"),
           slot = to_slot_key(ymd_hms(bc_start, quiet = T, tz = TZ_AM)),
           hh_slot = if_else(source %in% c("Uitzendmac", "WoJ-pc replay"), 
                             to_slot_key(ymd_hms(min_bc_start, quiet = T, tz = TZ_AM)),
                             NA_character_),
           uitz_wk = where_to_continue |>
             (\(x) sprintf("%d-%02d", isoyear(x), isoweek(x)))(),
           bc_start = bc_start |> str_sub(end = -4) |> str_replace(" ", "  "),
           min_bc_start = min_bc_start |> str_sub(end = -4) |> str_replace(" ", "  "),
           gereed = FALSE,
           banding = 0L
    ) |> 
    select(uitz_wk, 
           uitzending = bc_start, 
           slot,
           programma = ep_title, 
           herhaling_van = min_bc_start, 
           hh_slot,
           source,
           gereed,
           banding
    ) |> 
    distinct() |> 
    add_row(uitz_wk = where_to_continue |>
              (\(x) sprintf("%d-%02d", isoyear(x), isoweek(x)))(), 
            programma = str_sub(fmt_ts(where_to_continue), 1, 10),
            source = "A",
            banding = 1L
    ) |> 
    arrange(source, programma)
  
  append_week(ss = config$gws_playlistweeks, sheet = "WORLD_OF_JAZZ", new_rows = gws_week)
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con_cpnm)
close_tunnel(tunnel)
dbDisconnect(con_sqlite)
