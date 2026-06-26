# - - - - - - - - - - - - - - - - - - - - -
# Create next Weekly Playout for CZ
# - - - - - - - - - - - - - - - - - - - - -

# init & cfg ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite)

config <- read_yaml("config.yaml")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
fmt_ymd <- stamp("1958-12-25", quiet = T, orders = "ymd")
log_slug <- "pof_cz"
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
      gws_plws_raw <- read_sheet(ss = config$gws_playlistweeks, sheet = "CONCERTZENDER")
      gws_clock_catalg_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
      czts_pres_raw <- read_sheet(ss = config$czts_pres,
                                  sheet = "presentatie",
        .name_repair = ~ c("datum", "bc_ymd", "slotElem", "slotID", "dum1", "uren", "Presentatie", "Techniek",
                            "Status", "Def", "Bijzonderheden", "Studio_1_res", "Studio_2_res", "hdn_FullDate",
                            "hulp1", "hulp2", "hulp3", "hulp4", "liveType", "hum", "dockrefs_p2m"))
    },
    error = function(e1) {
      flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
      break
    }
  )
  
  where_to_continue <- ymd(max(str_sub(gws_plws_raw$uitzendingen, 1, 10), na.rm = T), quiet = T)
  hour(where_to_continue) <- 12
  minute(where_to_continue) <- 55
  where_to_stop <- where_to_continue + days(7L)
  source("R/custom_functions.R", encoding = "UTF-8")
  source("R/cpnm_db_setup.R", encoding = "UTF-8")

  # get week from cpnm-db ----  
  db_week <- dbGetQuery(con_cpnm, statement = read_file("SQL/clockweek.sql"), 
                        params = list(where_to_continue, where_to_stop, SITE$CONCERTZENDER)
             ) |> rename(catalg_key = playlist)

  # . get catalogue ----
  catalg <- gws_clock_catalg_raw |> select(
    catalg_key = `key-modelrooster`,
    title = `titel-NL`,
    mac = `cz-playout-mac`,
    uitzendtype,
    chain = `episode-chain`,
    nipper_mogelijk
  )
  
  # fallback for missing catalogue keys
  # - NB: assuming clockfactory ran right before this, as this normally happens
  epi_chains <- read_rds("resources/episode_chains.RDS") |> distinct()
  db_week_missing <- db_week |> filter(is.na(catalg_key)) |> select(ep_id) |>
    inner_join(epi_chains, by = join_by(ep_id == episode_entry_id)) |>
    distinct() |> 
    inner_join(catalg, by = join_by(episode_chain == chain)) |>
    select(ep_id, catalg_key)
  
  if (nrow(db_week_missing) > 0) {
    db_week <- db_week |> rows_update(y = db_week_missing, by = "ep_id")
  }
  
  # . get live bc-dates ----
  bc_live <- czts_pres_raw |> 
    filter(!is.na(Def) &
             Status == "Live") |> transmute(bc_ymd = fmt_ymd(bc_ymd)) |> mutate(hh_van_live = T)
  
  # . tibble for GWS
  gws_week <- db_week |>
    left_join(catalg, by = join_by(catalg_key)) |>
    mutate(min_bc_start_ymd = str_sub(min_bc_start, end = 10)) |>
    left_join(bc_live, by = join_by(min_bc_start_ymd == bc_ymd)) |>
    filter(nipper_mogelijk == "N") |>
    mutate(
      min_bc_start = if_else(min_bc_start == bc_start, NA_character_, min_bc_start),
      slot = to_slot_key(ymd_hms(bc_start, quiet = T, tz = TZ_AM)),
      hh_van_slot = if_else(!is.na(min_bc_start), 
                            to_slot_key(ymd_hms(min_bc_start, quiet = T, tz = TZ_AM)), 
                            NA_character_),
      uitz_wk = where_to_continue |> (\(x) sprintf("%d-%02d", isoyear(x), isoweek(x)))(),
      bc_start = bc_start |> str_sub(end = -4) |> str_replace(" ", "  "),
      min_bc_start = min_bc_start |> str_sub(end = -4) |> str_replace(" ", "  "),
      duur = as.integer(duur / 60),
      hh_van_live = if_else(is.na(hh_van_live) | !hh_van_live | uitzendtype != "live", 
                            FALSE, 
                            TRUE),
      itunes_playlist = str_remove(catalg_key, " S\\d$"),
      gereed = FALSE
    ) |>
    filter(uitzendtype != "live" | !is.na(min_bc_start)) |>
    select(
      mac,
      programma = title,
      duur,
      herhaling_van = min_bc_start,
      hh_van_slot,
      hh_van_live,
      itunes_playlist,
      uitzendtype,
      gereed,
      uitz_wk,
      uitzending = bc_start,
      slot
    ) |> distinct() |> 
    mutate(audiofile = if_else(is.na(herhaling_van), uitzending, herhaling_van)) |>
    arrange(itunes_playlist, audiofile, herhaling_van) |>
    group_by(itunes_playlist) |> 
    mutate(
      collision = n_distinct(audiofile) > 1,
      n_bcs = n(),
      combi = n_bcs > 1L & !collision,
      ord = list(order(uitzending)),
      uitzendingen = if_else(combi, paste(uitzending[ord[[1]]], collapse = ", "), uitzending),
      slots = if_else(combi, paste(slot[ord[[1]]], collapse = ", "), slot)
    ) |> ungroup() |>
    mutate(playlist = if_else(!is.na(herhaling_van) & collision,
                              paste0(itunes_playlist, " (herhaling)"),
                              itunes_playlist
           ),
           type = case_when(hh_van_live ~           "HiJack",
                            !is.na(herhaling_van) ~ "herhaling",
                            combi ~                 "combi n/h",
                            TRUE ~                  "nieuw"
           ),
           sorting = case_when(type == "HiJack"                    ~ 1L,
                               programma == "Geen Dag zonder Bach" ~ 2L,
                               str_detect(programma, "Nacht")      ~ 3L,
                               TRUE                                ~ 4L
           )
    ) |>
    filter(n_bcs == 1 | collision | is.na(herhaling_van)) |> 
    select(uitz_wk,
           mac,
           programma,
           type,
           duur,
           audiofile,
           hh_van_slot,
           playlist,
           uitzendingen,
           slots,
           gereed,
           sorting
    ) |> distinct() |> 
    add_row(uitz_wk = where_to_continue |>
              (\(x) sprintf("%d-%02d", isoyear(x), isoweek(x)))(), 
            mac = "CZ",
            programma = str_sub(fmt_ts(where_to_continue), 1, 10), 
            sorting = 0L) |> 
    arrange(mac, sorting, audiofile) |> 
    mutate(banding = case_when(sorting == 0L                       ~ 7L,
                               type == "HiJack"                    ~ 6L,
                               mac == "LGM"                        ~ 1L,
                               programma == "Geen Dag zonder Bach" ~ 2L,
                               str_detect(programma, "Nacht")      ~ 3L,
                               row_number() %% 2 == 0              ~ 4L,
                               TRUE                                ~ 5L
                     )
    )
  
  append_week(ss = config$gws_playlistweeks, sheet = "CONCERTZENDER", new_rows = gws_week)
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con_cpnm)
close_tunnel(tunnel)
