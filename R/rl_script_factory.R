pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite)

config <- read_yaml("config.yaml")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
fmt_ymd <- stamp("1958-12-25", quiet = T, orders = "ymd")
log_slug <- "pof_cz"
flap <- flog.appender(appender.file(config$log_appender_file), log_slug)
TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)

rls_date_fmt <- function(x, locale = "en_GB") {
  str_c(day(x), format(x, "%b %Y", locale = locale), sep = " ")
}

rls_dagletters <- function(sched_name) {
  # sched_name <- "2018-12-31_wo16-2_420_de-nacht-klassiek"
  dag_kort <- str_sub(sched_name, 12, 13)
  
  case_when(
    dag_kort == "ma" ~ "_M_____",
    dag_kort == "di" ~ "__T____",
    dag_kort == "wo" ~ "___W___",
    dag_kort == "do" ~ "____T__",
    dag_kort == "vr" ~ "_____F_",
    dag_kort == "za" ~ "______S",
    TRUE             ~ "S______"
  )
}

rls_lengte <- function(sched_name) {
  # sched_name <- "2018-12-31_wo16-2_420_de-nacht-klassiek"
  str_sub(sched_name, 19, 21)
}

rls_30m_blokken <- function(sched_name) {
  # sched_name <- "2018-12-31_wo16-2_420_de-nacht-klassiek"
  b1 <- as.integer(str_sub(sched_name, 14, 15))
  
  as.character(2 * b1)
}

rls_venster <- function(sched_name) {
  # sched_name <- "2018-12-31_wo00-2_420_de-nacht-klassiek"
  venster_datum_start <- ymd(str_sub(sched_name, 1, 10), quiet = T)
  venster_datum_stop <- venster_datum_start + days(1L)
  
  c(rls_date_fmt(venster_datum_start), rls_date_fmt(venster_datum_stop))
}

build_rl_scripts <- function(rls_week) {
  schedules_dir <- path(home_prop("home_radiologik"), "Schedule")
  start_number <- dir_ls(schedules_dir) |> length()
  
  rls_week |>
    mutate(script_nr = start_number + row_number()) |>
    group_split(row_number()) |>
    walk(build_rls_row)
}

build_script_lines <- function(rls_dagletters,
                               rl_sched_name,
                               rls_lengte,
                               rls_30m_blokken,
                               v_limiet,
                               color_rgb,
                               pl_name) {
  script_lines <- c(
    "Radiologik Schedule Segment",
    rls_dagletters,
    rls_lengte,
    "standaard",
    "ProgramTo=0",
    rls_30m_blokken,
    "0",
    "",
    paste(v_limiet[1], "0", sep = "\t"),
    paste(v_limiet[2], "0", sep = "\t"),
    "ProgramCopyPath=nopath",
    color_rgb,
    "0",
    "\tFalse",
    "Display=True",
    "PlayRotatedinMusic=False",
    "Notes=",
    paste("PrePostAppleScripts=", "", sep = "\t"),
    "AlbumSeparation=0",
    "SilenceSensorProfile=",
    "Enabled=True",
    "RotationType=0",
    "LastLog=",
    "Begin Script",
    paste(
      "play",
      "00:00",
      "",
      "",
      "",
      pl_name,
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      sep = "\t"
    )
  )
}

build_rls_row <- function(wk_row) {
  # test: wk_row <- cz_week[1, ]
  
  parts <- wk_row$rl_sched_name |>
    str_match("^(\\d{4}-\\d{2}-\\d{2})_([a-z]{2}\\d{2}-\\d)_(\\d{3})_(.+)$")
  date_iso <- parts[, 2]
  slot     <- parts[, 3]
  duration <- as.integer(parts[, 4])
  
  script_lines <- build_script_lines(
    rls_dagletters  = rls_dagletters_from_slot(slot),
    rl_sched_name   = wk_row$rl_sched_name,
    rls_lengte      = duration,
    rls_30m_blokken = rls_30m_blokken_from_slot(slot),
    v_limiet        = ls_venster_from_date_duration(date_iso, duration),
    color_rgb       = paste0("ColorLabel=", wk_row$sched_rgb),
    pl_name         = wk_row$playlist,
    duration        = duration
  )
  
  schedules_dir <- path(home_prop("home_radiologik"), "Schedule")
  
  next_number <- dir_ls(schedules_dir) |>
    length() |>
    {\(x) x + 1L}()
  
  script_file_name <- path(
    schedules_dir,
    sprintf("%03d - %s", next_number, rl_sched_name)
  )
  
  write_lines(
    x = script_lines,
    file = script_file_name
  )
  
  invisible(script_file_name)
}

# > MAIN < ----
# 1. load GD-sheet ----
tryCatch({
  # . trigger GD-auth
  options(gargle_oauth_cache = ".secrets-salsa")
  gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
  gws_pl_raw <- read_sheet(ss = config$gws_playlistweeks, sheet = "CONCERTZENDER")
}, error = function(e1) {
  flog.error("Load error GWS-playlist: %s", conditionMessage(e1), name = log_slug)
  stop("Load error GWS-playlist")
})

# 2. slice latest week ----
week_id <- gws_pl_raw |> filter(mac == "CZ") |>
  mutate(latest_week = max(uitz_wk)) |> 
  select(latest_week) |> distinct()

# 3. prep colour banding ----
colbands <- tribble(
  ~Group,   ~Row_type, ~RGB,
  "A",      "base",    "255,255,255",
  "A",      "band ",   "247,247,249",
  "B",      "base",    "250,250,252",
  "B",      "band ",   "241,241,244"
)

# 4. build sched details ----
cz_week <- gws_pl_raw |>
  inner_join(week_id, by = join_by(uitz_wk == latest_week)) |>
  filter(mac != "CZ") |>
  select(mac:duur, playlist:slots) |>
  mutate(rl_pgm_name = as_slug(programma)) |>
  separate_longer_delim(cols = c(uitzendingen, slots), delim = ",") |>
  mutate(
    date_slot = str_c(
      str_extract(str_squish(uitzendingen), "^\\d{4}-\\d{2}-\\d{2}"),
      "_",
      str_squish(slots)
    ),
    rl_sched_name = paste(
      date_slot,
      str_pad(
        as.character(duur),
        width = 3L,
        side = "left",
        pad = "0"
      ),
      rl_pgm_name,
      sep = "_"
    )
  ) |>
  arrange(mac, rl_sched_name) |>
  mutate(rgb_id = 1L + row_number() %% 2L,
         sched_rgb = colbands$RGB[rgb_id]) |>
  select(-uitzendingen,
         -slots,
         -rl_pgm_name,
         -date_slot,
         -programma,
         -type,
         -rgb_id)

# 5. compile the schedules ----
build_rl_scripts(rls_week = cz_week)
