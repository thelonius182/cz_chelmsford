pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite)

as_slug <- function(pm_str) {
  s1 <- str_replace_all(pm_str, "[^- [:word:]]", "")
  s1 <- str_trim(string = s1, side = "both")
  s1 <- str_replace_all(s1, " +", "-")
  s1 <- str_replace_all(s1, "-+", "-") |> str_to_lower()
  stringi::stri_trans_general(s1, "Latin-ASCII")
}

wdw_date_fmt <- function(x, locale = "en_GB") {
  str_c(day(x), format(x, "%b %Y", locale = locale), sep = " ")
}

day_mask <- function(slot) {
  # slot <- "wo16-2"
  nl_day <- str_sub(slot, 1, 2)
  
  case_match(
    nl_day,
    "zo" ~ "S______",
    "ma" ~ "_M_____",
    "di" ~ "__T____",
    "wo" ~ "___W___",
    "do" ~ "____T__",
    "vr" ~ "_____F_",
    "za" ~ "______S"
  )
}

# bc_start as offset from midnight in 30 minute blocks
# test: slot <- "wo16-2". SOLL = "32"
slot_start_halfhour_index  <- function(slot) {
  x <- as.integer(str_sub(slot, 3, 4))
  as.character(2 * x)
}

bc_wdw <- function(date_iso) {
  # test: date_iso <- "2018-05-31". SOLL: c("31 May 2018", "1 Jun 2019")  
  wdw_start <- ymd(date_iso, quiet = T)
  wdw_stop <- wdw_start + days(1L)
  
  c(wdw_date_fmt(wdw_start), wdw_date_fmt(wdw_stop))
}

run_assembler <- function(cur_week, cur_mac) {
  
  rls_week <- cur_week |> filter(mac == cur_mac)

  sched_home <- str_replace(config$rl_sched_home, "xxx", str_to_lower(cur_mac))
  start_number <- dir_ls(schedules_dir) |> length()
  
  rls_week |>
    mutate(
      script_nr = start_number + row_number(),
      schedules_dir = sched_home
    ) |>
    group_split(row_number()) |>
    walk(prep_script_vars)
}

build_script_lines <- function(bc_day_mask,
                               bc_halfhour_index,
                               bc_act_wdw,
                               color_rgb,
                               pl_name,
                               duration) {
  script_lines <- c(
    "Radiologik Schedule Segment",
    bc_day_mask,
    duration,
    "standaard",
    "ProgramTo=0",
    bc_halfhour_index,
    "0",
    "",
    paste(bc_act_wdw[1], "0", sep = "\t"),
    paste(bc_act_wdw[2], "0", sep = "\t"),
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

prep_script_vars <- function(wk_row) {
  # test: wk_row <- cz_week[1, ]
  parts <- wk_row$rl_sched_name |>
    str_match("^(\\d{4}-\\d{2}-\\d{2})_([a-z]{2}\\d{2}-\\d)_(\\d{3})_(.+)$")
  date_iso <- parts[, 2]
  slot     <- parts[, 3]
  duration <- as.integer(parts[, 4])
  
  script_lines <- build_script_lines(
    bc_day_mask        = day_mask(slot),
    bc_halfhour_index  = slot_start_halfhour_index(slot), 
    bc_act_wdw         = bc_wdw(date_iso),
    color_rgb          = paste0("ColorLabel=", wk_row$sched_rgb),
    pl_name            = wk_row$playlist,
    duration           = duration
  )
  
  script_file_name <- path(
    wk_row$schedules_dir,
    sprintf("%03d - %s", wk_row$script_nr, wk_row$rl_sched_name)
  )
  
  write_lines(
    x = script_lines,
    file = script_file_name
  )
  
  invisible(script_file_name)
}

# > MAIN < ----

# 0. init ----
config <- read_yaml("config.yaml")
log_slug <- "pof_cz"
flap <- flog.appender(appender.file(config$log_appender_file), log_slug)

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

# 5. assemble the schedules ----
run_assembler(cur_week = cz_week, cur_mac = "LGM")
run_assembler(cur_week = cz_week, cur_mac = "UZM")
