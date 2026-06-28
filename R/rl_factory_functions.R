replace_color_label <- function(path, rgb) {
  bytes <- read_file_raw(path)
  txt <- rawToChar(bytes)
  
  pattern <- "^ColorLabel=\\d{1,3},\\d{1,3},\\d{1,3}$"
  replacement <- paste0("ColorLabel=", rgb)
  
  txt_new <- str_replace(txt, regex(pattern, multiline = TRUE), replacement)
  
  write_file(charToRaw(txt_new), path)
  
  invisible(TRUE)
}


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

  # stop RL-scheduler to swap the folder contents
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  #+ op Uitzend-mac ----
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  flog.info("Start scriptgeneratie op de Uitzend-mac", name = "rlsc_log")
  host <- "uitzendmac"
  home_radiologik <- home_prop("home_radiologik")
  switch_home <- paste0(home_prop("home_schedulerswitch"), "nipper_msg.txt")
  
  # Stop RL-scheduler en wacht 5 seconden - stoppen duurt soms even
  flog.info("RL-scheduler stoppen", name = "rlsc_log")
  switch <- read_lines(file = switch_home)
  switch <- "stop RL-scheduler"
  write_lines(switch, file = switch_home, append = FALSE)
  
  Sys.sleep(time = 5)
  flog.info("RL-scheduler is gestopt", name = "rlsc_log")
  
  #    
  rls_week <- cur_week |> filter(mac == cur_mac)
  
  sched_home <- str_replace(config$rl_sched_home, "xxx", str_to_lower(cur_mac))
  start_number <- dir_ls(schedules_dir) |> length()
  
  rls_week |>
    mutate(
      script_nr = start_number + row_number(),
      schedules_dir = sched_home
    ) |>
    group_split(row_number()) |>
    walk(write_week_sched)
}

build_script_lines <- function(bc_day_mask,
                               bc_halfhour_index,
                               bc_act_wdw,
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
    "ColorLabel=255,255,255",
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

write_week_sched <- function(wk_row) {
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

fetch_rls <- function(cur_mac) {
  # test: cur_mac <- "LGM"
  
  # sched_home <- str_replace(config$rl_sched_home, "xxx", str_to_lower(cur_mac))
  # dir_ls(sched_home, type = "file") |> 
  read_lines("resources/RL-schedules.txt") |> 
    tibble(li = _) |>
    mutate(lif = path_file(li),
           r1 = str_split(lif, simplify = T, pattern = " - "),
           n1 = r1[, 1],
           t1 = r1[, 2]
    ) |>
    select(-c(2:3)) |> 
    mutate(ts1 = str_extract(t1, "^\\d{4}-\\d{2}-\\d{2}[_ -][a-z]{2}\\d{2}(-\\d(?!\\d))?"),
           dt = ymd_h(ts1, tz = "Europe/Amsterdam"),
           ts2 = if_else(str_starts(ts1, "\\d"), 
                         str_extract(t1, "[_-](\\d{3})[_-]", group = 1), 
                         NA_character_),
           p1 = if_else(str_starts(ts1, "\\d"), 
                        str_extract(t1, paste0(ts1, "[_-]", ts2, "[_-](.*)"), group = 1) |> 
                          str_to_lower() |> str_replace("_-_", "_") |> str_replace("__", "_"), 
                        t1),
           p1 = str_replace(p1, "[(]herhaling[)]", "hh"),
           p1 = str_remove(p1, "_(ma|di|wo|do|vr|za|zo)$")
    )
}
