# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Create UZM/LGM-RadioLogik schedules from latest GWS-playlist weeks
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

# 0. init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, 
               purrr, yaml, googlesheets4
               # DBI, digest, httr, jsonlite, ssh, uuid, RMariaDB, RSQLite, glue
)
config <- read_yaml("config.yaml")
log_slug <- "pof_cz"
flap <- flog.appender(appender.file(config$log_appender_file), log_slug)
source("R/rl_factory_functions.R", encoding = "UTF-8")

# 1. load GWS-sheet ----
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

# 3. build sched details ----
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
  # mutate(rgb_id = 1L + row_number() %% 2L,
  #        sched_rgb = colbands$RGB[rgb_id]) |>
  select(-uitzendingen,
         -slots,
         -rl_pgm_name,
         -date_slot,
         -programma,
         -type)

# 4. assemble the schedules ----
run_assembler(cur_week = cz_week, cur_mac = "LGM")
run_assembler(cur_week = cz_week, cur_mac = "UZM")

# 5. prep colour banding ----
colbands <- tribble(
  ~Group,   ~Row_type, ~RGB,
  "green",  "base",    "229,245,224",
  "green",  "band",    "161,217,155",
  "orange", "base",    "254,230,206",
  "orange", "band",    "253,174,107"
)

l1 <- fetch_rls(cur_mac = "LGM")
l1a <- l1 |> filter(is.na(ts2)) |> arrange(ts1) |> select(-t1)
l2 <- l1 |> filter(!is.na(ts2)) |> arrange(ts1) |> select(-t1)

nipper_studio_pattern <- "nacht(jazz|klassiek|oud)|geendag|ochtendeditie|ratatouille"

l2_ns <- l2 |> filter(str_detect(str_remove_all(p1, "[_-]"), nipper_studio_pattern)) |> 
  arrange(ts1)

l2_non_ns <- l2 |> filter(!str_detect(str_remove_all(p1, "[_-]"), nipper_studio_pattern)) |> 
  arrange(ts1)

l3 <- l1 |> filter(is.na(ts2)) |> arrange(ts1) |> select(-t1)
l4 <- l3 |> bind_rows(l2) |> 
  mutate(r2 = row_number() |> (\(x) sprintf("%03d", x))(),
         ln = if_else(!is.na(ts2),
                      paste0(r2, " - ", ts1, "-", ts2, "-", p1),
                      paste0(r2, " - ", p1)
         )
  ) |> 
  select(old_fn = li, new_fn = ln)

