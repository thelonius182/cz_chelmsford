pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, yaml, googlesheets4, purrr)

config <- read_yaml("config.yaml")
log_slug <- "nxt20"
apf <- flog.appender(appender.file(config$log_appender_file_nxt20), log_slug)
flog.info("\n= = = = = = extending WoJ Montagerooster = = = = = =", name = log_slug)

date_series <- function(earliest_date, slots, n = 20, tz = "Europe/Amsterdam") {
# date_series <- function(earliest_date, slots, n = 20) {
  
  nl_weekdays <- c("ma" = 1, "di" = 2, "wo" = 3, "do" = 4, "vr" = 5, "za" = 6, "zo" = 7)
  
  slots <- str_to_lower(slots)
  stopifnot(all(str_detect(slots, "^[[:alpha:]]{2}\\d{2}-([1-5]|\\*)$")))
  
  parsed_slots <- tibble(slot = slots) |>
    mutate(
      WD = str_extract(slot, "^[[:alpha:]]{2}"),
      HH = str_extract(slot, "(?<=^[[:alpha:]]{2})\\d{2}") |> as.integer(),
      WOM_raw = str_extract(slot, "(?<=-)([1-5]|\\*)$")
    ) |>
    filter(WD %in% names(nl_weekdays)) |>
    mutate(
      WOM = map(WOM_raw, \(x) {
        if (x == "*") 1:5 else as.integer(x)
      })
    ) |>
    unnest(WOM)
  
  stopifnot(
    all(parsed_slots$HH >= 0 & parsed_slots$HH <= 23),
    all(parsed_slots$WOM >= 1 & parsed_slots$WOM <= 5)
  )
  
  candidate_dates <- seq.Date(
    from = as_date(earliest_date),
    to = as_date(earliest_date) + years(5),
    by = "day"
  )
  
  tibble(date = candidate_dates) |>
    mutate(
      weekday_no = wday(date, week_start = 1),
      week_of_month = ((day(date) - 1) %/% 7) + 1
    ) |>
    crossing(parsed_slots) |>
    filter(
      weekday_no == nl_weekdays[WD],
      week_of_month == WOM
    ) |>
    mutate(
      datetime = ymd_h(
        paste(date, str_pad(HH, 2, pad = "0")),
        tz = tz
      )
    ) |>
    filter(datetime >= earliest_date) |>
    arrange(datetime) |>
    slice_head(n = n) |>
    select(datetime, date, slot, WD, HH, WOM)
}

# - - - - - - - - - - - - - - - - 
#     ---- >> MAIN << ----
# - - - - - - - - - - - - - - - - 

# which slots & titles
cur_slots <- c(
  "vr20-*", 
  "do14-4"
)

titles <- tibble(
  slot = cur_slots,
  title = c(
    "World of Jazz Live", 
    "Nordic Sounds"
  )
)

# load GD-sheets ----
montage_woj_URL <- config$gws_montage_woj
montage_woj_sheet <- "montage WoJ"
tryCatch(
  {
    # . trigger GD-auth
    options(gargle_oauth_cache = ".secrets-salsa")
    gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
    montage_woj_raw <- read_sheet(ss = montage_woj_URL, sheet = montage_woj_sheet)
  },
  error = function(e1) {
    flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
    break
  }
)

# find earliest date ----
nxt20_start_utc <- floor_date(max(montage_woj_raw$Uitzending), unit = "days") + days(1L)
nxt20_start_NL <- force_tz(nxt20_start_utc, tzone = "Europe/Amsterdam")

# create series ----
woj_rows <- date_series(earliest_date = nxt20_start_NL, slots = cur_slots) |> 
  mutate(slot_key = paste0(str_extract(slot, "^.{5}"), WOM),
         nxt20_month = month(datetime),
         band = 1 + nxt20_month %% 2,
         datetime = withr::with_locale(c(LC_TIME = "nl_NL.UTF-8"),
                                       format(datetime, "%a %e %b, %H:%M") |>
                                         str_squish() |>
                                         str_to_lower())) |> 
  inner_join(titles, by = join_by(slot)) |> 
  select(band, uitzending = datetime, titel = title)

# upload to GD ----
sapp <- sheet_append(ss = montage_woj_URL, sheet = montage_woj_sheet, data = woj_rows)
