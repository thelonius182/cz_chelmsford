date_series <- function(
    earliest_date,
    rules,
    n = 20,
    tz = "Europe/Amsterdam"
) {
  nl_weekdays <- c(
    "ma" = 1,
    "di" = 2,
    "wo" = 3,
    "do" = 4,
    "vr" = 5,
    "za" = 6,
    "zo" = 7
  )
  
  rules <- str_to_lower(rules)
  
  stopifnot(all(str_detect(rules, "^[[:alpha:]]{2}\\d{2}-([1-5]|\\*)$")))
  
  parsed_rules <- tibble(rule = rules) |>
    mutate(
      WD = str_extract(rule, "^[[:alpha:]]{2}"),
      HH = str_extract(rule, "(?<=^[[:alpha:]]{2})\\d{2}") |> as.integer(),
      WOM_raw = str_extract(rule, "(?<=-)([1-5]|\\*)$")
    ) |>
    filter(WD %in% names(nl_weekdays)) |>
    mutate(
      WOM = map(WOM_raw, \(x) {
        if (x == "*") 1:5 else as.integer(x)
      })
    ) |>
    unnest(WOM)
  
  stopifnot(
    all(parsed_rules$HH >= 0 & parsed_rules$HH <= 23),
    all(parsed_rules$WOM >= 1 & parsed_rules$WOM <= 5)
  )
  
  min_start_ts <- ymd_hms(earliest_date, tz = tz, quiet = TRUE)
  
  if (is.na(min_start_ts)) {
    min_start_ts <- as_datetime(as_date(earliest_date), tz = tz)
  }
  
  candidate_dates <- seq.Date(
    from = as_date(min_start_ts),
    to = as_date(min_start_ts) + years(5),
    by = "day"
  )
  
  tibble(date = candidate_dates) |>
    mutate(
      weekday_no = wday(date, week_start = 1),
      week_of_month = ((day(date) - 1) %/% 7) + 1
    ) |>
    crossing(parsed_rules) |>
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
    filter(datetime >= min_start_ts) |>
    arrange(datetime) |>
    slice_head(n = n) |>
    select(datetime, date, rule, WD, HH, WOM)
}

result <- date_series(earliest_date = "2026-05-30", rules = c("vr19-*", "do00-3")) |> 
  mutate(slot_key = paste0(str_extract(rule, "^.{5}"), WOM)) |> 
  select(datetime, slot_key, rule)

result |>mutate(datetime = fmt_ts(datetime)) |>
  clipr::write_clip()
