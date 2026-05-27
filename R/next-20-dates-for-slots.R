date_series <- function(
    start_date,
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
  
  start_datetime <- ymd_hms(start_date, tz = tz, quiet = TRUE)
  
  if (is.na(start_datetime)) {
    start_datetime <- as_datetime(as_date(start_date), tz = tz)
  }
  
  candidate_dates <- seq.Date(
    from = as_date(start_datetime),
    to = as_date(start_datetime) + years(5),
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
    filter(datetime >= start_datetime) |>
    arrange(datetime) |>
    slice_head(n = n) |>
    select(datetime, date, rule, WD, HH, WOM)
}

result <- date_series(start_date = "2026-05-30", rules = c("vr19-*")) |> 
  mutate(slot = paste0(str_extract(rule, "^.{5}"), WOM)) |> 
  select(datetime)

result |>mutate(datetime = format(datetime, "%Y-%m-%d %H:%M:%S")) |>
  clipr::write_clip()
