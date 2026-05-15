# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# remove all rows from week
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
begin_ts <- force_tz(ymd_hms("2026-06-11 12:50:00", quiet = T), tzone = "Europe/Amsterdam")
end_ts <- begin_ts + days(7L)

# load SQL
sql <- read_file("resources/remove_week.sql")

# split statements
statements <- sql |>
  str_split(";") |>
  pluck(1) |>
  str_trim() |>
  discard(\(x) x == "")

# get query definition that needs variable binding
bind_stmt <- which(str_detect(statements, "--\\s*@bind:\\s*insert_values"))

# execute
walk(seq_along(statements), \(i) {
  if (i == bind_stmt) {
    dbExecute(
      con,
      statements[[i]],
      params = list(fmt_ts(begin_ts), fmt_ts(end_ts))
    )
  } else {
    dbExecute(con, statements[[i]])
  }
})
