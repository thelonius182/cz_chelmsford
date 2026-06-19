# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# remove all rows from week X site Y
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
begin_ts <- force_tz(ymd_hms("2026-06-04 12:55:00", quiet = T), tzone = "Europe/Amsterdam")
end_ts <- begin_ts + days(7L)
site_id <- 1L

# load SQL
sql <- read_file("SQL/remove_week.sql")

# split statements
statements <- sql |>
  str_split(";") |>
  pluck(1) |>
  str_trim() |>
  discard(\(x) x == "")

# get query definition that needs variable binding
bind_stmt_ins <- which(str_detect(statements, "--\\s*@bind:\\s*insert_values"))
bind_stmt_del <- which(str_detect(statements, "--\\s*@bind:\\s*delete those broadcasts"))

# execute
walk(seq_along(statements), \(i) {
  if (i == bind_stmt_ins) {
    dbExecute(
      con_cpnm,
      statements[[i]],
      params = list(fmt_ts(begin_ts), fmt_ts(end_ts), site_id)
    )
  } else if (i == bind_stmt_del) {
    dbExecute(
      con_cpnm,
      statements[[i]],
      params = site_id
    )
  } else {
    dbExecute(
      con_cpnm, 
      statements[[i]]
    )
  }
})
