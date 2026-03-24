sql_stmt <- "SELECT
                 JSON_UNQUOTE(JSON_EXTRACT(dates, '$.start')) AS start_ts,
                 JSON_UNQUOTE(JSON_EXTRACT(dates, '$.end')) AS stop_ts,
                 id as bc_id
             FROM entries
             WHERE type = 'broadcast'
               AND site_id = 1
               AND deleted_at IS NULL
               AND JSON_EXTRACT(dates, '$.start') IS NOT NULL
               AND JSON_EXTRACT(dates, '$.end')   IS NOT NULL
             order by 1 desc
             limit 600
"
tbl <- dbGetQuery(con, sql_stmt)

tz_used <- "Europe/Amsterdam"
x <- tbl |> mutate(start = coalesce(ymd_hms(start_ts, tz = tz_used, quiet = T), ymd_hm(start_ts, tz = tz_used, quiet = T)),
                   stop  = coalesce(ymd_hms(stop_ts,  tz = tz_used, quiet = T), ymd_hm(stop_ts,  tz = tz_used, quiet = T))) |>
  arrange(start, stop) |> select(-start_ts, -stop_ts)

candidate_starts <- x |> filter(wday(start, week_start = 1) == 4,
                                hour(start) == 13,
                                minute(start) == 0,
                                second(start) == 0) |> pull(start) |> unique() |> sort(decreasing = TRUE)
checks <- candidate_starts |> map(\(s) check_168h_w_report(s, x))
first_ok <- checks |> keep(\(z) z$ok) |> pluck(1, .default = NULL)

if (is.null(first_ok)) {
  message("No valid 168-hour sequence found.")
} else {
  valid_sequence <- first_ok$sequence
}

failure_report <- checks |> keep(\(z) !z$ok) |> map_dfr(\(z) {
    z$problems |> mutate(candidate_start = z$start_point, candidate_end = z$end_point, .before = 1)
  }
)
