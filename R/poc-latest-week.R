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

check_168h_w_report <- function(start_point, dat) {
  end_point <- start_point + hours(168)
  
  seq_dat <- dat |>
    filter(stop > start_point, start < end_point) |>
    arrange(start, stop)
  
  problems <- list()
  
  if (nrow(seq_dat) == 0) {
    return(list(
      ok = FALSE,
      start_point = start_point,
      end_point = end_point,
      sequence = tibble(),
      problems = tibble(
        type = "no_data",
        from = as.POSIXct(NA, tz = tz_used),
        to = as.POSIXct(NA, tz = tz_used),
        duration_secs = NA_real_,
        detail = "No rows intersect the requested 168-hour window")
    )
    )
  }
  
  seq_dat <- seq_dat |>
    # clip each interval to the requested 168-hour window. 
    # example: start = 2026-03-05 12:00:00
    #          stop  = 2026-03-05 14:00:00
    # That row does cover the beginning of the window, but it starts before the window.
    mutate(eff_start = pmax(start, start_point),
           eff_stop  = pmin(stop, end_point)) |> filter(eff_start < eff_stop)
  
  if (nrow(seq_dat) == 0) {
    return(list(ok = FALSE,
                start_point = start_point,
                end_point = end_point,
                sequence = tibble(),
                problems = tibble(
                  type = "no_coverage",
                  from = as.POSIXct(NA, tz = tz_used),
                  to = as.POSIXct(NA, tz = tz_used),
                  duration_secs = NA_real_,
                  detail = "Rows exist, but none cover any part of the requested window")))
  }
  
  if (min(seq_dat$eff_start) > start_point) {
    problems <- append(problems, list(tibble(
      type = "gap_at_start",
      from = start_point,
      to = min(seq_dat$eff_start),
      duration_secs = as.numeric(min(seq_dat$eff_start) - start_point, units = "secs"),
      detail = "Coverage starts after requested start")))
  }
  
  if (max(seq_dat$eff_stop) < end_point) {
    problems <- append(problems, list(tibble(
      type = "gap_at_end",
      from = max(seq_dat$eff_stop),
      to = end_point,
      duration_secs = as.numeric(end_point - max(seq_dat$eff_stop), units = "secs"),
      detail = "Coverage ends before requested end")))
  }
  
  if (nrow(seq_dat) > 1) {
    pair_checks <- tibble(
      prev_stop = seq_dat$eff_stop[-nrow(seq_dat)],
      next_start = seq_dat$eff_start[-1]
    ) |> mutate(diff_secs = as.numeric(next_start - prev_stop, units = "secs"),
                type = case_when(diff_secs > 0 ~ "gap",
                                 diff_secs < 0 ~ "overlap",
                                 TRUE ~ "ok"),
                from = case_when(diff_secs > 0 ~ prev_stop,
                                 diff_secs < 0 ~ next_start,
                                 TRUE ~ as.POSIXct(NA, tz = tz_used)),
                to = case_when(diff_secs > 0 ~ next_start,
                               diff_secs < 0 ~ prev_stop,
                               TRUE ~ as.POSIXct(NA, tz = tz_used)),
                duration_secs = abs(diff_secs),
                detail = case_when(diff_secs > 0 ~ "Gap between consecutive intervals",
                                   diff_secs < 0 ~ "Overlap between consecutive intervals",
                                   TRUE ~ "No problem")) |> 
      filter(type != "ok") |> select(type, from, to, duration_secs, detail)
    
    if (nrow(pair_checks) > 0) {
      problems <- append(problems, list(pair_checks))
    }
  }
  
  problems_tbl <- bind_rows(problems)
  
  list(
    ok = nrow(problems_tbl) == 0,
    start_point = start_point,
    end_point = end_point,
    sequence = seq_dat,
    problems = problems_tbl)
}
