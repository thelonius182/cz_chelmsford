separate_dt <- function(fn) {

  sep <- "[ _.-]"
  day <- "(?:ma|di|wo|do|vr|za|zo)"
  hour <- "(?:[01]?\\d|2[0-3])"
  
  hour_pat <- paste0(
    "^(?:",
    
    # Case 1: ordinal weekday, e.g. za2_18u
    day, "[1-5]", sep,
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    "|",
    
    # Case 2: weekday directly before hour, e.g. za17
    "(?!", day, "[1-5]", sep, ")",
    day,
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    "|",
    
    # Case 3: plain hour, e.g. 2100 or 0000
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    ")"
  )
  
  tibble(raw = fn) |>
    mutate(
      ymd_chr = str_sub(raw, 1, 8),
      after_date = str_sub(raw, 10),
      m = str_match(after_date, hour_pat),
      hour = coalesce(m[, 2], m[, 3], m[, 4]),
      
      datetime_candidate = if_else(!is.na(hour),
                                   str_c(ymd_chr, " ", str_pad(hour, 2, pad = "0"), ":00:00"),
                                   NA_character_),
      
      datetime = ymd_hms(datetime_candidate, quiet = TRUE),
      
      datetime_chr = if_else(!is.na(datetime),
                             format(datetime, "%Y-%m-%d %H:%M:%S"),
                             NA_character_)
    ) |>
    pull(datetime_chr) 
}
