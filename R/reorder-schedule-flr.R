# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, fs
)

l1 <- read_lines("resources/RL-schedules.txt") |> tibble(li = _) |> 
  mutate(r1 = str_split(li, simplify = T, pattern = " - "),
         n1 = r1[, 1],
         t1 = r1[, 2]
  ) |>
  select(-c(2:3)) |> 
  # select(li, r1, n1, t1) |> 
  mutate(ts1 = str_sub(t1, 1, 15),
         ts2 = if_else(str_starts(ts1, "\\d"), 
                       str_sub(t1, 17, 19), 
                       NA_character_),
         p1 = if_else(str_starts(ts1, "\\d"), 
                      str_sub(t1, 21) |> str_to_lower() |> str_replace("_-_", "_") |> str_replace("__", "_"), 
                      t1)
  )

l2 <- l1 |> filter(!is.na(ts2)) |> arrange(ts1) |> select(-t1)
l3 <- l1 |> filter(is.na(ts2)) |> arrange(ts1) |> select(-t1)
l4 <- l3 |> bind_rows(l2) |> 
  mutate(r2 = row_number() |> (\(x) sprintf("%03d", x))(),
         ln = if_else(!is.na(ts2),
                      paste0(r2, " - ", ts1, "-", ts2, "-", p1),
                      paste0(r2, " - ", p1)
              )
  ) |> 
  select(old_fn = li, new_fn = ln)
  

