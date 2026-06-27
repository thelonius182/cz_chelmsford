# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, fs
)

sched_home <- str_replace(config$rl_sched_home, "xxx", str_to_lower(cur_mac))
# file_set <- dir_ls(sched_home, type = "file")
l1 <- read_lines("resources/RL-schedules.txt") |> tibble(li = _) |>
# l1 <- file_set |> tibble(li = _) |>
  mutate(lif = path_file(li),
         r1 = str_split(lif, simplify = T, pattern = " - "),
         n1 = r1[, 1],
         t1 = r1[, 2]
  ) |>
  select(-c(2:3)) |> 
  mutate(ts1 = str_extract(t1, "^\\d{4}-\\d{2}-\\d{2}[_ -][a-z]{2}\\d{2}(-\\d(?!\\d))?"),
         ts2 = if_else(str_starts(ts1, "\\d"), 
                       str_extract(t1, "[_-](\\d{3})[_-]", group = 1), 
                       NA_character_),
         p1 = if_else(str_starts(ts1, "\\d"), 
                      str_extract(t1, paste0(ts1, "[_-]", ts2, "[_-](.*)"), group = 1) |> 
                        str_to_lower() |> str_replace("_-_", "_") |> str_replace("__", "_"), 
                      t1)
  )

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
  

