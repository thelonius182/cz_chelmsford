# trigger GD-auth ----
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# clockmodel from GD ----
path_clockmodel_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockmodel_cz)

# clockcatalogue from GD ----
path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)

# sheets as df ----
df_clockmodel_cz_raw <- cz_extract_sheet(path_clockmodel_cz, sheet_name = "cz-data")
df_clockcatalogue_raw <- cz_extract_sheet(path_clockcatalogue, sheet_name = "gids-info")

# tidy clockmodel ----
df_clockmodel_cz_raw <- df_clockmodel_cz_raw |> rename(slot_key = slot) |> 
  mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key)
mk_weekly <- df_clockmodel_cz_raw |> filter(!is.na(wekelijks)) |> select(1:6) |> 
  rename(catalg_key = wekelijks, prod_type = te)
mk_biweekly<- df_clockmodel_cz_raw |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
  rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
mk_week.1 <- df_clockmodel_cz_raw |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
  mutate(block = 1L)
mk_week.2 <- df_clockmodel_cz_raw |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
  mutate(block = 2L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
mk_week.3 <- df_clockmodel_cz_raw |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
  mutate(block = 3L)
mk_week.4 <- df_clockmodel_cz_raw |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
  mutate(block = 4L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
mk_week.5 <- df_clockmodel_cz_raw |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
  mutate(block = 5L)

# build a clock ----
bc_week_ts <- tibble(ts = seq(from = start_ts, to = stop_ts - hours(1), by = "hour"))
df_calendar <- add_bc_cols(bc_week_ts, ts) |> 
  mutate(cycle = if_else(bc_day_label == "do" & bc_hour_start == 13, week_label(ts), NA_character_)) |> 
  fill(cycle, .direction = "down") |> select(slot = ts, slot_key, block = bc_week_of_month, cycle)
df_clock_cz_weekly <- df_calendar |> inner_join(mk_weekly, by = join_by(slot_key))
df_clock_cz_biweekly <- df_calendar |> inner_join(mk_biweekly, by = join_by(slot_key, cycle))
mk_5_weeks <- mk_week.1 |> bind_rows(mk_week.2) |>
                           bind_rows(mk_week.3) |>
                           bind_rows(mk_week.4) |>
                           bind_rows(mk_week.5)
df_clock_cz_5_weeks <- df_calendar |> inner_join(mk_5_weeks, by = join_by(slot_key, block))
df_clock_cz <- df_clock_cz_weekly |> bind_rows(df_clock_cz_biweekly) |> bind_rows(df_clock_cz_5_weeks) |> arrange(slot)
