# trigger GD-auth ----
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# clockprofile from GD ----
path_clockprofile_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockprofile_cz)

# clockcatalogue from GD ----
path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)

# sheets as df ----
df_clockprofile_cz_raw <- cz_extract_sheet(path_clockprofile_cz, sheet_name = "cz-data")
df_clockcatalogue_raw <- cz_extract_sheet(path_clockcatalogue, sheet_name = "gids-info")
df_clockcatalogue <- df_clockcatalogue_raw |> rename(catalg_key = `key-modelrooster`,
                                                     episode_chain = `episode-chain`) |> 
  mutate(catalg_key = str_replace(catalg_key, "_(ma|di|wo|do|vr|za|zo)", ""))

# tidy clockprofile ----
df_clockprofile_cz_raw <- df_clockprofile_cz_raw |> rename(slot_key = slot) |> 
  mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key)

mk_weekly <- df_clockprofile_cz_raw |> filter(!is.na(wekelijks)) |> select(1:6) |> 
  rename(catalg_key = wekelijks, prod_type = te)
mk_biweekly<- df_clockprofile_cz_raw |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
  rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
mk_week.1 <- df_clockprofile_cz_raw |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
  mutate(block = 1L)
mk_week.2 <- df_clockprofile_cz_raw |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
  mutate(block = 2L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
mk_week.3 <- df_clockprofile_cz_raw |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
  mutate(block = 3L)
mk_week.4 <- df_clockprofile_cz_raw |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
  mutate(block = 4L, slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
mk_week.5 <- df_clockprofile_cz_raw |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
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

df_clock_cz_cur <- df_clock_cz_weekly |> bind_rows(df_clock_cz_biweekly) |> bind_rows(df_clock_cz_5_weeks) |> arrange(slot) |> 
  left_join(df_clockcatalogue, by = join_by(catalg_key)) |> 
  mutate(hh = if_else(prod_type == "h", "H", hh),
         prod_type = if_else(prod_type == "h", "u", prod_type)) |> 
  mutate(is_replay = if_else(is.na(hh), FALSE, TRUE)) |> 
  select(-hh)

# build clock history ----
fmt_start_ts = fmt_ts(start_ts)
qry <- "
with ds1 as (
   select ec.label, ci.episode_entry_id, e.content, min(b.dates->>'$.start') as bc_start
         from episode_chain_item ci 
            join episode_chain ec on ec.chain_id = ci.chain_id
            join entries e on e.id = ci.episode_entry_id
                          and e.deleted_at is null  
                          and e.type = 'episode'
                          and e.site_id = 1
            join entries b on b.parent_id = e.id
                          and b.deleted_at is null
                          and b.type = 'broadcast'
                          and b.site_id = 1
   group by ec.label, ci.episode_entry_id, e.content
)
select bc_start, 
       label, 
       case when bc_start < now() then 'PLAYOUT'
            when content is null then 'FACTORY'
            when content->>'$.nl' = 'null' then 'FACTORY'
            else 'EDITOR' end as episode_status,
       episode_entry_id
from ds1 where bc_start >= '2025-08-28 13:00:00'
order by 1;"
db_clock_history <- dbGetQuery(con, qry)

df_clock_history <- db_clock_history |> mutate(bc_start = ymd_hms(bc_start, tz = "Europe/Amsterdam", quiet = T))

df_clock_cz_his <- add_bc_cols(df_clock_history, bc_start) |> 
  select(slot = bc_start, slot_key, block = bc_week_of_month, episode_chain = label, episode_entry_id, episode_status) |> 
  left_join(df_clockcatalogue, by = join_by(episode_chain))

df_clock_cz <- df_clock_cz_cur |> bind_rows(df_clock_cz_his) |> arrange(slot, episode_status) |> 
  group_by(slot) |> mutate(rn = row_number()) |> ungroup() |> 
  select(1:5, is_replay, rn, episode_entry_id, episode_status, episode_chain, everything()) |> filter(rn == 1L) |> 
  select(-rn) |> 
  mutate(is_replay = if_else(!is.na(episode_entry_id), FALSE, is_replay))

df_new_episodes_fresh <- df_clock_cz |> filter(!is_replay & is.na(episode_entry_id))
job_id <- UUIDgenerate(use.time = FALSE) 
flog.info(str_glue("adding clock_fresh to database, job = {job_id}"), name = "clof")
func_result <- clock2db(pm_clock_tib = df_new_episodes_fresh, pm_job_id = job_id, pm_epi_step = STEP$FACTORY, pm_db = con)

