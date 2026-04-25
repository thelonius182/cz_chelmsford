# - - - - - - - - - - - - -
# Build CZ programme clock
# - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI, digest,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
apf <- flog.appender(appender.file(config$log_appender_file_cz), "clof")
flog.info("Building a programme clock for CZ", name = "clof")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")

SITE <- list(
  CONCERTZENDER = 1L,
  WORLD_OF_JAZZ = 2L
)

STEP <- list(
  FACTORY = 10L,
  EDITOR  = 20L,
  DESK    = 30L,
  PLAYOUT = 99L
)

step_lookup <- tibble(
  step = c(10L, 20L, 30L, 99L),
  step_name = c("FACTORY", "EDITOR", "DESK", "PLAYOUT")
)

source("R/custom_functions.R", encoding = "UTF-8")
# connect to CPNM-database ----
source("R/cpnm_db_setup.R", encoding = "UTF-8")  

# > Main Control Loop ----
repeat {
  # Find start of week to build ----
  clock_start_utc <- start_of_cz_week(pm_cpnm_db = con)
  tz_am <- "Europe/Amsterdam"
  start_ts <- force_tz(clock_start_utc$next_week_start, tzone = tz_am)
  stop_ts   <- start_ts + days(7L)
  flog.info(str_glue("Week: Thursday {logfmt_ts(start_ts)} to Thursday {logfmt_ts(stop_ts)}"), name = "clof")
  
  # clockprofile from GD ----
  # . trigger GD-auth
  drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
  path_clockprofile_cz <- "/home/lon/R_projects/cz_chelmsford/resources/modelklok_cz.xlsx"
  drive_download(file = cz_get_url("modelklok_cz"), overwrite = T, path = path_clockprofile_cz)
  
  # clockcatalogue from GD ----
  path_clockcatalogue <- "/home/lon/R_projects/cz_chelmsford/resources/klokcatalogus.xlsx"
  drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_clockcatalogue)
  
  # sheets as df ----
  df_clockprofile_cz_raw <- cz_extract_sheet(path_clockprofile_cz, sheet_name = "cz-data")
  df_clockcatalogue_raw <- cz_extract_sheet(path_clockcatalogue, sheet_name = "gids-info")
  df_clockcatalogue <- df_clockcatalogue_raw |> rename(catalg_key = `key-modelrooster`,
                                                       titel_NL = `titel-NL`,
                                                       titel_EN = `titel-EN`,
                                                       productie = `productie-1-taak`,
                                                       redacteurs = `productie-1-mdw`,
                                                       genre_1 = `genre-1-NL`,
                                                       genre_2 = `genre-2-NL`,
                                                       intro_NL = `std.samenvatting-NL`,
                                                       intro_EN = `std.samenvatting-EN`,
                                                       afbeelding = feat_img_ids,
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
    select(-hh, -mac, -`dummy-1`, -slug) |> 
    pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  
  # validate clock length ----
  cur_clock_minutes <- df_clock_cz_cur |> select(slot, slot_minutes) |> distinct() |> summarise(total = sum(slot_minutes))
  
  if (cur_clock_minutes$total != 10080L) {
    flog.error("invalid clock length (1): expected 10080 minutes, but got {cur_clock_minutes}; quiting this job.", name = "clof")
    break
  }
  
  # validate catalogue ----
  missing <- df_clock_cz_cur |> filter(is.na(titel_NL)) |> nrow()
  
  if (missing > 0) {
    flog.error("clock catalogue is incomplete (title); quiting this job.", name = "clof")
    break
  }
  
  missing <- df_clock_cz_cur |> filter(is.na(episode_chain)) |> nrow()
  
  if (missing > 0) {
    flog.error("clock catalogue is incomplete (episode chain); quiting this job.", name = "clof")
    break
  }
  
  # genres ----
  query <- "select name->>'$.nl' as genre_NL, 
                   id as ty_genre_id 
            from taxonomies 
            where type = 'genre' 
              and deleted_at is null
              and site_id in (1, 2)
              and name->>'$.nl' not in ('Algemeen', 'World of Jazz')
            order by 1
            ;"
  
  ty_genres <- dbGetQuery(con, query)
  df_clock_cz_cur.1 <- df_clock_cz_cur |> left_join(ty_genres, by = join_by(genre == genre_NL))
  missing <- df_clock_cz_cur.1 |> filter(is.na(ty_genre_id)) |> nrow()
  
  if (missing > 0) {
    flog.error("genres missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # editors ----
  query <- "select name->>'$.nl' as editor_name, 
                   id as ty_editor_id
            from taxonomies 
            where type = 'colofon'
              and deleted_at is null
              and site_id in (1, 2)
              and name->>'$.en' is not null
            order by 1
            ;"
  ty_editors <- dbGetQuery(con, query)
  df_clock_cz_cur.2 <- df_clock_cz_cur.1 |> left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  
  # . check complete ----
  missing <- df_clock_cz_cur.2 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct() |> nrow()
  
  if (missing > 0) {
    flog.error("editors missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # images ----
  df_afb <- df_clock_cz_cur.2 |> filter(!is.na(afbeelding)) |> select(afbeelding) |> distinct() |> 
    mutate(afbeelding = as.integer(afbeelding), image_id = NA_character_) |> arrange(afbeelding)
  
  for (rn in seq_len(nrow(df_afb))) {
    df_image <- cpnm_img_get(pm_img_id = df_afb$afbeelding[rn], pm_cpnm_db = con)
    
    if (is.na(df_image$id)) {
      next
    }
    
    df_afb$image_id[rn] <- df_image$id
  }
  
  # . check complete ----
  missing <- df_afb |> filter(is.na(image_id)) |> nrow()
  
  if (missing > 0) {
    flog.error("images are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # . append ----
  df_clock_cz_cur.3 <- df_clock_cz_cur.2 |> left_join(df_afb, by = join_by(afbeelding))
  
  # programs ----
  query <- "WITH counts AS (
                SELECT LOWER(p.title_nl) AS titel_nl_lc,
                       p.id AS pgm_id,
                       COUNT(b.id) AS n_bcs
                FROM entries p LEFT JOIN entries e ON e.parent_id = p.id
                                             and e.type = 'episode'
                                             AND e.deleted_at IS NULL
                               LEFT JOIN entries b ON b.parent_id = e.id
                                             and b.type = 'broadcast'
                                             AND b.deleted_at IS NULL
                WHERE p.type = 'program'
                  AND p.deleted_at IS NULL
                  AND p.site_id IN (1, 2)
                GROUP BY LOWER(p.title_nl),
                         p.id
            ), ranked AS (SELECT titel_nl_lc,
                                 pgm_id,
                                 n_bcs,
                                 ROW_NUMBER() OVER (PARTITION BY titel_nl_lc
                                                    ORDER BY n_bcs DESC, pgm_id) AS rn
                          FROM counts
            )
            SELECT titel_nl_lc, pgm_id FROM ranked WHERE rn = 1 ORDER BY titel_nl_lc;"
  program_titles <- dbGetQuery(con, query)
  
  df_clock_cz_cur.4 <- df_clock_cz_cur.3 |> left_join(program_titles, by = join_by("titel_nl_lc"))
  missing <- df_clock_cz_cur.4 |> filter(is.na(pgm_id)) |> nrow()
  
  # . check complete ----
  if (missing > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = "clof")
    break
  }
  
  # . check length ----
  cur_clock_minutes <- df_clock_cz_cur.4 |> distinct(slot, slot_minutes) |> summarise(total = sum(slot_minutes))
  
  if (cur_clock_minutes$total != 10080L) {
    flog.error(str_glue("invalid clock length (2): expected 10080 minutes, but got {cur_clock_minutes$total}; quiting this job."), 
               name = "clof")
    break
  }
  
  # build clock history ----
  fmt_start_ts_m10 = fmt_ts(start_ts - minutes(10L))
  fmt_stop_ts_m10 = fmt_ts(stop_ts - minutes(10L))
  qry <- glue_sql("
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
                case when bc_start < {fmt_start_ts_m10} then 'PLAYOUT'
                     when content is null then 'FACTORY'
                     when content->>'$.nl' = 'null' then 'FACTORY'
                     else 'EDITOR' end as episode_status,
                episode_entry_id
         from ds1 where bc_start between '2025-10-02 13:00:00' and {fmt_stop_ts_m10}
         order by 1;", .con = con)
  db_clock_history <- dbGetQuery(con, qry)
  
  df_clock_history <- db_clock_history |> mutate(bc_start = ymd_hms(bc_start, tz = "Europe/Amsterdam", quiet = T))
  
  df_clock_cz_his <- add_bc_cols(df_clock_history, bc_start) |> 
    select(slot = bc_start, slot_key, block = bc_week_of_month, episode_chain = label, episode_entry_id, episode_status) |> 
    left_join(df_clockcatalogue, by = join_by(episode_chain)) |> select(-`dummy-1`, -slug, -genre_1, -genre_2)
  
  # merge cur/his-clocks ----
  df_clock_cz <- df_clock_cz_cur.4 |> bind_rows(df_clock_cz_his) |> arrange(slot, episode_status) |> 
    group_by(slot) |> mutate(rn = row_number()) |> ungroup() |> 
    select(1:5, is_replay, rn, episode_entry_id, episode_status, episode_chain, everything()) |> filter(rn == 1L) |> 
    select(-rn) |> 
    mutate(is_replay = if_else(!is.na(episode_entry_id), FALSE, is_replay))
  
  # > add new to database ----
  # do new ones first, to make sure that episodes replaying early, so this week, can be found in this week
  df_new_episodes_fresh <- df_clock_cz |> filter(!is_replay & is.na(episode_entry_id))
  ts_now = now()
  # show prepped value the way the database stores it: with timezone UTC
  fmt_created_at = format(ts_now, tz = "UTC", usetz = FALSE)
  flog.info(str_glue("adding fresh clock items to database, using `created_at` = {fmt_created_at} (UTC, as in database)"), 
            name = "clof")
  # insert everything with identical `created_at` timestamp, making them easy to spot later
  func_result <- clock2db(pm_clock_tib = df_new_episodes_fresh, pm_created_at = ts_now, pm_db = con)

  # . update clock tibble rows ----
  # test: fmt_created_at <- "2026-04-24 18:42:00"
  qry <- glue_sql(
    "select cast(b.dates->>'$.start' as datetime) as slot,
       e.id as episode_entry_id,
       'FACTORY' as episode_status
     from entries b join entries e on e.id = b.parent_id
                                  and e.deleted_at is null
                                  and e.type = 'episode'
     where b.deleted_at is null
       and b.type = 'broadcast'
       and b.site_id = 1
       and b.created_at = {fmt_created_at}
     order by 1;", .con = con)
  db_new_items <- dbGetQuery(con, qry) |> mutate(slot = force_tz(slot, tzone = tz_am))
  df_clock_cz.1 <- df_clock_cz |> rows_update(db_new_items, by = "slot")
  write_rds(x = df_clock_cz.1, file = "resources/df_clock_cz_1.RDS")
  
  # . check bc-count ----
  n_bcs_expected <- df_clock_cz.11 |> select(bc_ts) |> distinct() |> nrow()
  sql_stmt <- glue_sql("select count(*) as n from episode_chain_item 
                        where clockfactory_job = {job_id};", .con = con)
  n_bcs_added <- dbGetQuery(con, sql_stmt)
  
  if (n_bcs_added$n != n_bcs_expected) {
    flog.error(str_glue("adding originals failed: expected {n_bcs_expected}, but got {n_bcs_added$n}; quiting this job."), 
               name = "clof")
    break
  }
  
  flog.info(str_glue("added {n_bcs_added$n} fresh broadcasts"), name = "clof")
  
  # Exit from MCL
  break
}

# TOT HIER ----

# . get the replays ----
# replays only add a broadcast
df_clock_cz.13 <- df_clock_cz.11 |> filter(is_rp) |> 
  select(bc_ts, titel_nl_lc, slot_minutes, bc_type, nipper_mogelijk, episode_chain) |> distinct()

# get reusable episodes ----
# . if a broadcast is cancelled, its episode can be reused, so no need to add a new one.

  filter(is.na(dates))

# add "replay of"-dates ----
for (rn in seq_len(nrow(df_clock_cz.13))) {
  
  # 'live' broadcasts can't be replayed in the current week, as they need curating first; that won't happen until next
  # Thursday. So replay an episode that was broadcast last week or earlier
  # # pm_max_start <- case_when(str_match(df_clock_cz.13$bc_type[rn], "l|v") ~ start_ts,
  # #                           df_clock_cz.13$bc_type[rn] == "h" & df_clock_cz.13$nipper_mogelijk[rn] != "N" ~ 
  # } else {
  #   df_clock_cz.13$bc_ts[rn]
  # }
  
  df_replay <- cpnm_uni_get(pm_pgm_id = df_clock_cz.13$pgm_id[rn],
                            pm_max_start,
                            pm_cpnm_db = con)
  
  if (is.na(df_replay$epi_id)) {
    next
  }
  
  df_clock_cz.9$replay_of_epi_id[rn] <- df_replay$epi_id
  df_clock_cz.9$replay_of_ts[rn] <- df_replay$epi_start
}

for (rn in seq_len(nrow(woj_schedule_w_ids.6))) {
  
  if (woj_schedule_w_ids.6$broadcast_type[rn] != "Universe") {
    next
  }
}

  # Replays ----
  # - . adjust offset ----
  # - NipperStudio programs should get replays of 25 weeks ago. Subtract 24 weeks more (cuurently: 1 week ago), unless
  #   that episode doesn´t exist
  df_clock_cz.10 <- df_clock_cz.9 |> 
    mutate(rp_of_ts_adj = if_else(!is.na(rp_of_ts) & nipper_mogelijk %in% c("BUM", "VOT"),
                                  rp_of_ts - days(168),
                                  rp_of_ts))
  # - . check episodes exist ----
  adjusted_replays <- df_clock_cz.10 |> select(ts, pgm_id, rp_of_ts, rp_of_ts_adj, rp_formula) |> 
    filter(!is.na(rp_of_ts_adj)) |> mutate(pgm_id_valid = FALSE) |> distinct()
  
  # - . adjust replay titles ----
  # Biweekly replays have the same title as the original. Other replays have the title found in the original slot. 
  # So, first add the original slot
  adjusted_titles.1 <- adjusted_replays |> 
    mutate(
      rp_of_day = c("ma", "di", "wo", "do", "vr", "za", "zo")[wday(ts, week_start = 1)],
      rp_of_week_of_month = 1 + (mday(rp_of_ts_adj) - 1) %/% 7,
      rp_start = hour(rp_of_ts_adj)
    )
  # next, add bc_clock_key
  adjusted_titles.2 <- adjusted_titles.1 |> left_join(df_clock_cz.3, by = join_by(rp_of_week_of_month == bc_week_of_month,
                                                                                  rp_of_day == bc_day_label,
                                                                                  rp_start == bc_hour_start))
  
  for (rn in seq_len(nrow(adjusted_replays))) {
    # sql_res will be empty for replays within the current week, as the original doesn't exist yet
    # assume it's OK
    cur_rp_of_ts <- adjusted_replays$rp_of_ts_adj[rn]
    replay_valid <- if (cur_rp_of_ts >= start_ts) {
      TRUE
    } else {
      cpnm_chk_cz_rp(pm_ts = fmt_ts(adjusted_replays$rp_of_ts_adj[rn]),
                     pm_pgm_id = adjusted_replays$pgm_id[rn],
                     pm_cpnm_db = con)
    }
    adjusted_replays$pgm_id_valid[rn] <- replay_valid
  }
  
  df_clock_cz.11_err <- df_clock_cz.11 |> inner_join(adjusted_replays, by = join_by(ts))
  
  for (rn in seq_len(nrow(cz_schedule_w_ids.6))) {
    
    if (cz_schedule_w_ids.6$broadcast_type[rn] != "ReplayWoJ") {
      next
    }
    
    df_replay <- cpnm_uni_get(pm_pgm_id = cz_schedule_w_ids.6$pgm_id[rn],
                              pm_max_start = cz_schedule_w_ids.6$bc_start[rn],
                              pm_cpnm_db = con)
    
    if (is.na(df_replay$epi_id)) {
      next
    }
    
    cz_schedule_w_ids.6$replay_of_epi_id[rn] <- df_replay$epi_id
    cz_schedule_w_ids.6$replay_of_ts[rn] <- df_replay$epi_start
  }
  
  # . check complete ----
  cz_schedule_w_ids_missing <- cz_schedule_w_ids.6 |> filter(broadcast_type == "Universe" & is.na(replay_of_epi_id))
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("WorldOfJazz-replays are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # Build new WJ-week ----
  # - some programs have 2 main genres, so have 2 records; treat them separately: 7a for all columns,
  #   and 7b just for the extra genre
  cz_schedule_w_ids.7 <- cz_schedule_w_ids.6 |> group_by(bc_start) |> mutate(sch_item = row_number()) |> ungroup()
  cz_schedule_w_ids.7a <- cz_schedule_w_ids.7 |> filter(sch_item == 1)
  cz_schedule_w_ids.7b <- cz_schedule_w_ids.7 |> filter(sch_item == 2)
  
  for (rn in seq_len(nrow(cz_schedule_w_ids.7a))) {
    # temp exception
    if (rn %in% c(1, 2)) next
    # temp exception
    if (is.na(cz_schedule_w_ids.7a$replay_of_epi_id[rn])) {
      # . fresh episode & broadcast ----
      fresh_epi_bc <- cpnm_epi_bc_ins(pm_pgm_id = cz_schedule_w_ids.7a$pgm_id[rn],
                                      pm_descr_NL = cz_schedule_w_ids.7a$intro_NL[rn],
                                      pm_descr_EN = cz_schedule_w_ids.7a$intro_EN[rn],
                                      pm_img_id = cz_schedule_w_ids.7a$image_id[rn],
                                      pm_site_id = wj_site_id,
                                      pm_bc_start = cz_schedule_w_ids.7a$bc_start[rn],
                                      pm_bc_minutes = cz_schedule_w_ids.7a$minutes[rn],
                                      pm_cpnm_db = con)
      # . genre ----
      # - add an `episode` taxonomable record for first genre
      txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                              pm_txy_id = cz_schedule_w_ids.7a$ty_genre_id[rn],
                              pm_order = 1,
                              pm_cpnm_db = con)
      
      # - add an `episode` taxonomable record for second genre
      df_g2 <- cz_schedule_w_ids.7b |> filter(bc_start == cz_schedule_w_ids.7a$bc_start[rn])
      
      if (nrow(df_g2) == 1) {
        txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                                pm_txy_id = df_g2$ty_genre_id,
                                pm_order = 2,
                                pm_cpnm_db = con)
      }
      
      # . editor ----
      # - add an `episode` taxonomable record for editors and production-role (txy-type colofon)
      txb_res <- cpnm_txb_edi_ins(pm_epi_id = fresh_epi_bc,
                                  pm_txy_id = cz_schedule_w_ids.7a$ty_editor_id[rn],
                                  pm_role_NL = cz_schedule_w_ids.7a$production_role[rn],
                                  pm_cpnm_db = con)
    } else {
      # . replay ----
      bc_replay_res <- cpnm_bc_ins(pm_pgm_id = cz_schedule_w_ids.7a$pgm_id[rn],
                                   pm_epi_id = cz_schedule_w_ids.7a$replay_of_epi_id[rn],
                                   pm_site_id = wj_site_id,
                                   pm_bc_start = cz_schedule_w_ids.7a$bc_start[rn],
                                   pm_bc_minutes = cz_schedule_w_ids.7a$minutes[rn],
                                   pm_cpnm_db = con)
    }
  }
  
  # exit MCL
  break
# }

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("job finished", name = "clof")
