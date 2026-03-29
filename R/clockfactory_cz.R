# - - - - - - - - - - - - -
# Build CZ programme clock
# - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)
config <- read_yaml("config.yaml")
apf <- flog.appender(appender.file(config$log_appender_file_cz), "clof")
flog.info("Building a programme clock for CZ", name = "clof")
cz_site_id <- 1L
wj_site_id <- 2L
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
source("R/custom_functions.R", encoding = "UTF-8")

# > Main Control Loop ----
repeat {
  # connect to DB
  source("R/cpnm_db_setup.R", encoding = "UTF-8")  
  
  # Find start of week to build ----
  clock_start_utc <- start_of_cz_week(pm_cpnm_db = con)
  tz_am <- "Europe/Amsterdam"
  start_ts <- force_tz(clock_start_utc$next_week_start, tzone = tz_am)
  stop_ts   <- start_ts + days(7)
  flog.info(str_glue("Week: Thursday {logfmt_ts(start_ts)} to Thursday {logfmt_ts(stop_ts)}"), name = "clof")
  
  bc_week_ts <- tibble(
    ts = seq(from = start_ts, to = stop_ts - hours(1), by = "hour")
  )
  
  bc_week <- add_bc_cols(bc_week_ts, ts)
  
  # downloads GD ----
  # . trigger GD-auth
  drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
  
  # . get CZ clock matrix from GD
  path_rooster_cz <- "/home/lon/R_projects/cz_chelmsford/resources/rooster_cz.xlsx"
  drive_download(file = cz_get_url("rooster_cz"), overwrite = T, path = path_rooster_cz)
  
  # . get programme catalogue from GD
  path_wp_gidsinfo <- "/home/lon/R_projects/cz_chelmsford/resources/wordpress_gidsinfo.xlsx"
  drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_wp_gidsinfo)

  # sheets as df ----
  df_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")
  df_raw_zenderschema_cz <- cz_extract_sheet(path_rooster_cz, sheet_name = paste0("modelrooster-", config$modelrooster_versie))
  
  df_clock_cz.1 <- df_raw_zenderschema_cz |> 
    mutate( # start = as.integer(start),
           start = str_pad(string = start, side = "left", width = 5, pad = "0"), 
           slot = paste0(str_sub(dag, 1, 2), str_sub(start, 1, 2)),
           slot_day = str_sub(dag, 1, 2),
           slot_start = as.integer(str_sub(start, 1, 2)),
           slot_minutes = as.integer(str_sub(start, 3))) |> 
    rename(slot_replay = `hhOffset-dag.uur`,
           weekly = `elke week`,
           AB_cycle = `twee-wekelijks`,
           cycle_A = A,
           cycle_B = B,
           week_1 = `week 1`,
           week_2 = `week 2`,
           week_3 = `week 3`,
           week_4 = `week 4`,
           week_5 = `week 5`) |> 
    select(-dag, -start, -Balk, -Toon, -starts_with("r"), -starts_with("b")) |> 
    select(slot:slot_minutes, slot_replay, everything())
  
  # split 5-week cycle (a) and 2-week cycle (b)
  df_clock_cz.1a <- df_clock_cz.1 |> select(!contains("cycle")) |> filter(is.na(slot_replay) | slot_replay != "tw")
  df_clock_cz.1b <- df_clock_cz.1 |> select(slot:slot_replay, AB_cycle, cycle_A, cycle_B) |> filter(slot_replay == "tw")
  
  # pivot the 5-week repeating columns to rows
  df_clock_cz.2a <- df_clock_cz.1a |> rename(week_0 = weekly, t0 = te) |>
    pivot_longer(cols = !starts_with("slot"),
                 names_to = c(".value", "bc_week"),
                 names_pattern = "^(week|t)_?(\\d+)$") |> rename(bc = week, bc_type = t) |> filter(!is.na(bc))
  
  # prep the 2-week cycle for binding to the pivoted 5-week cycle
  cur_week_label <- week_label(date(start_ts))
  df_clock_cz.2b <- df_clock_cz.1b |> mutate(cycle_vec = cur_week_label,
                                             bc_type = if_else(cycle_vec == "A", cycle_A, cycle_B)) |> 
    select(slot:slot_replay, bc = AB_cycle, bc_type)
  
  # bind the cycles
  df_clock_cz.2c <- df_clock_cz.2a |> bind_rows(df_clock_cz.2b) |> mutate(bc_week = as.integer(bc_week))
  df_clock_cz.2d <- df_clock_cz.2c |>
    mutate(n = if_else(is.na(bc_week) | bc_week == 0L, 5L, 1L)) |>
    uncount(n, .id = "id") |>
    mutate(bc_week = if_else(is.na(bc_week) | bc_week == 0L, id, bc_week)) |> select(-id) |> arrange(slot, bc_week)
  
  # combine week and schedule ----
  df_clock_cz.3 <- bc_week |> left_join(df_clock_cz.2d, 
                                        by = join_by(bc_day_label == slot_day, 
                                                     bc_week_of_month == bc_week, 
                                                     bc_hour_start == slot_start)) |> 
    mutate(slot_replay_4 = str_extract(slot_replay, "^\\d{2}(.{4})", group = 1),
           slot = if_else(is.na(slot), 
                          paste0(bc_day_label, str_pad(bc_hour_start, side = "left", width = 2, pad = "0")),
                          slot)) |> 
    select(-bc_week_of_month, -bc_day_label, -bc_hour_start, -slot_replay) |> 
    rename(slot_replay = slot_replay_4, bc_ts = ts, bc_clock_key = bc) |> 
    mutate(slot_replay = if_else(bc_type == "h", slot, slot_replay))
    
  # add "replay on"-dates
  prep_rp_ts <- df_clock_cz.3 |> select(slot, rp_on_ts = bc_ts)  
  df_clock_cz.4 <- df_clock_cz.3 |> left_join(prep_rp_ts, join_by(slot_replay == slot))
  
  # prep as replays ----
  df_clock_rp <- df_clock_cz.4 |> filter(!is.na(rp_on_ts) | bc_type == "h") |> 
    select(bc_ts = rp_on_ts, slot = slot_replay, slot_minutes:bc_type) |> 
    mutate(is_rp = TRUE)
  
  # join origs & replays ----
  df_clock_cz.5 <- df_clock_cz.4 |> bind_rows(df_clock_rp) |> arrange(bc_ts) |>
    filter(!if_all(slot_minutes:is_rp, is.na) & (bc_type != "h" | !is.na(is_rp))) |> select(-slot_replay, -rp_on_ts) |> 
    mutate(is_rp = coalesce(is_rp, FALSE))
  
  # validate clock length ----
  cur_clock_minutes <- sum(df_clock_cz.5$slot_minutes)
  
  if (cur_clock_minutes != 10080L) {
    flog.error("invalid clock length (1): expected 10080 minutes, but got {cur_clock_minutes}; quiting this job.", name = "clof")
    break
  }
  
  # cz_weekdays <- c("ma", "di", "wo", "do", "vr", "za", "zo")
  # 
  # df_replays <- df_clock_cz.4 |> filter(!is.na(hh_formule) & (bc_cycle == "h" | is.na(bc_cycle))) |> 
  #   rename(bc_ts = ts) |> 
  #   left_join(bc_week, by = join_by(hh_day == bc_day_label, rp_start == bc_hour_start)) |> 
  #   select(-bc_week_of_month) |> 
  #   rename(rp_on_ts = ts, rp_on_day = hh_day, rp_on_start = rp_start, rp_formula = hh_formule, rp_on_offset = hh_offset) |> 
  #   mutate(rp_of_ts = rp_on_ts - days(rp_on_offset),
  #          rp_of_ts = update(rp_of_ts, hour = bc_hour_start),
  #          rp_of_weekblock = 1 + (mday(rp_of_ts) - 1) %/% 7,
  #          rp_of_day = c("ma", "di", "wo", "do", "vr", "za", "zo")[wday(rp_of_ts, week_start = 1)],
  #          rp_of_start = hour(rp_of_ts)) |> 
  #   select(rp_on_ts, rp_on_day, rp_on_start, mr_key:bc_minutes, rp_of_ts:rp_of_start, rp_formula) |> 
  #   left_join(df_clock_cz.3, by = join_by(rp_of_weekblock == bc_week_of_month,
  #                                         rp_of_day == bc_day_label,
  #                                         rp_of_start == bc_hour_start))
  #   # rename(ts = rp_on_ts, bc_day_label = rp_on_day, bc_hour_start = rp_on_start)
  #   
  # df_clock_cz.5 <- df_clock_cz.4 |> select(ts, bc_day_label, bc_hour_start, mr_key, bc_minutes, rp_formula = hh_formule) |> 
  #   bind_rows(df_replays) |> arrange(ts, rp_of_ts) |> group_by(ts) |> mutate(rn = row_number()) |> ungroup() |> 
  #   filter(rn == 1 & !is.na(mr_key)) |> select(-rn)
  
  # link clock details ----
  df_clock_cz.6 <- df_clock_cz.5 |> 
    left_join(df_raw_wpgidsinfo, by = join_by(bc_clock_key == `key-modelrooster`)) |> 
    select(bc_ts:is_rp, 
           titel_NL = `titel-NL`,
           titel_EN = `titel-EN`,
           productie = `productie-1-taak`,
           redacteurs = `productie-1-mdw`,
           genre_1 = `genre-1-NL`,
           genre_2 = `genre-2-NL`,
           intro_NL = `std.samenvatting-NL`,
           intro_EN = `std.samenvatting-EN`,
           afbeelding = feat_img_ids,
           woj_bcid,
           nipper_mogelijk) |> 
    pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  uniques_titles_cz <- df_clock_cz.6 |> select(titel_NL, titel_EN) |> distinct() |> arrange(titel_NL)

  # validate gids-info ----
  missing_gi <- df_clock_cz.6 |> filter(is.na(titel_NL)) |> nrow()
  
  if (missing_gi > 0) {
    flog.error("clock details are incomplete; quiting this job.", name = "clof")
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
  df_clock_cz.7 <- df_clock_cz.6 |> left_join(ty_genres, by = join_by(genre == genre_NL))
  n_missing <- df_clock_cz.7 |> filter(is.na(ty_genre_id)) |> nrow()
  
  if (n_missing > 0) {
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
            order by 1
            ;"
  ty_editors <- dbGetQuery(con, query)
  df_clock_cz.8 <- df_clock_cz.7 |> left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  
  # . check complete ----
  n_missing <- df_clock_cz.8 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct() |> nrow()
  
  if (n_missing > 0) {
    flog.error("editors missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # images ----
  df_afb <- df_clock_cz.8 |> filter(!is.na(afbeelding)) |> select(afbeelding) |> distinct() |> 
    mutate(afbeelding = as.integer(afbeelding), image_id = NA_character_) |> arrange(afbeelding)
  
  for (rn in seq_len(nrow(df_afb))) {
    df_image <- cpnm_img_get(pm_img_id = df_afb$afbeelding[rn], pm_cpnm_db = con)
    
    if (is.na(df_image$id)) {
      next
    }
    
    df_afb$image_id[rn] <- df_image$id
  }
  
  # . check complete ----
  df_missing <- df_afb |> filter(is.na(image_id))
  
  if (nrow(df_missing) > 0) {
    flog.error("images are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # . append ----
  df_clock_cz.9 <- df_clock_cz.8 |> left_join(df_afb, by = join_by(afbeelding))
  
  # programs ----
  query <- "WITH counts AS (
                SELECT LOWER(p.title->>'$.nl') AS titel_nl_lc,
                       p.id AS pgm_id,
                       COUNT(*) AS n_bcs
                FROM entries p JOIN entries e ON e.parent_id = p.id
                                             AND e.deleted_at IS NULL
                               JOIN entries b ON b.parent_id = e.id
                                             AND b.deleted_at IS NULL
                WHERE p.type = 'program'
                  AND p.site_id IN (1, 2)
                  AND p.deleted_at IS NULL
                GROUP BY LOWER(p.title->>'$.nl'),
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
  
  df_clock_cz.10 <- df_clock_cz.9 |> left_join(program_titles, by = join_by("titel_nl_lc"))
  n_missing <- df_clock_cz.10 |> filter(is.na(pgm_id)) |> nrow()
  
  # . check complete ----
  if (n_missing > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = "clof")
    break
  }
  
  # . check length ----
  wi_tot_minutes <- df_clock_cz.10 |> distinct(bc_ts, slot_minutes) |> mutate(total_minutes = sum(slot_minutes)) |> 
    head(1) |> select(total_minutes) |> pull()
  
  if (wi_tot_minutes != 10080L) {
    flog.error(str_glue("invalid clock length (2): expected 10080 minutes, but got {wi_tot_minutes}; quiting this job."), name = "clof")
    break
  }

  # remove locked slots ----
  df_locked <- locked_slots(pm_start = start_ts, pm_stop = stop_ts, pm_db = con) |> 
    mutate(locked_slot_ts = ymd_hms(locked_slot, quiet = T, tz = tz_am), .keep = "none")
  df_clock_cz.11 <- df_clock_cz.10 |> anti_join(df_locked, by = join_by(bc_ts == locked_slot_ts))
  
  # add originals ----
  # all replays within the current week need to exist as originals first!
  df_clock_cz.12 <- df_clock_cz.11 |> filter(!is_rp)
  job_id <- UUIDgenerate(use.time = FALSE) 
  flog.info(str_glue("running job-id = {job_id}"), name = "clof")
  func_result <- clock2db(pm_clock_tib = df_clock_cz.12, pm_job_id = job_id, pm_db = con)
  
  # . check bc-count ----
  n_bcs_expected <- df_clock_cz.12 |> select(bc_ts) |> distinct() |> nrow()
  sql_stmt <- glue_sql("SELECT count(distinct(dates->>'$.start')) as n
                        FROM entries
                        WHERE attributes->>'$.job_id' = {job_id}
                          and type = 'broadcast';", .con = con)
  n_bcs_added <- dbGetQuery(con, sql_stmt)
  
  if (n_bcs_added$n != n_bcs_expected) {
    flog.error(str_glue("adding originals failed: expected {n_bcs_expected}, but got {n_bcs_added}; quiting this job."), 
               name = "clof")
    break
  }
  
  # Exit from MCL
  break
}

# TOT HIER ----

# . get the replays ----
df_clock_cz.13 <- df_clock_cz.11 |> filter(is_rp)


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
flog.info("Clockfactory CZ: job finished", name = "clof")
