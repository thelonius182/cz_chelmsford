# - - - - - - - - - - - - - - - - - - -
# Build this week's CZ programme clock
# - - - - - - - - - - - - - - - - - - -

# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)
config <- read_yaml("config.yaml")
apf <- flog.appender(appender.file(config$log_appender_file_cz), "clof")
flog.info("Building a programme clock for CZ", name = "clof")
cz_site_id <- 1L
wj_site_id <- 2L
source("R/custom_functions.R", encoding = "UTF-8")

# > Main Control Loop ----
repeat {
  # connect to DB
  source("R/cpnm_db_setup.R", encoding = "UTF-8")  
  
  # Find latest week ----
  # ... available on the site, to know where to start the next one
  # `latest week`: 19:00-slots of the latest 7 days on the site
  df_latest_week <- latest_week(pm_cpnm_db = con)
  
  # expect 7 consecutive dates without gaps or duplicates
  n_rows <- nrow(df_latest_week)
  if (n_rows != 7) {
    flog.error("finding latest week failed: list is incomplete", name = "clof")
    break
  }
  
  has_gaps <- df_latest_week |> filter(diff_days != 1) |> nrow()
  if (has_gaps > 0) {
    flog.error("finding latest week failed: invalid date sequence", name = "clof")
    break
  }
  
  # assign start/stop-of-week ----
  tz_am <- "Europe/Amsterdam"
  start_ts <- force_tz(df_latest_week$d[1] + days(1), tzone = tz_am) |> update(hour = 13, minute = 0, second = 0)
  stop_ts   <- start_ts + days(7)
  flog.info(str_glue("Clock will run Thursday {logfmt_ts(start_ts)} to {logfmt_ts(stop_ts)}"), name = "clof")
  
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

  # sheets as df -----------------------------------------------------------
  tbl_raw_zenderschema_cz <- cz_extract_sheet(path_rooster_cz, sheet_name = paste0("modelrooster-", config$modelrooster_versie))
  df_clock_cz_cz <- tbl_raw_zenderschema_cz |> mutate(start = as.integer(start))
  tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")
  
  df_clock_cz.1 <- df_clock_cz_cz |> 
    mutate(start = str_pad(string = start, side = "left", width = 5, pad = "0"), 
           slot = paste0(str_sub(dag, start = 1, end = 2), start)
    ) |> 
    rename(hh_formule = `hhOffset-dag.uur`,
           wekelijks = `elke week`,
           AB_cyclus = `twee-wekelijks`,
           cyclus_A = A,
           cyclus_B = B,
           week_1 = `week 1`,
           week_2 = `week 2`,
           week_3 = `week 3`,
           week_4 = `week 4`,
           week_5 = `week 5`
    ) 
  
  df_clock_cz.2 <- df_clock_cz.1 |>
    select(-starts_with("r"), -starts_with("b"), -starts_with("t", ignore.case = F), -dag, -start, -Toon) |>  
    select(slot, hh_formule, everything()) |> 
    pivot_longer(names_to = "wanneer", cols = starts_with("week_"), values_to = "mr_key") 
  
  cur_week_label <- week_label(date(start_ts))
  
  df_clock_cz.3 <- df_clock_cz.2 |> 
    mutate(mr_key = if_else(!is.na(mr_key), mr_key, if_else(!is.na(wekelijks), wekelijks, AB_cyclus)),
           bc_minutes = as.integer(str_extract(slot, "\\d{3}$")),
           bc_day_label = str_extract(slot, "^.."),
           bc_week_of_month = as.integer(str_extract(wanneer, "\\d$")),
           bc_hour_start = as.integer(str_extract(slot, "^..(\\d{2})", group = 1)),
           cycle_vec = cur_week_label,
           bc_cycle = if_else(cycle_vec == "A", cyclus_A, cyclus_B)) |> 
    select(mr_key, starts_with("bc_"), hh_formule)
  
  # combine week and schedule ----
  df_clock_cz.4 <- bc_week |> left_join(df_clock_cz.3, 
                                      by = join_by(bc_day_label, bc_week_of_month, bc_hour_start)) |> 
    mutate(hh_start = as.integer(str_extract(hh_formule, "(\\d{2}).$", group = 1)),
           hh_start = if_else(hh_formule == "tw", bc_hour_start, hh_start),
           hh_day = str_extract(hh_formule, "^..(..)", group = 1),
           hh_day = if_else(hh_formule == "tw", bc_day_label, hh_day),
           hh_offset = case_when(is.na(hh_formule) ~ NA_integer_,
                                 hh_formule == "tw" ~ 7L,
                                 TRUE ~ as.integer(str_extract(hh_formule, "^\\d{2}"))))
  cz_weekdays <- c("ma", "di", "wo", "do", "vr", "za", "zo")
  df_replays <- df_clock_cz.4 |> filter(!is.na(hh_formule) & (bc_cycle == "h" | is.na(bc_cycle))) |> 
    select(replay_on_day = hh_day, replay_on_start = hh_start) |> 
    arrange(factor(replay_on_day, levels = cz_weekdays, ordered = TRUE))

  df_replays_to_add <- df_replays |> 
    left_join(df_clock_cz.4, by = join_by(replay_on_day == bc_day_label, replay_on_start == bc_hour_start))

  df_clock_cz.5 <- df_clock_cz.4 |> 
    left_join(tbl_raw_wpgidsinfo, by = join_by(mr_key == `key-modelrooster`)) |> 
    select(moro_key = mr_key,
           woj_bcid,
           titel_NL = `titel-NL`,
           titel_EN = `titel-EN`,
           productie = `productie-1-taak`,
           redacteurs = `productie-1-mdw`,
           genre_1 = `genre-1-NL`,
           genre_2 = `genre-2-NL`,
           intro_NL = `std.samenvatting-NL`,
           intro_EN = `std.samenvatting-EN`,
           afbeelding = feat_img_ids
    ) |> pivot_longer(cols = c(genre_1, genre_2), 
                      names_to = NULL, 
                      values_to = "genre", 
                      values_drop_na = TRUE) |> arrange(moro_key) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  uniques_titles_cz <- df_clock_cz.4 |> select(titel_NL, titel_EN) |> distinct() |> arrange(titel_NL)
  
  # unique_titles_wj <- df_clock_cz_woj |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
  #   rename(genre_1 = `genre-1-NL`, genre_2 = `genre-2-NL`) |> 
  #   pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
  #   select(titel_NL = `titel-NL`, titel_EN = `titel-EN`) |> distinct() |> arrange(titel_NL)
  
  # uniques_titles <- bind_rows(uniques_titles_cz, unique_titles_wj) |> distinct() |> arrange(titel_NL)
  
  cz_schedule_w_ids.1 <- cz_schedule |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
    rename(genre_1 = `genre-1-NL`, genre_2 = `genre-2-NL`) |> 
    pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) 
  
  cz_schedule_w_ids.2 <- cz_schedule_w_ids.1 |> 
    select(bc_start = ts,
           minutes,
           titel_NL = `titel-NL`,
           titel_EN = `titel-EN`,
           redacteurs = `productie-1-mdw`,
           production_role = `productie-1-taak`,
           genre,
           intro_NL = `std.samenvatting-NL`,
           intro_EN = `std.samenvatting-EN`,
           production_type = uitzendtype,
           broadcast_type,
           afbeelding = feat_img_ids) |> 
    mutate(titel_nl_lc = str_to_lower(titel_NL))
  
  # validate gids-info ----
  missing_gi <- cz_schedule_w_ids.1 |> filter(is.na(`key-modelrooster`)) |> nrow()
  
  if (missing_gi > 0) {
    flog.error("gidsinfo is incomplete; quiting this job.", name = "clof")
    break
  }
  
  # genres ----
  query <- "select name->>'$.nl' as genre_NL, 
                   id as ty_genre_id 
            from taxonomies 
            where type = 'genre' 
              and site_id in (1, 2)
              and name->>'$.nl' not in ('Algemeen', 'World of Jazz')
            order by 1
            ;"
  
  ty_genres <- dbGetQuery(con, query)
  cz_schedule_w_ids.3 <- cz_schedule_w_ids.2 |> left_join(ty_genres, by = join_by(genre == genre_NL))
  cz_schedule_w_ids_missing <- cz_schedule_w_ids.3 |> filter(is.na(ty_genre_id))
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("genres missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # editors ----
  query <- "select name->>'$.nl' as editor_name, 
                   id as ty_editor_id
            from taxonomies 
            where type = 'colofon'
            order by 1
            ;"
  ty_editors <- dbGetQuery(con, query)
  cz_schedule_w_ids.4 <- cz_schedule_w_ids.3 |> 
    left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  cz_schedule_w_ids_missing <- cz_schedule_w_ids.4 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct()
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("editors missing in the taxonomy; quiting this job.", name = "clof")
    break
  }
  
  # programs ----
  query <- "with ds1 as (
       select lower(p.title->>'$.nl') as titel_nl_lc, 
              p.id as pgm_id,
              b.dates
       from entries p join entries e on e.parent_id = p.id
                      join entries b on b.parent_id = e.id
       where p.type = 'program' 
  ), ds2 as (
       select titel_nl_lc, pgm_id, count(*) as n_bcs
       from ds1
       group by titel_nl_lc, pgm_id
  ), ds3 as (
       select ds2.*,
       ROW_NUMBER() OVER (PARTITION BY titel_nl_lc
   	    			     ORDER BY n_bcs desc) AS rn
       from ds2
  )
  select titel_nl_lc, pgm_id from ds3 where rn = 1 order by 1;"
  program_titles <- dbGetQuery(con, query)
  cz_schedule_w_ids.5 <- cz_schedule_w_ids.4 |> 
    left_join(program_titles, by = join_by("titel_nl_lc"))
  cz_schedule_w_ids_missing <- cz_schedule_w_ids.5 |> filter(is.na(pgm_id))
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = "clof")
    break
  }
  
  # check length ----
  wi_tot_minutes <- cz_schedule_w_ids.5 |> distinct(bc_start, minutes) |> mutate(total_minutes = sum(minutes)) |> 
    head(1) |> select(total_minutes) |> pull()
  
  if (wi_tot_minutes != 10080L) {
    flog.error(str_glue("cz_schedule: expected 10080 minutes, but got {wi_tot_minutes}; quiting this job."), name = "clof")
    break
  }
  
  # LaCie ----
  # these are archived CZ-programs kept on external hard drives (made by the LaCie company) that get a replay on WJ. 
  tbl_lacie <- tbl_raw_lacie |> filter(!is.na(bc_woj_ts)) |>  # remove fully empty lines
    mutate(bc_woj_ts = force_tz(bc_woj_ts, tzone = "Europe/Amsterdam"),
           replay_of = force_tz(replay_of, "Europe/Amsterdam"))
  cz_schedule_w_ids.6 <- cz_schedule_w_ids.5 |> left_join(tbl_lacie, by = join_by(bc_start == bc_woj_ts)) |> 
    select(bc_start:pgm_id, replay_of_ts = replay_of) |> mutate(replay_of_epi_id = NA_character_)
  fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
  
  for (rn in seq_len(nrow(cz_schedule_w_ids.6))) {
    
    if (cz_schedule_w_ids.6$broadcast_type[rn] != "LaCie") {
      next
    }
    
    if (is.na(cz_schedule_w_ids.6$replay_of_ts[rn])) {
      next
    }
    
    df_replay <- cpnm_epi_get(pm_pgm_id = cz_schedule_w_ids.6$pgm_id[rn],
                              pm_start = fmt_ts(cz_schedule_w_ids.6$replay_of_ts[rn]),
                              pm_cpnm_db = con)
    
    if (is.na(df_replay$epi_id)) {
      next
    }
    
    cz_schedule_w_ids.6$replay_of_epi_id[rn] <- df_replay$epi_id
  }
  
  # . check complete ----
  cz_schedule_w_ids_missing <- cz_schedule_w_ids.6 |> filter(broadcast_type == "LaCie" & is.na(replay_of_epi_id))
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("LaCie-replays are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # Universe ----
  # - these are recent CZ-programs that get a replay on WJ (after being broadcast on CZ in the last month or so). 
  #   This week's Live CZ's can´t be replayed on WJ until next week, as they need prepping first, next Thursday. 
  #   For the current week that means `max_start` for live CZ's is the start of this broacast-week: `start_ts` 
  #                                   `max_start` for other CZ's is the start of the respective WJ-slot: `bc_start`
  # NB.1 - production-type = e.g. upload, montage, live
  #        broadcast-type  = LaCie, NonStop, Universe, WorldOfJazz, ReplayWoJ
  # NB.2 - for Universe broadcasts, the production type is the type on CZ, not the one on WJ
  for (rn in seq_len(nrow(cz_schedule_w_ids.6))) {
    
    if (cz_schedule_w_ids.6$broadcast_type[rn] != "Universe") {
      next
    }
    
    pm_max_start <- if (cz_schedule_w_ids.6$production_type[rn] == "live") start_ts else cz_schedule_w_ids.6$bc_start[rn]
    df_replay <- cpnm_uni_get(pm_pgm_id = cz_schedule_w_ids.6$pgm_id[rn],
                              pm_max_start,
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
    flog.error("Universe-replays are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # Replays ----
  # - these are replays of native WJ-programs on WJ, but otherwise this works just like Universe
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
  
  # Images ----
  df_afb <- cz_schedule_w_ids.6 |> filter(!is.na(afbeelding)) |> select(afbeelding) |> distinct() |> 
    mutate(afbeelding = as.integer(afbeelding), image_id = NA_character_) |> arrange(afbeelding)
  
  for (rn in seq_len(nrow(df_afb))) {
    df_image <- cpnm_img_get(pm_img_id = df_afb$afbeelding[rn], pm_cpnm_db = con)
    
    if (is.na(df_image$id)) {
      next
    }
    
    df_afb$image_id[rn] <- df_image$id
  }
  
  # . check complete ----
  cz_schedule_w_ids_missing <- df_afb |> filter(is.na(image_id))
  
  if (nrow(cz_schedule_w_ids_missing) > 0) {
    flog.error("WorldOfJazz images are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # . append ----
  cz_schedule_w_ids.6 <- cz_schedule_w_ids.6 |> left_join(df_afb, by = join_by(afbeelding))
  
  # check latest slot ----
  df_slots <- cpnm_chk_slots(pm_site_id = 2, pm_cpnm_db = con) |> 
    mutate(max_start_cz = ymd_hms(max_start_cz, tz = "Europe/Amsterdam"))
  
  if (df_slots$max_start_cz >= start_ts) {
    flog.error("Not all required slots are free; quiting this job.", name = "clof")
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
}

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("Clockfactory CZ: job finished", name = "clof")
