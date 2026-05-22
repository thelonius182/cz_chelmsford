# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# prep WoJ programme clock + catalogue to link them to CPNM-id's
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

cz_site_id <- 1L
wj_site_id <- 2L
config <- read_yaml("config.yaml")
source("R/custom_functions.R", encoding = "UTF-8")

# init logger ----
apf <- flog.appender(appender.file(config$log_appender_file_wj), "clof")

# Current job date (Amsterdam)
tz_am <- "Europe/Amsterdam"
now_am <- with_tz(Sys.time(), tz_am)

# Calculate when to start: first Thursday 13:00:00 after current job date
candidate <- update(now_am, hour = 13, minute = 0, second = 0)
days_ahead <- (4 - wday(candidate, week_start = 1)) %% 7  # 4 = Thursday (Mon=1)

if (days_ahead == 0 && candidate <= now_am) {
  days_ahead <- 7
}

start_ts <- candidate + days(days_ahead)
end_ts   <- start_ts + days(7)
flog.info(str_glue("Building a clock for the WJ-week starting {logfmt_ts(start_ts)}"), name = "clof")

bc_week_ts <- tibble(
  ts = seq(from = start_ts, to = end_ts, by = "hour")
)

bc_week <- add_bc_cols(bc_week_ts, ts)

# downloads GD ----
# . trigger GD-auth
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# . get WoJ clock matrix from GD
path_rooster_woj <- "/home/lon/R_projects/cz_chelmsford/resources/rooster_woj.xlsx"
drive_download(file = cz_get_url("rooster_woj"), overwrite = T, path = path_rooster_woj)

# . get catalogue from GD
path_wp_gidsinfo <- "/home/lon/R_projects/cz_chelmsford/resources/wordpress_gidsinfo.xlsx"
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_wp_gidsinfo)

# LaCies
path_gd_lacie <- "/home/lon/R_projects/cz_chelmsford/resources/lacie.xlsx"
drive_download(file = cz_get_url("lacie"), overwrite = T, path = path_gd_lacie)

# sheets as df -----------------------------------------------------------
tbl_raw_zenderschema_woj <- cz_extract_sheet(path_rooster_woj, sheet_name = "schedule_woj") |> select(-parent)
tbl_zenderschema_woj <- tbl_raw_zenderschema_woj |> mutate(start = as.integer(start))
tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")
tbl_raw_lacie <- cz_extract_sheet(path_gd_lacie, sheet_name = "woj_herhalingen_4.2")

unique_titles_wj <- tbl_zenderschema_woj |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
  rename(genre_1 = `genre-1-NL`, genre_2 = `genre-2-NL`) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
  select(titel_NL = `titel-NL`, titel_EN = `titel-EN`) |> distinct() |> arrange(titel_NL)

# uniques_titles <- bind_rows(uniques_titles_cz, unique_titles_wj) |> distinct() |> arrange(titel_NL)

# combine week and schedule -----------------------------------------------
woj_schedule <- bc_week |> 
  left_join(tbl_zenderschema_woj, 
            by = join_by(bc_day_label == day, 
                         bc_week_of_month == week_vd_mnd, 
                         bc_hour_start == start)) |> 
  filter(!is.na(slot_id))

woj_schedule_w_ids.1 <- woj_schedule |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
  rename(genre_1 = `genre-1-NL`, genre_2 = `genre-2-NL`) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) 

woj_schedule_w_ids.2 <- woj_schedule_w_ids.1 |> 
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

source("R/cpnm_db_setup.R", encoding = "UTF-8")  

# > Main Control Loop ----
repeat {
  # validate gids-info ----
  missing_gi <- woj_schedule_w_ids.1 |> filter(is.na(`key-modelrooster`)) |> nrow()
  
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
  woj_schedule_w_ids.3 <- woj_schedule_w_ids.2 |> left_join(ty_genres, by = join_by(genre == genre_NL))
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.3 |> filter(is.na(ty_genre_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
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
  woj_schedule_w_ids.4 <- woj_schedule_w_ids.3 |> 
    left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.4 |> filter(is.na(ty_editor_id)) |> select(redacteurs) |> distinct()
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
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
  woj_schedule_w_ids.5 <- woj_schedule_w_ids.4 |> 
    left_join(program_titles, by = join_by("titel_nl_lc"))
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.5 |> filter(is.na(pgm_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
    flog.error("programs missing in 'entries'; quiting this job.", name = "clof")
    break
  }
  
  # check length ----
  wi_tot_minutes <- woj_schedule_w_ids.5 |> distinct(bc_start, minutes) |> mutate(total_minutes = sum(minutes)) |> 
    head(1) |> select(total_minutes) |> pull()
  
  if (wi_tot_minutes != 10080L) {
    flog.error(str_glue("woj_schedule: expected 10080 minutes, but got {wi_tot_minutes}; quiting this job."), name = "clof")
    break
  }
  
  # LaCie ----
  # these are archived CZ-programs kept on external hard drives (made by the LaCie company) that get a replay on WJ. 
  tbl_lacie <- tbl_raw_lacie |> filter(!is.na(bc_woj_ts)) |>  # remove fully empty lines
    mutate(bc_woj_ts = force_tz(bc_woj_ts, tzone = "Europe/Amsterdam"),
           replay_of = force_tz(replay_of, "Europe/Amsterdam"))
  woj_schedule_w_ids.6 <- woj_schedule_w_ids.5 |> left_join(tbl_lacie, by = join_by(bc_start == bc_woj_ts)) |> 
    select(bc_start:pgm_id, replay_of_ts = replay_of) |> mutate(replay_of_epi_id = NA_character_)
  fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
  
  for (rn in seq_len(nrow(woj_schedule_w_ids.6))) {
    
    if (woj_schedule_w_ids.6$broadcast_type[rn] != "LaCie") {
      next
    }
      
    if (is.na(woj_schedule_w_ids.6$replay_of_ts[rn])) {
      next
    }

    df_replay <- cpnm_epi_get(pm_pgm_id = woj_schedule_w_ids.6$pgm_id[rn],
                              pm_start = fmt_ts(woj_schedule_w_ids.6$replay_of_ts[rn]),
                              pm_cpnm_db = con)
    
    if (is.na(df_replay$epi_id)) {
      next
    }
    
    woj_schedule_w_ids.6$replay_of_epi_id[rn] <- df_replay$epi_id
  }
  
  # . check complete ----
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.6 |> filter(broadcast_type == "LaCie" & is.na(replay_of_epi_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
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
  for (rn in seq_len(nrow(woj_schedule_w_ids.6))) {
    
    if (woj_schedule_w_ids.6$broadcast_type[rn] != "Universe") {
      next
    }

    pm_max_start <- if (woj_schedule_w_ids.6$production_type[rn] == "live") start_ts else woj_schedule_w_ids.6$bc_start[rn]
    df_replay <- cpnm_uni_get(pm_pgm_id = woj_schedule_w_ids.6$pgm_id[rn],
                              pm_max_start,
                              pm_cpnm_db = con)

    if (is.na(df_replay$epi_id)) {
      next
    }
    
    woj_schedule_w_ids.6$replay_of_epi_id[rn] <- df_replay$epi_id
    woj_schedule_w_ids.6$replay_of_ts[rn] <- df_replay$epi_start
  }
  
  # . check complete ----
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.6 |> filter(broadcast_type == "Universe" & is.na(replay_of_epi_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
    flog.error("Universe-replays are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # Replays ----
  # - these are replays of native WJ-programs on WJ, but otherwise this works just like Universe
  for (rn in seq_len(nrow(woj_schedule_w_ids.6))) {
    
    if (woj_schedule_w_ids.6$broadcast_type[rn] != "ReplayWoJ") {
      next
    }
    
    df_replay <- cpnm_uni_get(pm_pgm_id = woj_schedule_w_ids.6$pgm_id[rn],
                              pm_max_start = woj_schedule_w_ids.6$bc_start[rn],
                              pm_cpnm_db = con)
    
    if (is.na(df_replay$epi_id)) {
      next
    }
    
    woj_schedule_w_ids.6$replay_of_epi_id[rn] <- df_replay$epi_id
    woj_schedule_w_ids.6$replay_of_ts[rn] <- df_replay$epi_start
  }
  
  # . check complete ----
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.6 |> filter(broadcast_type == "Universe" & is.na(replay_of_epi_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
    flog.error("WorldOfJazz-replays are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # Images ----
  df_afb <- woj_schedule_w_ids.6 |> filter(!is.na(afbeelding)) |> select(afbeelding) |> distinct() |> 
    mutate(afbeelding = as.integer(afbeelding), image_id = NA_character_) |> arrange(afbeelding)
  
  for (rn in seq_len(nrow(df_afb))) {
    df_image <- cpnm_img_get(pm_img_id = df_afb$afbeelding[rn], pm_cpnm_db = con)
    
    if (is.na(df_image$id)) {
      next
    }
    
    df_afb$image_id[rn] <- df_image$id
  }
  
  # . check complete ----
  woj_schedule_w_ids_missing <- df_afb |> filter(is.na(image_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
    flog.error("WorldOfJazz images are incomplete; quiting this job.", name = "clof")
    break
  }
  
  # . append ----
  woj_schedule_w_ids.6 <- woj_schedule_w_ids.6 |> left_join(df_afb, by = join_by(afbeelding))
  
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
  woj_schedule_w_ids.7 <- woj_schedule_w_ids.6 |> group_by(bc_start) |> mutate(sch_item = row_number()) |> ungroup()
  woj_schedule_w_ids.7a <- woj_schedule_w_ids.7 |> filter(sch_item == 1)
  woj_schedule_w_ids.7b <- woj_schedule_w_ids.7 |> filter(sch_item == 2)
  
  for (rn in seq_len(nrow(woj_schedule_w_ids.7a))) {
    # temp exception
    if (rn %in% c(1, 2)) next
    # temp exception
    if (is.na(woj_schedule_w_ids.7a$replay_of_epi_id[rn])) {
      # . fresh episode & broadcast ----
      fresh_epi_bc <- cpnm_epi_bc_ins(pm_pgm_id = woj_schedule_w_ids.7a$pgm_id[rn],
                                      pm_descr_NL = woj_schedule_w_ids.7a$intro_NL[rn],
                                      pm_descr_EN = woj_schedule_w_ids.7a$intro_EN[rn],
                                      pm_img_id = woj_schedule_w_ids.7a$image_id[rn],
                                      pm_site_id = wj_site_id,
                                      pm_bc_start = woj_schedule_w_ids.7a$bc_start[rn],
                                      pm_bc_minutes = woj_schedule_w_ids.7a$minutes[rn],
                                      pm_cpnm_db = con)
      # . genre ----
      # - add an `episode` taxonomable record for first genre
      txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                              pm_txy_id = woj_schedule_w_ids.7a$ty_genre_id[rn],
                              pm_order = 1,
                              pm_cpnm_db = con)
      
      # - add an `episode` taxonomable record for second genre
      df_g2 <- woj_schedule_w_ids.7b |> filter(bc_start == woj_schedule_w_ids.7a$bc_start[rn])
      
      if (nrow(df_g2) == 1) {
        txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                                pm_txy_id = df_g2$ty_genre_id,
                                pm_order = 2,
                                pm_cpnm_db = con)
      }
      
      # . editor ----
      # - add an `episode` taxonomable record for editors and production-role (txy-type colofon)
      txb_res <- cpnm_txb_edi_ins(pm_epi_id = fresh_epi_bc,
                                  pm_txy_id = woj_schedule_w_ids.7a$ty_editor_id[rn],
                                  pm_role_NL = woj_schedule_w_ids.7a$production_role[rn],
                                  pm_cpnm_db = con)
    } else {
      # . replay ----
      bc_replay_res <- cpnm_bc_ins(pm_pgm_id = woj_schedule_w_ids.7a$pgm_id[rn],
                                   pm_epi_id = woj_schedule_w_ids.7a$replay_of_epi_id[rn],
                                   pm_site_id = wj_site_id,
                                   pm_bc_start = woj_schedule_w_ids.7a$bc_start[rn],
                                   pm_bc_minutes = woj_schedule_w_ids.7a$minutes[rn],
                                   pm_cpnm_db = con)
    }
  }
   
  # exit MCL
  break
}

# Cleanup ----
dbDisconnect(con)
close_tunnel(tunnel)
flog.info("Clockfactory WJ: job finished", name = "clof")
