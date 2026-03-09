# prep modelrooster en wp-gidsinfo tbv de koppeling met CPNM-id's
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
source("R/custom_functions.R", encoding = "UTF-8")

tz_am <- "Europe/Amsterdam"

# Current job date (Amsterdam)
now_am <- with_tz(Sys.time(), tz_am)

# First Thursday 13:00:00 Amsterdam time strictly after current job date
candidate <- update(now_am, hour = 13, minute = 0, second = 0)

days_ahead <- (4 - wday(candidate, week_start = 1)) %% 7  # 4 = Thursday (Mon=1)
if (days_ahead == 0 && candidate <= now_am) {
  days_ahead <- 7
}

start_ts <- candidate + days(days_ahead)
end_ts   <- start_ts + days(7)

bc_week_ts <- tibble(
  ts = seq(from = start_ts, to = end_ts, by = "hour")
)

bc_week <- add_bc_cols(bc_week_ts, ts)

# downloads GD ------------------------------------------------------------

# trigger GD-auth
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# Uitzendschema WoJ ophalen bij GD
path_rooster_woj <- "/home/lon/R_projects/cz_chelmsford/resources/rooster_woj.xlsx"
drive_download(file = cz_get_url("rooster_woj"), overwrite = T, path = path_rooster_woj)

# WP-gids-info ophalen bij GD
path_wp_gidsinfo <- "/home/lon/R_projects/cz_chelmsford/resources/wordpress_gidsinfo.xlsx"
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_wp_gidsinfo)

# LaCies
path_gd_lacie <- "/home/lon/R_projects/cz_chelmsford/resources/lacie.xlsx"
drive_download(file = cz_get_url("lacie"), overwrite = T, path = path_gd_lacie)

# sheets als df -----------------------------------------------------------
tbl_raw_zenderschema_woj <- cz_extract_sheet(path_rooster_woj, sheet_name = "schedule_woj") |> select(-parent)
tbl_zenderschema_woj <- tbl_raw_zenderschema_woj |> mutate(start = as.integer(start))
tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")
tbl_raw_lacie <- cz_extract_sheet(path_gd_lacie, sheet_name = "woj_herhalingen_4.2")

unique_titles_wj <- tbl_zenderschema_woj |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
  rename(genre_1 = `genre-1-NL`, genre_2 = `genre-2-NL`) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
  select(titel_NL = `titel-NL`, titel_EN = `titel-EN`) |> distinct() |> arrange(titel_NL)

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
         productie = `productie-1-taak`,
         redacteurs = `productie-1-mdw`,
         genre,
         intro_NL = `std.samenvatting-NL`,
         intro_EN = `std.samenvatting-EN`,
         broadcast_type,
         afbeelding = feat_img_ids) 

source("R/cpnm_db_setup.R", encoding = "UTF-8")  

# Main Control Loop
repeat {
  # validate gids-info ----
  missing_gi <- woj_schedule_w_ids.1 |> filter(is.na(`key-modelrooster`)) |> nrow()
  
  if (missing_gi > 0) {
    print("gidsinfo is incomplete; quiting this job.")
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
    print("genres missing in the taxonomy; quiting this job.")
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
    print("editors missing in the taxonomy; quiting this job.")
    break
  }
  
  # programs ----
  query <- "with ds1 as (
   select replace(pgms.title->>'$.nl', '&amp;', '&') as pgm_title_NL, 
          pgms.id as pgm_id
   from entries pgms left join entries epis on epis.parent_id = pgms.id
                                           and pgms.type = 'program'
                                           and epis.type = 'episode'
   where length(pgms.title->>'$.nl') > 0
   ), ds2 as (
   select pgm_title_NL, pgm_id, count(*) as n_episodes 
   from ds1
   group by pgm_title_NL,
            pgm_id
   ), ds3 as (
   select pgm_title_NL, 
          pgm_id, 
          n_episodes,
          ROW_NUMBER() OVER (PARTITION BY pgm_title_NL
                             ORDER BY n_episodes desc) AS rn
   from ds2
   )
   select * from ds3  where rn = 1 
   order by 1;"
  program_titles <- dbGetQuery(con, query) |> select(pgm_title_NL, pgm_id)
  woj_schedule_w_ids.5 <- woj_schedule_w_ids.4 |> 
    left_join(program_titles, by = join_by("titel_NL" == "pgm_title_NL"))
  woj_schedule_w_ids_missing <- woj_schedule_w_ids.5 |> filter(is.na(pgm_id))
  
  if (nrow(woj_schedule_w_ids_missing) > 0) {
    print("programs missing in 'entries'; quiting this job.")
    break
  }
  
  # check length ----
  wi_tot_minutes <- woj_schedule_w_ids.5 |> distinct(bc_start, minutes) |> mutate(total_minutes = sum(minutes)) |> 
    head(1) |> select(total_minutes) |> pull()
  
  if (wi_tot_minutes != 10080L) {
    print(str_glue("woj_schedule: expected 10080 minutes, but got {wi_tot_minutes}; quiting this job."))
    break
  }
  
  # LaCie ----
  tbl_lacie <- tbl_raw_lacie |> filter(!is.na(bc_woj_ts)) |>  # remove empty lines
    mutate(bc_woj_ts = force_tz(bc_woj_ts, tzone = "Europe/Amsterdam"),
           replay_of = force_tz(replay_of, "Europe/Amsterdam"))
  woj_schedule_w_ids.6 <- woj_schedule_w_ids.5 |> left_join(tbl_lacie, by = join_by(bc_start == bc_woj_ts)) |> 
    select(bc_start:pgm_id, replay_of)
  
  # Universe ----
  tbl_uni <- woj_schedule_w_ids.6 |> filter(broadcast_type == "Universe")
  
  for (rn in seq_len(nrow(tbl_uni))) {
    qry <- glue_sql("select * from entries where id = {tbl_uni$pgm_id[rn]};", .con = con)
    uni_pgms <- dbGetQuery(con, qry)
  }
    
  # Replays ----

  # exit MCL
  break
}

# Cleanup
dbDisconnect(con)
close_tunnel(tunnel)
