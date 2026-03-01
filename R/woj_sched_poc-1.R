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

# sheets als df -----------------------------------------------------------
tbl_raw_zenderschema_woj <- cz_extract_sheet(path_rooster_woj, sheet_name = "schedule_woj") |> select(-parent)
tbl_zenderschema_woj <- tbl_raw_zenderschema_woj |> mutate(start = as.integer(start))
tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")

# combine week and schedule -----------------------------------------------
woj_schedule <- bc_week |> 
  left_join(tbl_raw_zenderschema_woj, 
            by = join_by(bc_day_label == day, 
                         bc_week_of_month == week_vd_mnd, 
                         bc_hour_start == start)) |> 
  filter(!is.na(slot_id))

woj_schedule_w_ids.1 <- woj_schedule |> left_join(tbl_raw_wpgidsinfo, by = join_by(broadcast_id == woj_bcid)) |> 
  rename(titel_NL = `titel-NL`,
         titel_EN = `titel-EN`,
         productie = `productie-1-taak`,
         redacteurs = `productie-1-mdw`,
         genre_1 = `genre-1-NL`,
         genre_2 = `genre-2-NL`,
         intro_NL = `std.samenvatting-NL`,
         intro_EN = `std.samenvatting-EN`,
         afbeelding = feat_img_ids) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) 

  woj_schedule_w_ids.2 <- woj_schedule_w_ids.1 |> left_join(ty_genres, by = join_by(genre == genre_NL))
