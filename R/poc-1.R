# prep moderooster en wp-gidsinfo tbv de koppeling met CPNM-id's
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, ssh, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, keyring, openxlsx)

cz_extract_sheet <- function(ss_name, sheet_name) {
  read_xlsx(ss_name,
            sheet = sheet_name,
            .name_repair = ~ ifelse(nzchar(.x), .x, LETTERS[seq_along(.x)]))
}

cz_get_url <- function(cz_ss) {
  cz_url <- paste0("url_", cz_ss)
  
  # use [[ instead of $, because it is a variable, not a constant
  paste0("https://", config$url_pfx, config[[cz_url]]) 
}

config <- read_yaml("config.yaml")

# downloads GD ------------------------------------------------------------

# trigger GD-auth
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# Roosters 3.0 ophalen bij GD
path_roosters <- "/home/lon/R_projects/cz_chelmsford/resources/roosters.xlsx"
drive_download(file = cz_get_url("roosters"), overwrite = T, path = path_roosters)

# WP-gids-info ophalen bij GD
path_wp_gidsinfo <- "/home/lon/R_projects/cz_chelmsford/resources/wordpress_gidsinfo.xlsx"
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_wp_gidsinfo)

# sheets als df -----------------------------------------------------------
tbl_raw_zenderschema <- cz_extract_sheet(path_roosters, sheet_name = paste0("modelrooster-", config$modelrooster_versie))
tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")

# zenderschema ------------------------------------------------------------
  tbl_zenderschema.1 <- tbl_raw_zenderschema |> 
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

tbl_zenderschema.2 <- tbl_zenderschema.1 |>
  select(-starts_with("r"), -starts_with("b"), -starts_with("t", ignore.case = F), -dag, -start, -Toon) |>  
  select(slot, hh_formule, everything()) |> 
  pivot_longer(names_to = "wanneer", cols = starts_with("week_"), values_to = "titel") 

tbl_zenderschema.3 <- tbl_zenderschema.2 |> 
  mutate(titel = if_else(!is.na(titel), titel, if_else(!is.na(wekelijks), wekelijks, AB_cyclus))) |> 
  select(titel) |> distinct() 

tbl_zenderschema.4 <- tbl_zenderschema.3 |> left_join(tbl_raw_wpgidsinfo, by = join_by(titel == `key-modelrooster`)) |> 
  select(moro_key = titel,
         titel_NL = `titel-NL`,
         titel_EN = `titel-EN`,
         productie = `productie-1-taak`,
         redacteurs = `productie-1-mdw`,
         genre_1 = `genre-1-NL`,
         genre_2 = `genre-2-NL`,
         intro_NL = `std.samenvatting-NL`,
         intro_EN = `std.samenvatting-EN`,
         afbeelding = feat_img_ids
         ) |> arrange(moro_key)

tunnel <- "<see_pws>"

# Connect via tunnel
con <- "<see_pws>"

# ... run queries ...
query <- "select t1.name->>'$.nl' as pgm_title, t2.name->>'$.nl' as pgm_genre, t1.id as pgm_title_to_genre_id
from taxonomies t1 join taxonomies t2 on t1.parent_id = t2.id
where t1.parent_id is not null
  and t1.type = 'subgenre' 
  and t2.type = 'genre'
;"
tbl_title2genre_raw <- dbGetQuery(con, query)
tbl_title2genre <- tbl_title2genre_raw |> mutate(pgm_title = str_replace_all(pgm_title, "&amp;", "&"))
tbl_title2genre_w_ids <- tbl_zenderschema.4 |> 
  left_join(tbl_title2genre, by = join_by(titel_NL == pgm_title, genre_1 == pgm_genre))

# Cleanup
dbDisconnect(con)
# Kill the tunnel; find PID(s) of ssh tunnel on local port 3307
pid <- system2("lsof", args = c("-ti", "tcp:3307"), stdout = TRUE)

if (length(pid) > 0) {
  system2("kill", pid)
}  
