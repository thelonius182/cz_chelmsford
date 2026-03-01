# prep modelrooster en wp-gidsinfo tbv de koppeling met CPNM-id's
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

config <- read_yaml("config.yaml")
source("R/custom_functions.R", encoding = "UTF-8")

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
  ) |> pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> arrange(moro_key)

# prepare tunnel/database settings
env_tun_auth <- Sys.getenv("CPNM_TUNNEL_AUTH_UBU")
if (!has_value(env_tun_auth)) stop("Missing (Ubuntu-)authentication for SSH-tunnel to CPNM")
env_tun_map <- Sys.getenv("CPNM_TUNNEL_MAPPING")
if (!has_value(env_tun_map)) stop("Missing local/remote mapping for SSH-tunnel to CPNM")
env_tun_usr <- Sys.getenv("CPNM_TUNNEL_USER")
if (!has_value(env_tun_usr)) stop("Missing user for SSH-tunnel to CPNM")

env_db_user <- Sys.getenv("CPNM_DB_USER")
if (!has_value(env_db_user)) stop("Missing CPNM db-user")
env_db_pwd <- Sys.getenv("CPNM_DB_PWD")
if (!has_value(env_db_pwd)) stop("Missing CPNM db-password")
env_db_name <- Sys.getenv("CPNM_DB_NAME")
if (!has_value(env_db_name)) stop("Missing CPNM db-name")
env_db_host <- Sys.getenv("CPNM_DB_HOST")
if (!has_value(env_db_host)) stop("Missing CPNM db-host")
env_db_port <- Sys.getenv("CPNM_DB_PORT")
if (!has_value(env_db_port)) stop("Missing CPNM db-port")

# create SSH-tunnel
tunnel <- system2(
  "ssh",
  args = c("-f", "-N",         # run in background, no command
           "-i", env_tun_auth,
           "-L", env_tun_map,  # local:remote mapping
           env_tun_usr),
  wait = FALSE
)

wait_for_tunnel(env_db_host, env_db_port)

# Use tunnel to connect to CPNM-database
con <- dbConnect(
  drv = RMariaDB::MariaDB(),
  host = env_db_host,
  port = as.integer(env_db_port),
  user = env_db_user,
  password = env_db_pwd,
  dbname = env_db_name
)

# Main Control Loop
repeat {
  # genres in 'taxonomies'-table
  query <- "select name->>'$.nl' as genre_NL, 
                   id as ty_genre_id 
            from taxonomies 
            where type = 'genre' 
              and site_id in (1, 2)
              and name->>'$.nl' not in ('Algemeen', 'World of Jazz')
            order by 1
            ;"
  
  ty_genres <- dbGetQuery(con, query)

  tbl_w_ids_1 <- tbl_zenderschema.4 |> left_join(ty_genres, by = join_by(genre == genre_NL))
  tbl_w_ids_1_missing <- tbl_w_ids_1 |> filter(is.na(ty_genre_id))
  
  if (nrow(tbl_w_ids_1_missing) > 0) {
    print("genres missing in the taxonomy; quiting this job.")
    break
  }
  
  # editors in 'taxonomies'-table
  query <- "select name->>'$.nl' as editor_name, 
                   id as ty_editor_id 
            from taxonomies 
            where legacy_type = 'programma_maker' or legacy_type is null
            order by 1
            ;"
  ty_editors <- dbGetQuery(con, query)
  tbl_w_ids_2 <- tbl_w_ids_1 |> 
    left_join(ty_editors, by = join_by("redacteurs" == "editor_name"))
  
  tbl_w_ids_2_missing <- tbl_w_ids_2 |> filter(is.na(ty_editor_id))
  
  if (nrow(tbl_w_ids_2_missing) > 0) {
    print("editors missing in the taxonomy; quiting this job.")
    break
  }
  
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
          pgm_id, n_episodes,
          ROW_NUMBER() OVER (PARTITION BY pgm_title_NL
                             ORDER BY n_episodes desc) AS rn
   from ds2
   )
   select * from ds3  where rn = 1 
   order by 1;"
  program_titles <- dbGetQuery(con, query) |> select(pgm_title_NL, pgm_id)
  # program_titles <- program_titles_raw |> mutate(pgm_title_NL = str_replace_all(pgm_title_NL, "&amp;", "&"))
  tbl_w_ids_3 <- tbl_w_ids_2 |> 
    left_join(program_titles, by = join_by("titel_NL" == "pgm_title_NL"), relationship = "many-to-many")
  
  tbl_w_ids_3_missing <- tbl_w_ids_3 |> filter(is.na(pgm_id))
  
  if (nrow(tbl_w_ids_3_missing) > 0) {
    print("programs missing in 'entries'-table; quiting this job.")
    break
  }
  
  # exit MCL
  break
}

# Cleanup
dbDisconnect(con)

# Kill the tunnel; find PID(s) of ssh tunnel on local port
pid <- system2("lsof", args = c("-ti", paste0("tcp:", env_db_port)), stdout = TRUE)

if (length(pid) > 0) {
  system2("kill", pid)
}  
