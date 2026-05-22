# prep moderooster en wp-gidsinfo tbv de koppeling met CPNM-id's
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, readxl, DBI,
               purrr, httr, jsonlite, yaml, ssh, googledrive, openxlsx, glue, uuid, RMariaDB)

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

# improve name for 'has non-zero number of characters'
has_value <- function(x) nzchar(x)

# Wait for tunnel to become available
wait_for_tunnel <- function(t_host, t_port, total_timeout = 10) {
  start <- Sys.time()
  repeat {
    con <- suppressWarnings(
      try(
        socketConnection(
          host = t_host,
          port = t_port,
          open = "r",
          timeout = 1
        ),
        silent = TRUE
      )
    )
    
    if (!inherits(con, "try-error")) {
      close(con)
      return(TRUE)
    }
    
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > total_timeout) {
      stop("Spinning up the SSH-tunnel failed.")
    }
    
    Sys.sleep(0.2)
  }
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

repeat {
  # ... run queries ...
  query <- "select t1.name->>'$.nl' as ty_pgm_title_NL, 
                   t1.id as ty_pgm_title_id, 
                   t2.name->>'$.nl' as ty_genre_NL,
                   t2.id as ty_genre_id,
                   t1.name as ty_pgm_title_json, 
                   t1.slug as ty_pgm_slug_json
            from taxonomies t1 join taxonomies t2 on t2.id = t1.parent_id
            where t1.type = 'subgenre' and t1.parent_id is not null order by 1;"
  ty_subgenres_raw <- dbGetQuery(con, query)
  ty_subgenres <- ty_subgenres_raw |> mutate(ty_pgm_title_NL = str_replace(ty_pgm_title_NL, "&amp;", "&"))
  
  # query <- "select t1.name->>'$.nl' as pgm_title, t2.name->>'$.nl' as pgm_genre, t1.id as pgm_title_to_genre_id
  #           from taxonomies t1 join taxonomies t2 on t1.parent_id = t2.id
  #           where t1.parent_id is not null
  #             and t1.type = 'subgenre' 
  #             and t2.type = 'genre'
  #           ;"
  # tbl_title2genre_raw <- dbGetQuery(con, query)
  # tbl_title2genre <- tbl_title2genre_raw |> mutate(pgm_title = str_replace_all(pgm_title, "&amp;", "&"))

  tbl_title2genre_w_ids_1 <- tbl_zenderschema.4 |> 
    left_join(ty_subgenres, by = join_by(titel_NL == ty_pgm_title_NL, genre == ty_genre_NL)) # |> filter(!is.na(pgm_title_to_genre_id))
  
  # tbl_title2genre_w_ids_2 <- tbl_zenderschema.4 |> 
  #   left_join(tbl_title2genre, by = join_by(titel_NL == pgm_title, genre_2 == pgm_genre)) |> filter(!is.na(pgm_title_to_genre_id)) |> 
  #   anti_join(tbl_title2genre_w_ids_1, by = join_by(moro_key))
  # tbl_title2genre_w_ids_3 <- bind_rows(tbl_title2genre_w_ids_1, tbl_title2genre_w_ids_2) |> distinct()
  
  tbl_title2genre_w_ids_1_missing <- tbl_title2genre_w_ids_1 |> filter(is.na(ty_genre_id))
  
  if (missing > 0) {
    print("tbl_title2genre_w_ids_3 is incomplete; quiting this job.")
    break
  }
  
  query <- "WITH ranked AS (
               SELECT distinct
                 p.id              AS pgm_id,
                 p.title->>'$.nl'  AS pgm_title_nl,
                 ty.id             AS genre_id,
                 ty.name->>'$.nl'  AS genre_name_nl,
                 ROW_NUMBER() OVER (
                   PARTITION BY p.id
                   ORDER BY ty.id
                 ) AS rn
               FROM entries AS p
               JOIN taxonomables AS ta
                 ON ta.taxonomable_id = p.id
               JOIN taxonomies AS ty
                 ON ty.id = ta.taxonomy_id
                AND ty.type = 'genre'
               WHERE p.type = 'program'
                 and ty.id != 'f0f77a5b-ac0e-4e92-b143-5917223acc7a' -- 'Algemeen' overslaan
            )
            SELECT
              pgm_id,
              pgm_title_nl,
              MAX(CASE WHEN rn = 1 THEN genre_id END)       AS genre_id_1,
              MAX(CASE WHEN rn = 1 THEN genre_name_nl END)  AS genre_name_nl_1,
              MAX(CASE WHEN rn = 2 THEN genre_id END)       AS genre_id_2,
              MAX(CASE WHEN rn = 2 THEN genre_name_nl END)  AS genre_name_nl_2
            FROM ranked
            WHERE rn <= 2
            GROUP BY pgm_id, pgm_title_nl
            order by pgm_title_nl
            ;"
  
  pgm_genres_raw <- dbGetQuery(con, query)
  pgm_genres_df <- pgm_genres_raw |> mutate(pgm_title_nl = str_replace_all(pgm_title_nl, "&amp;", "&"),
                                            genre_id_2 = if_else(genre_id_2 != genre_id_1, genre_id_2, NA_character_),
                                            genre_name_nl_2 = if_else(genre_id_2 != genre_id_1, genre_name_nl_2, NA_character_))
  tbl_title2genre_w_ids_4 <- tbl_zenderschema.4 |> 
    # left_join(pgm_genres_df, by = join_by(titel_NL == pgm_title_nl, genre_1 == genre_name_nl_1), relationship = "many-to-many") |> 
    left_join(pgm_genres_df, by = join_by(titel_NL == pgm_title_nl), relationship = "many-to-many") |> 
    group_by(moro_key) |> mutate(n_dup_keys = n()) |> ungroup()
  
  dup_keys <- tbl_title2genre_w_ids_4 |> filter(n_dup_keys > 1) |> nrow()
  
  query <- "select name->>'$.nl' as editor_name, id as editor_id from taxonomies 
            where legacy_type = 'programma_maker' or legacy_type is null
            order by 1
            ;"
  editor_df <- dbGetQuery(con, query)
  tbl_title2genre_w_ids_5 <- tbl_title2genre_w_ids_4 |> 
    left_join(editor_df, by = join_by("redacteurs" == "editor_name"))
  
  missing_editors <- tbl_title2genre_w_ids_5 |> filter(is.na(editor_id))
  
  tbl_title2genre_w_ids_6 <- tbl_title2genre_w_ids_5 |> 
    left_join(ty_subgenres, by = join_by(titel_NL == ty_pgm_title_NL), relationship = "many-to-many")
  
  break
}

# Cleanup
dbDisconnect(con)

# Kill the tunnel; find PID(s) of ssh tunnel on local port
pid <- system2("lsof", args = c("-ti", paste0("tcp:", env_db_port)), stdout = TRUE)

if (length(pid) > 0) {
  system2("kill", pid)
}  
