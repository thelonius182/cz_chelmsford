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

ins_cpnm_pgm <- function(pm_title_NL, 
                         pm_title_EN, 
                         pm_slug_NL, 
                         pm_slug_EN, 
                         pm_descr_NL,
                         pm_descr_EN,
                         pm_cpnm_db) {
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  sql_stmt <- glue_sql("INSERT INTO entries (
    id,
    type,
    title,
    slug,
    description,
    duration,
    order,
    site_id,
    user_id,
    created_at
     )
     VALUES (
         {new_id},
         'program',
         JSON_OBJECT('nl', {pm_title_NL},
                     'en', {pm_title_EN}),
         JSON_OBJECT('nl', {pm_slug_NL},
                     'en', {pm_slug_EN}),
         JSON_OBJECT('nl', {pm_descr_NL},
                     'en', {pm_descr_EN}),
         0,                                      -- duration
         0,                                      -- order
         1,                                      -- site_id
         5,                                      -- user_id LvdA
         NOW()                                   -- created_at
     );", .con = pm_cpnm_db)
  dbExecute(pm_cpnm_db, sql_stmt)
}

ins_cpnm_editor <- function(pm_name_NL, 
                            pm_name_EN, 
                            pm_slug_NL, 
                            pm_slug_EN, 
                            pm_cpnm_db) {
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  sql_stmt <- glue_sql("INSERT INTO taxonomies (
       id,
       type,
       name,
       slug,
       site_id,
       user_id,
       legacy_type,
       created_at
     )
     VALUES (
         {new_id},
         'creator',
         JSON_OBJECT('nl', {pm_name_NL},
                     'en', {pm_name_EN}),
         JSON_OBJECT('nl', {pm_slug_NL},
                     'en', {pm_slug_EN}),
         1,                                      -- site_id
         5,                                      -- user_id LvdA
         'programma_maker',                      -- legacy type
         NOW()                                   -- created_at
     );", .con = pm_cpnm_db)
  dbExecute(pm_cpnm_db, sql_stmt)
}


ins_cpnm_episode <- function(pm_pgm_id, 
                             pm_editor_id, 
                             pm_slug_EN, 
                             pm_descr_NL,
                             pm_descr_EN,
                             pm_cpnm_db) {
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  sql_stmt <- glue_sql("INSERT INTO entries (
    id,
    type,
    surtitle,
    title,
    subtitle,
    slug,
    description,
    content,
    blocks,
    sections,
    dates,
    attributes,
    properties,
    playlist,
    duration,
    order,
    parent_id,
    image_id,
    media_id,
    site_id,
    user_id,
    legacy_id,
    legacy_type,
    legacy_data,
    published_at,
    created_at,
    updated_at,
    deleted_at
)
VALUES (
    UUID(),
    'default',

    NULL,                                   -- surtitle (JSON)
    JSON_OBJECT('en', 'Main title'),        -- title (required JSON)
    NULL,                                   -- subtitle
    JSON_OBJECT('en', 'my-slug'),           -- slug
    NULL,                                   -- description
    NULL,                                   -- content
    NULL,                                   -- blocks
    NULL,                                   -- sections
    NULL,                                   -- dates
    NULL,                                   -- attributes
    NULL,                                   -- properties
    NULL,                                   -- playlist

    0,                                      -- duration
    0,                                      -- order

    NULL,                                   -- parent_id
    NULL,                                   -- image_id
    NULL,                                   -- media_id

    1,                                      -- site_id
    1,                                      -- user_id

    NULL,                                   -- legacy_id
    NULL,                                   -- legacy_type
    NULL,                                   -- legacy_data

    NOW(),                                  -- published_at
    NOW(),                                  -- created_at
    NOW(),                                  -- updated_at
    NULL                                    -- deleted_at
);", .con = pm_cpnm_db)
dbExecute(pm_cpnm_db, sql_stmt)
}

add_bc_cols <- function(data, ts_col, tz = "Europe/Amsterdam") {
  ts_col <- rlang::ensym(ts_col)
  
  day_map <- c("zo","ma","di","wo","do","vr","za")  # lubridate wday: 1=Sunday .. 7=Saturday
  
  data %>%
    mutate(
      .ts_amsterdam = with_tz(as.POSIXct(!!ts_col, tz = tz), tz = tz),
      bc_day_label = day_map[wday(.ts_amsterdam)],
      bc_week_of_month = 1L + (day(.ts_amsterdam) - 1L) %/% 7L,
      bc_hour_start = hour(.ts_amsterdam),
      .ts_amsterdam = NULL
    )
}

as_slug <- function(pm_str) {
  s1 <- str_replace_all(pm_str, "[^- [:word:]]", "")
  s1 <- str_trim(string = s1, side = "both")
  s1 <- str_replace_all(s1, " +", "-")
  s1 <- str_replace_all(s1, "-+", "-") |> str_to_lower()
  stringi::stri_trans_general(s1, "Latin-ASCII")
}
