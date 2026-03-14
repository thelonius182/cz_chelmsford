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

wait_for_tunnel <- function(
    tunnel,
    t_host,
    t_port,
    total_timeout = 10,
    poll_interval = 0.2
) {
  start_time <- Sys.time()
  
  repeat {
    if (!tunnel$is_alive()) {
      err <- tunnel$read_all_error()
      stop(
        "SSH tunnel process exited before becoming ready.\n",
        if (nzchar(err)) err else "No SSH error output captured."
      )
    }
    
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
      return(invisible(TRUE))
    }
    
    elapsed <- as.numeric(
      difftime(Sys.time(), start_time, units = "secs")
    )
    
    if (elapsed > total_timeout) {
      stop("Spinning up the SSH tunnel failed.")
    }
    
    Sys.sleep(poll_interval)
  }
}

close_tunnel <- function(tunnel) {
  if (!is.null(tunnel) && tunnel$is_alive()) {
    tunnel$kill()
    tunnel$wait(timeout = 2000)
  }
  
  invisible(NULL)
}

cpnm_edi_ins <- function(pm_name_NL, 
                            pm_cpnm_db) {
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  ed_name_slug <- as_slug(pm_name_NL)
  sql_stmt <- glue_sql("INSERT INTO taxonomies (
       id,
       type,
       name,
       slug,
       site_id,
       user_id,
       created_at
     )
     VALUES (
         {new_id},
         'colofon',
         JSON_OBJECT('nl', {pm_name_NL},
                     'en', {pm_name_NL}),
         JSON_OBJECT('nl', {ed_name_slug},
                     'en', {ed_name_slug}),
         1,                                      -- site_id
         5,                                      -- user_id LvdA
         NOW()                                   -- created_at
     );", .con = pm_cpnm_db)
  dbExecute(pm_cpnm_db, sql_stmt)
}

cpnm_pgm_ins <- function(pm_title_NL, 
                         pm_title_EN, 
                         pm_descr_NL,
                         pm_descr_EN,
                         pm_cpnm_db) {
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  slug_NL <- as_slug(pm_title_NL) 
  slug_EN <- as_slug(pm_title_EN) 
  sql_stmt <- glue_sql("INSERT INTO entries (
    id,
    type,
    title,
    slug,
    description,
    site_id,
    user_id,
    created_at
     )
     VALUES (
         {new_id},
         'program',
         JSON_OBJECT('nl', {pm_title_NL},
                     'en', {pm_title_EN}),
         JSON_OBJECT('nl', {slug_NL},
                     'en', {slug_EN}),
         JSON_OBJECT('nl', {pm_descr_NL},
                     'en', {pm_descr_EN}),
         1,                                      -- site_id
         5,                                      -- user_id LvdA
         NOW()                                   -- created_at
     );", .con = pm_cpnm_db)
  dbExecute(pm_cpnm_db, sql_stmt)
}

cpnm_pgm_upd <- function(pm_pgm_id,
                         pm_cpnm_db) {
  sql_stmt <- glue_sql("update entries set 
                          title = json_object('nl´, ),
                          slug = json_object()
                          description = json_object()
                        where id = {pm_pgm_id};")
}

cpnm_epi_ins <- function(pm_pgm_id, 
                         pm_descr_NL,
                         pm_descr_EN,
                         pm_img_id,
                         pm_site_id,
                         pm_cpnm_db) {
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  new_id <- UUIDgenerate(use.time = FALSE)  # v4
  sql_stmt <- glue_sql("
      INSERT INTO entries (id,
                           type,
                           title,
                           slug,
                           description,
                           duration,
                           order,
                           parent_id,
                           image_id,
                           site_id,
                           user_id,
                           created_at)
      VALUES ({new_id},                               -- id
              'episode',                              -- type
              {df_cur_pgm$title},                     -- title 
              {df_cur_pgm$slug},                      -- slug
              JSON_OBJECT('nl', {pm_descr_NL},        -- description
                          'en', {pm_descr_EN}),       
              {pm_seconds},                           -- duration
              0,                                      -- order
              {pm_pgm_id},                            -- parent_id
              {pm_img_id},                            -- image_id
              {pm_site_id},                           -- site_id
              5,                                      -- user_id admin/LvdA
              NOW()                                   -- created_at
      );", .con = pm_cpnm_db)
  dbExecute(pm_cpnm_db, sql_stmt)
  return(new_id)
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

cpnm_epi_get <- function(pm_pgm_id,
                         pm_start,
                         pm_cpnm_db) {
  sql_stmt <- glue_sql("select e.id as epi_id
                        from entries p join entries e on e.parent_id = p.id
                                       join entries b on b.parent_id = e.id
                        where b.dates->>'$.start' = {pm_start}
                          and p.id = {pm_pgm_id}
                        ;", .con = pm_cpnm_db)
  sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
}

cpnm_uni_get <- function(pm_pgm_id, pm_max_start, pm_cpnm_db) {
  sql_stmt <- glue_sql("select e.id as epi_id, b.dates->>'$.start' as epi_start 
                        from entries p join entries e on e.parent_id = p.id
                                       join entries b on b.parent_id = e.id
                        where p.id = {pm_pgm_id}
                          and b.dates->>'$.start' < {pm_max_start}
                        order by 2 desc 
                        limit 1
                        ;", .con = pm_cpnm_db)
  sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
}

cpnm_img_get <- function(pm_img_id, pm_cpnm_db) {
  sql_stmt <- glue_sql("SELECT id FROM media 
                        where type = 'image'
                          and site_id in (1, 2)
                          and legacy_data->>'$.wp_post_id' = {pm_img_id};", 
                       .con = pm_cpnm_db)
  sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
}

cpnm_chk_slots <- function(pm_site_id, pm_cpnm_db) {
  sql_stmt <- glue_sql("select max(dates->>'$.start') as max_start_cz 
                        from entries 
                        where type = 'broadcast' 
                          and site_id = {pm_site_id}
                          and deleted_at is null;",
                       .con = pm_cpnm_db)
  sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
}
