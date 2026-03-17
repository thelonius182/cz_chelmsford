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

logfmt_ts <- function(x) {
  format(x, "%Y-%m-%d %H:%M:%S %Z")
}

# Infer the bi-weekly cycle (A or B) for the current CZ-broadcast week (Thu-Thu).
# 'x' is expected to be Thursday-start-of-week
week_label <- function(x) {
  # test a Friday: x <- "2025-12-19" 
  ymd_x <- ymd(x)
  if (is.na(ymd_x)) stop(str_glue("expected: a valid ymd date string. Got: {x}"))
  a_Monday <- 1L
  wd_x <- wday(ymd_x, week_start = a_Monday)
  if (wd_x != 4) stop(str_glue("expected: 4 = Thursday. Got: {wd_x}"))
  ref_date_B_cycle <- ymd("2019-10-17")
  n_weeks <- as.integer(ymd_x - ref_date_B_cycle) %/% 7
  if (n_weeks %% 2 == 0) "B" else "A"
}

latest_week <- function(pm_cpnm_db) {
  sql_stmt <- "WITH latest_8 AS (
      SELECT DATE(dates->>'$.start') AS d
      FROM entries
      where type = 'broadcast' 
	      and site_id = 1 
        and dates->>'$.start' regexp '19:00:00$'
      ORDER BY 1 DESC
      LIMIT 8
   ),
   ordered AS (
      SELECT d,
             LEAD(d) OVER (ORDER BY d desc) AS next_d
      FROM latest_8
   )
   select d, 
          next_d, 
          datediff(d, next_d) as diff_days 
   from ordered
   where next_d is not null
   ;"
  dbGetQuery(pm_cpnm_db, sql_stmt)
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

cpnm_epi_bc_ins <- function(pm_pgm_id, 
                            pm_descr_NL,
                            pm_descr_EN,
                            pm_img_id,
                            pm_site_id,
                            pm_bc_start,
                            pm_bc_minutes,
                            pm_cpnm_db) {
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  
  new_id_epi <- UUIDgenerate(use.time = FALSE)  # v4
  bc_seconds <- 60 * pm_bc_minutes
  sql_stmt <- glue_sql("
      INSERT INTO entries (id,
                           type,
                           title,
                           slug,
                           description,
                           duration,
                           parent_id,
                           image_id,
                           site_id,
                           user_id,
                           created_at)
      VALUES ({new_id_epi},                           -- id
              'episode',                              -- type
              cast({df_cur_pgm$title}as json),        -- title 
              cast({df_cur_pgm$slug} as json),        -- slug
              JSON_OBJECT('nl', {pm_descr_NL},        -- description
                          'en', {pm_descr_EN}),       
              {bc_seconds},                           -- duration
              {pm_pgm_id},                            -- parent_id
              {pm_img_id},                            -- image_id
              {pm_site_id},                           -- site_id
              5,                                      -- user_id admin/LvdA
              NOW()                                   -- created_at
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
  
  new_id_bc <- UUIDgenerate(use.time = FALSE)  # v4
  fmt_start_ts = fmt_ts(pm_bc_start)
  fmt_stop_ts = fmt_ts(pm_bc_start + minutes(pm_bc_minutes))
  sql_stmt <- glue_sql("
      INSERT INTO entries (id,
                           type,
                           title,
                           slug,
                           dates,
                           duration,
                           parent_id,
                           site_id,
                           user_id,
                           created_at)
      VALUES ({new_id_bc},                            -- id
              'broadcast',                            -- type
              cast({df_cur_pgm$title}as json),        -- title 
              cast({df_cur_pgm$slug} as json),        -- slug
              JSON_OBJECT('start', {fmt_start_ts},    -- dates
                          'end', {fmt_stop_ts}),       
              {bc_seconds},                           -- duration
              {new_id_epi},                           -- parent_id
              {pm_site_id},                           -- site_id
              5,                                      -- user_id admin/LvdA
              NOW()                                   -- created_at
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
  
  return(new_id_epi)
}

cpnm_bc_ins <- function(pm_pgm_id, 
                        pm_epi_id,
                        pm_site_id,
                        pm_bc_start,
                        pm_bc_minutes,
                        pm_cpnm_db) {
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  
  bc_seconds <- 60 * pm_bc_minutes
  new_id_bc <- UUIDgenerate(use.time = FALSE)  # v4
  fmt_start_ts = fmt_ts(pm_bc_start)
  fmt_stop_ts = fmt_ts(pm_bc_start + minutes(pm_bc_minutes))
  sql_stmt <- glue_sql("
      INSERT INTO entries (id,
                           type,
                           title,
                           slug,
                           dates,
                           duration,
                           parent_id,
                           site_id,
                           user_id,
                           created_at)
      VALUES ({new_id_bc},                            -- id
              'broadcast',                            -- type
              cast({df_cur_pgm$title} as json),       -- title 
              cast({df_cur_pgm$slug} as json),        -- slug
              JSON_OBJECT('start', {fmt_start_ts},    -- dates
                          'end', {fmt_stop_ts}),       
              {bc_seconds},                           -- duration
              {pm_epi_id},                            -- parent_id
              {pm_site_id},                           -- site_id
              5,                                      -- user_id admin/LvdA
              NOW()                                   -- created_at
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
}

cpnm_txb_edi_ins <- function(pm_epi_id,
                         pm_txy_id,
                         pm_role_NL,
                         pm_cpnm_db) {
  role_EN <- if (str_ends(pm_role_NL, pattern = "tatie")) {
    "Produced & presented by"
  } else {
    "Produced by"
  }
  
  sql_stmt <- glue_sql("
      INSERT INTO taxonomables (taxonomy_id,
                                taxonomable_type,
                                taxonomable_id,
                                label)
      VALUES ({pm_txy_id},
              'episode',
              {pm_epi_id},
              JSON_OBJECT('nl', {pm_role_NL},
                          'en', {role_EN})
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
  
}

cpnm_txb_ins <- function(pm_epi_id,
                         pm_txy_id,
                         pm_order,
                         pm_cpnm_db) {
  sql_stmt <- glue_sql("
      INSERT INTO taxonomables (taxonomy_id,
                                taxonomable_type,
                                taxonomable_id,
                                `order`)
      VALUES ({pm_txy_id},
              'episode',
              {pm_epi_id},
              {pm_order}
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
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
  sql_stmt <- glue_sql("select e.id as epi_id, 
                               b.dates->>'$.start' as epi_start 
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
