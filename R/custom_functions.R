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
  
  data |> 
    mutate(
      .ts_amsterdam = with_tz(as.POSIXct(!!ts_col, tz = tz), tz = tz),
      bc_day_label = day_map[wday(.ts_amsterdam)],
      bc_week_of_month = 1L + (day(.ts_amsterdam) - 1L) %/% 7L,
      bc_hour_start = hour(.ts_amsterdam),
      slot_key = paste0(bc_day_label, str_pad(bc_hour_start, width = 2, side = "left", pad = "0")),
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

# Infer the bi-weekly cycle (A or B) 
# 'start_of_week' is expected to be Thursday 13:00
week_label <- function(start_of_week) {
  ref_date_B_cycle <- ymd_hms("2019-10-17 13:00:00", tz = "Europe/Amsterdam", quiet = TRUE)
  n_weeks <- as.integer(start_of_week - ref_date_B_cycle) %/% 7
  if_else(n_weeks %% 2 == 0, "B", "A")
}

start_of_cz_week <- function(pm_cpnm_db) {
  sql_stmt <- "with thu_13h as (
	                select dates->>'$.start' as start_ts
	                from entries 
	                where type = 'broadcast' 
	                  and site_id = 1 
                    and deleted_at is null
                    and dates->>'$.start' regexp '13:00:00$'
                    and weekday(dates->>'$.start') = 3 -- 3=Thursday 
                  order by 1 desc
                  limit 1
               )
               select date(start_ts) + interval 7 day + interval 13 HOUR as next_week_start
               from thu_13h;"
  dbGetQuery(pm_cpnm_db, sql_stmt)
}

locked_slots <- function(pm_start, pm_stop, pm_db) {
  fmt_start_ts = fmt_ts(pm_start)
  fmt_stop_ts = fmt_ts(pm_stop)
  sql_stmt <- glue_sql(
  "select dates->>'$.start' as locked_slot
   from entries
   where type = 'broadcast'
     and site_id in (1, 2)
     and deleted_at is null
     and dates->>'$.start' between {fmt_start_ts} and {fmt_stop_ts};", 
  .con = pm_db)
  sql_res <- dbGetQuery(pm_db, sql_stmt)
}

cpnm_edi_ins <- function(pm_name_NL, pm_cpnm_db) {
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
                            pm_created_at,
                            pm_cpnm_db) {
  
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  
  # add episode
  new_id_epi <- UUIDgenerate(use.time = FALSE)  # v4
  bc_seconds <- 60 * pm_bc_minutes
  pm_now <- format(pm_created_at, tz = "UTC", usetz = FALSE)
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
      VALUES ({new_id_epi},                                  -- id
              'episode',                                     -- type
              cast({df_cur_pgm$title} as json),              -- title 
              cast({df_cur_pgm$slug} as json),               -- slug
              JSON_OBJECT('nl', {pm_descr_NL},               -- description
                          'en', {pm_descr_EN}),       
              {bc_seconds},                                  -- duration
              {pm_pgm_id},                                   -- parent_id
              {pm_img_id},                                   -- image_id
              {pm_site_id},                                  -- site_id
              5,                                             -- user_id admin/LvdA
              {pm_now}                                       -- created_at
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
  
  # add episode to its chain
  # aci <- append_chain_item(pm_con = pm_cpnm_db,
  #                          pm_label = pm_epi_chain,
  #                          pm_episode_entry_id = new_id_epi,
  #                          pm_step_completed_last = pm_epi_step,
  #                          pm_clockfactory_job = pm_job_id)
  
  # add broadcast
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
      VALUES ({new_id_bc},                                        -- id
              'broadcast',                                        -- type
              cast({df_cur_pgm$title} as json),                   -- title 
              cast({df_cur_pgm$slug} as json),                    -- slug
              JSON_OBJECT('start', {fmt_start_ts},                -- dates
                          'end', {fmt_stop_ts}),                   
              {bc_seconds},                                       -- duration
              {new_id_epi},                                       -- parent_id
              {pm_site_id},                                       -- site_id
              5,                                                  -- user_id admin/LvdA
              {pm_now}                                            -- created_at
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
  
  return(new_id_epi)
}

cpnm_bc_ins <- function(pm_pgm_id, 
                        pm_epi_id,
                        pm_site_id,
                        pm_bc_start,
                        pm_bc_minutes,
                        pm_created_at,
                        pm_cpnm_db) {
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  
  bc_seconds <- 60 * pm_bc_minutes
  new_id_bc <- UUIDgenerate(use.time = FALSE)  # v4
  fmt_start_ts = fmt_ts(pm_bc_start)
  fmt_stop_ts = fmt_ts(pm_bc_start + minutes(pm_bc_minutes))
  pm_now <- format(pm_created_at, tz = "UTC", usetz = FALSE)
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
      VALUES ({new_id_bc},                                      -- id
              'broadcast',                                      -- type
              cast({df_cur_pgm$title} as json),                 -- title 
              cast({df_cur_pgm$slug} as json),                  -- slug
              JSON_OBJECT('start', {fmt_start_ts},              -- dates
                          'end', {fmt_stop_ts}),                 
              {bc_seconds},                                     -- duration
              {pm_epi_id},                                      -- parent_id
              {pm_site_id},                                     -- site_id
              5,                                                -- user_id admin/LvdA
              {pm_now}                                          -- created_at
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

# cpnm_epi_get <- function(pm_pgm_id,
#                          pm_start,
#                          pm_cpnm_db) {
#   sql_stmt <- glue_sql("select e.id as epi_id
#                         from entries p join entries e on e.parent_id = p.id
#                                        join entries b on b.parent_id = e.id
#                         where b.dates->>'$.start' = {pm_start}
#                           and p.id = {pm_pgm_id}
#                         ;", .con = pm_cpnm_db)
#   sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
# }

cpnm_uni_get <- function(pm_pgm_id, pm_max_start, pm_cpnm_db) {
  sql_stmt <- glue_sql("select e.id as epi_id, 
                               b.dates->>'$.start' as epi_start 
                        from entries p join entries e on e.parent_id = p.id
                                                     and e.deleted_at is null
                                       join entries b on b.parent_id = e.id
                                                     and b.deleted_at is null
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

# cpnm_chk_slots <- function(pm_site_id, pm_cpnm_db) {
#   sql_stmt <- glue_sql("select max(dates->>'$.start') as max_start_cz 
#                         from entries 
#                         where type = 'broadcast' 
#                           and site_id = {pm_site_id}
#                           and deleted_at is null;",
#                        .con = pm_cpnm_db)
#   sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
# }

# cpnm_chk_cz_rp <- function(pm_ts, pm_pgm_id, pm_cpnm_db) {
#   sql_stmt <- glue_sql("SELECT g.id AS pgm_id
#                         FROM entries AS b
#                         JOIN entries AS p ON p.id = b.parent_id
#                         JOIN entries AS g ON g.id = p.parent_id
#                         WHERE b.type = 'broadcast'
#                           AND b.site_id = 1
#                           AND CAST(JSON_UNQUOTE(JSON_EXTRACT(b.dates, '$.start')) AS DATETIME) = {pm_ts}
#                         limit 1
#                         ;", 
#                        .con = pm_cpnm_db)
#   sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
#   sql_res$pgm_id == pm_pgm_id
# }

clock2db <- function(pm_clock_tib, pm_created_at, pm_site, pm_db) {
  
  # - some fresh programs have 2 main genres, so have 2 records; treat them separately: 'a' for all columns,
  #   and 'b' just for the extra genre
  cur_clock <- pm_clock_tib |> group_by(slot) |> mutate(tib_item = row_number()) |> ungroup()
  cur_clock_a <- cur_clock |> filter(tib_item == 1L)
  cur_clock_b <- cur_clock |> filter(tib_item == 2L)
  
  for (rn in seq_len(nrow(cur_clock_a))) {
    
    if (!cur_clock_a$is_replay[rn]) {
      # . fresh episode & broadcast ----
      fresh_epi_bc <- cpnm_epi_bc_ins(pm_pgm_id = cur_clock_a$pgm_id[rn],
                                      pm_descr_NL = cur_clock_a$intro_NL[rn],
                                      pm_descr_EN = cur_clock_a$intro_EN[rn],
                                      pm_img_id = cur_clock_a$image_id[rn],
                                      pm_site_id = pm_site,
                                      pm_bc_start = cur_clock_a$slot[rn],
                                      pm_bc_minutes = cur_clock_a$slot_minutes[rn],
                                      pm_created_at,
                                      pm_cpnm_db = pm_db)
      # . genre ----
      # - add an `episode` taxonomable record for first genre
      txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                              pm_txy_id = cur_clock_a$ty_genre_id[rn],
                              pm_order = 1L,
                              pm_cpnm_db = pm_db)
      
      # - add an `episode` taxonomable record for second genre
      df_g2 <- cur_clock_b |> filter(slot == cur_clock_a$slot[rn])
      
      if (nrow(df_g2) == 1) {
        txb_res <- cpnm_txb_ins(pm_epi_id = fresh_epi_bc,
                                pm_txy_id = df_g2$ty_genre_id,
                                pm_order = 2L,
                                pm_cpnm_db = pm_db)
      }

      # . editor ----
      # - add an `episode` taxonomable record for editors and production-role (txy-type colofon)
      txb_res <- cpnm_txb_edi_ins(pm_epi_id = fresh_epi_bc,
                                  pm_txy_id = cur_clock_a$ty_editor_id[rn],
                                  pm_role_NL = cur_clock_a$productie[rn],
                                  pm_cpnm_db = pm_db)
    } else {
      # . replay ----
      bc_replay_res <- cpnm_bc_ins(pm_pgm_id = cur_clock_a$pgm_id[rn],
                                   pm_epi_id = cur_clock_a$replay_of_epi_id[rn],
                                   pm_site_id = pm_site,
                                   pm_bc_start = cur_clock_a$slot[rn],
                                   pm_bc_minutes = cur_clock_a$slot_minutes[rn],
                                   pm_created_at,
                                   pm_cpnm_db = pm_db)
    }
  }
}

epi_replays <- function(pm_pgm_id, pm_genre, pm_max_ts, pm_cpnm_db) {
  sqlstmt <- glue_sql("WITH episodes AS (
         SELECT e.id,
                e.title->>'$.nl' AS epi_title
         FROM entries e
         WHERE e.parent_id = {pgm_id}
           AND e.site_id = 1
           AND e.deleted_at IS NULL
     ),
     broadcasts AS (
         SELECT b.parent_id AS epi_id,
                max(b.dates->>'$.start') AS bc_start
         FROM entries b JOIN episodes e ON e.id = b.parent_id
         WHERE b.site_id = 1
           AND b.deleted_at IS NULL
         GROUP BY b.parent_id
     ),
     base as (
         SELECT
         e.id AS epi_id,
         e.epi_title,
         b.bc_start,
         (
             SELECT GROUP_CONCAT(ty.name->>'$.nl' ORDER BY ty.name->>'$.nl' SEPARATOR '-')
             FROM taxonomables tb JOIN taxonomies ty ON ty.id = tb.taxonomy_id
                                                    AND ty.type = 'genre'
                                                    AND ty.site_id = 1
                                                    AND ty.deleted_at IS NULL
             WHERE tb.taxonomable_id = e.id
               AND tb.taxonomable_type = 'episode'
         ) AS genre_name
         FROM episodes e LEFT JOIN broadcasts b ON b.epi_id = e.id
     )
     select * from base
     where genre_name = {pm_genre} and bc_start < {pm_max_ts}
     ORDER BY bc_start DESC
     limit 1;", .con = pm_cpnm_db)
}

derive_clock_key <- function(slot_ts) {
  slot_week <- 1 + floor((day(slot_ts - 1) / 7))
  wday_nr <- wday(slot_ts, week_start = 1)
  day_abbr <- c("ma", "di", "wo", "do", "vr", "za", "zo")
  slot_weekday <- day_abbr[wday_nr]
  slot_hour <- hour(slot_ts)
  paste0(slot_weekday, str_pad(slot_hour, width = 2, side = "left", pad = "0"), "-", slot_week)
}

append_chain_item <- function(pm_con,
                              pm_label,
                              pm_step_completed_last,
                              pm_episode_entry_id,
                              pm_clockfactory_job = NULL) {
  dbWithTransaction(pm_con, {
    chain_row <- dbGetQuery(
      pm_con,
      "
      SELECT chain_id, next_position
      FROM episode_chain
      WHERE label = ?
      FOR UPDATE
      ",
      params = list(pm_label)
    )
    
    if (nrow(chain_row) == 0) {
      dbExecute(
        pm_con,
        "
        UPDATE episode_chain_seq
        SET id = LAST_INSERT_ID(id + 1)
        "
      )
      
      chain_id <- dbGetQuery(
        pm_con,
        "SELECT LAST_INSERT_ID() AS chain_id"
      )$chain_id[[1]]
      
      position <- 1L
      
      dbExecute(
        pm_con,
        "
        INSERT INTO episode_chain (chain_id, label, next_position)
        VALUES (?, ?, 2)
        ",
        params = list(chain_id, pm_label)
      )
    } else {
      chain_id <- chain_row$chain_id[[1]]
      position <- chain_row$next_position[[1]]
      
      dbExecute(
        pm_con,
        "
        UPDATE episode_chain
        SET next_position = next_position + 1
        WHERE chain_id = ?
        ",
        params = list(chain_id)
      )
    }
    
    dbExecute(
      pm_con,
      "
      INSERT INTO episode_chain_item
        (chain_id, position, step_completed_last, episode_entry_id, clockfactory_job)
      VALUES (?, ?, ?, ?, ?)
      ",
      params = list(
        chain_id,
        position,
        pm_step_completed_last,
        pm_episode_entry_id,
        pm_clockfactory_job
      )
    )
    
    tibble(
      chain_id = chain_id,
      position = position
    )
  })
}

lookup_replay <- function(pm_chains, pm_cur_chain, pm_replay_slot, pm_offset) {
  replay_candidates <- pm_chains |> filter(episode_chain == pm_cur_chain & slot < pm_replay_slot)
  replay_candidates$episode_entry_id[pm_offset + 1L]
}
