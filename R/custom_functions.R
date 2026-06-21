# improve name for 'has non-zero number of characters'
has_value <- function(x) nzchar(x)

fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")

add_bc_cols <- function(data, ts_col, tz = TZ_AM) {
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
bc_week_label <- function(pm_start_of_bc_week, pm_site_id) {
  ref_date_B_cycle <- ymd_hms("2019-10-17 13:00:00", tz = TZ_AM, quiet = TRUE)
  hour(ref_date_B_cycle) <- bc_week_start_hour(pm_site_id)
  n_weeks <- as.integer(pm_start_of_bc_week - ref_date_B_cycle) %/% 7
  if_else(n_weeks %% 2 == 0, "B", "A")
}

bc_week_start_hour <- function(pm_site_id) {
  case_when(pm_site_id == SITE$CONCERTZENDER ~ config$bc_week_start_hour_cz,
            pm_site_id == SITE$WORLD_OF_JAZZ ~ config$bc_week_start_hour_wj,
            TRUE ~ 0L)
}

next_bc_week_start <- function(pm_site_id, pm_cpnm_db) {
  next_bc_week_start_hour <- bc_week_start_hour(pm_site_id)
  nws_rgx <- paste0(next_bc_week_start_hour, ":00:00$")
  SQL_THURSDAY <- 3L
  sql_stmt <- glue_sql("with thu_hh as (
	                        select dates->>'$.start' as max_thursday
	                        from entries 
	                        where type = 'broadcast' 
	                          and site_id = {pm_site_id}
                            and deleted_at is null
                            and dates->>'$.start' regexp {nws_rgx}
                            and weekday(dates->>'$.start') = {SQL_THURSDAY}
                          order by 1 desc
                          limit 1
                        )
                        select date(max_thursday) + interval 7 DAY + interval {next_bc_week_start_hour} HOUR as value
                        from thu_hh;", .con = pm_cpnm_db)
  dbGetQuery(pm_cpnm_db, sql_stmt)
}

# locked_slots <- function(pm_start, pm_stop, pm_site_id, pm_db) {
#   fmt_start_ts = fmt_ts(pm_start - minutes(10L))
#   fmt_stop_ts = fmt_ts(pm_stop - minutes(10L))
#   sql_stmt <- glue_sql(
#   "select dates->>'$.start' as slot,
#    parent_id as episode_entry_id
#    from entries
#    where type = 'broadcast'
#      and site_id = {pm_site_id}
#      and deleted_at is null
#      and dates->>'$.start' between {fmt_start_ts} and {fmt_stop_ts};",
#   .con = pm_db)
#   dbGetQuery(pm_db, sql_stmt)
# }

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
  
  # always get program name from PRD-table 'entries'!
  sql_stmt <- glue_sql("select title, slug from entries where id = {pm_pgm_id};", .con = pm_cpnm_db)
  df_cur_pgm <- dbGetQuery(pm_cpnm_db, sql_stmt)
  
  # add episode
  new_id_epi <- UUIDgenerate(use.time = FALSE)  # v4
  bc_seconds <- 60 * pm_bc_minutes
  pm_now <- format(pm_created_at, tz = "UTC", usetz = FALSE)
  pm_img_id <- if (is.na(pm_img_id)) DFT_IMG[site_id] else pm_img_id
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

cpnm_cck_ins <- function(pm_epi_id,
                         pm_cck,
                         pm_cpnm_db) {
  sql_stmt <- glue_sql("
      INSERT INTO episode_catlg_keys (ep_id,
                                      ep_catkey)
      VALUES ({pm_epi_id},
              {pm_cck}
      );", .con = pm_cpnm_db)
  sql_res <- dbExecute(pm_cpnm_db, sql_stmt)
}

# cpnm_uni_get <- function(pm_pgm_id, pm_max_start, pm_cpnm_db) {
#   sql_stmt <- glue_sql("select e.id as epi_id, 
#                                b.dates->>'$.start' as epi_start 
#                         from entries p join entries e on e.parent_id = p.id
#                                                      and e.deleted_at is null
#                                        join entries b on b.parent_id = e.id
#                                                      and b.deleted_at is null
#                         where p.id = {pm_pgm_id}
#                           and b.dates->>'$.start' < {pm_max_start}
#                         order by 2 desc 
#                         limit 1
#                         ;", .con = pm_cpnm_db)
#   sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
# }

cpnm_img_get <- function(pm_img_id, pm_cpnm_db) {
  sql_stmt <- glue_sql("SELECT id FROM media 
                        where type = 'image'
                          and site_id in (1, 2)
                          and legacy_data->>'$.wp_post_id' = {pm_img_id};", 
                       .con = pm_cpnm_db)
  sql_res <- dbGetQuery(pm_cpnm_db, sql_stmt)
}

clock2db <- function(pm_clock_tib, pm_created_at, pm_site_id, pm_db) {
  
  # - some fresh programs have 2 main genres, so have 2 records; treat them separately: 'a' for all columns,
  #   and 'b' just for the extra genre
  cur_clock <- pm_clock_tib |> group_by(slot) |> mutate(tib_item = row_number()) |> ungroup()
  cur_clock_a <- cur_clock |> filter(tib_item == 1L)
  cur_clock_b <- cur_clock |> filter(tib_item == 2L)
  
  for (rn in seq_len(nrow(cur_clock_a))) {
    
    # . fresh episode & broadcast ----
    fresh_epi_bc <- cpnm_epi_bc_ins(pm_pgm_id = cur_clock_a$pgm_id[rn],
                                    pm_descr_NL = cur_clock_a$intro_NL[rn],
                                    pm_descr_EN = cur_clock_a$intro_EN[rn],
                                    pm_img_id = cur_clock_a$image_id[rn],
                                    pm_site_id = pm_site_id,
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
    
    # . catalogue key ----
    cck_res <- cpnm_cck_ins(pm_epi_id = fresh_epi_bc,
                            pm_cck = cur_clock_a$catalg_key[rn],
                            pm_cpnm_db = pm_db)
  }
}

# epi_replays <- function(pm_pgm_id, pm_genre, pm_max_ts, pm_cpnm_db) {
#   sqlstmt <- glue_sql("WITH episodes AS (
#          SELECT e.id,
#                 e.title->>'$.nl' AS epi_title
#          FROM entries e
#          WHERE e.parent_id = {pgm_id}
#            AND e.site_id = 1
#            AND e.deleted_at IS NULL
#      ),
#      broadcasts AS (
#          SELECT b.parent_id AS epi_id,
#                 max(b.dates->>'$.start') AS bc_start
#          FROM entries b JOIN episodes e ON e.id = b.parent_id
#          WHERE b.site_id = 1
#            AND b.deleted_at IS NULL
#          GROUP BY b.parent_id
#      ),
#      base as (
#          SELECT
#          e.id AS epi_id,
#          e.epi_title,
#          b.bc_start,
#          (
#              SELECT GROUP_CONCAT(ty.name->>'$.nl' ORDER BY ty.name->>'$.nl' SEPARATOR '-')
#              FROM taxonomables tb JOIN taxonomies ty ON ty.id = tb.taxonomy_id
#                                                     AND ty.type = 'genre'
#                                                     AND ty.site_id = 1
#                                                     AND ty.deleted_at IS NULL
#              WHERE tb.taxonomable_id = e.id
#                AND tb.taxonomable_type = 'episode'
#          ) AS genre_name
#          FROM episodes e LEFT JOIN broadcasts b ON b.epi_id = e.id
#      )
#      select * from base
#      where genre_name = {pm_genre} and bc_start < {pm_max_ts}
#      ORDER BY bc_start DESC
#      limit 1;", .con = pm_cpnm_db)
# }

# usage: ts_NL <- ymd_hms("2026-05-28 01:09:25", tz = "Europe/Amsterdam")
#        to_slot_key(ts_NL) # "do01-4"
to_slot_key <- function(ts_NL) {
  slot_week <- 1 + ((day(ts_NL) - 1) %/% 7) # not modulo (%%) but integer division (%/%)
  day_abbr <- c("ma", "di", "wo", "do", "vr", "za", "zo")
  wday_nr <- wday(ts_NL, week_start = 1)
  slot_weekday <- day_abbr[wday_nr]
  slot_hour <- hour(ts_NL)
  paste0(slot_weekday, str_pad(slot_hour, width = 2, side = "left", pad = "0"), "-", slot_week)
}

# append_chain_item <- function(pm_con,
#                               pm_label,
#                               pm_episode_entry_id,
#                               pm_clockfactory_job = NULL) {
#   dbWithTransaction(pm_con, {
#     chain_row <- dbGetQuery(
#       pm_con,
#       "
#       SELECT chain_id, next_position
#       FROM episode_chain
#       WHERE label = ?
#       FOR UPDATE
#       ",
#       params = list(pm_label)
#     )
#     
#     if (nrow(chain_row) == 0) {
#       dbExecute(
#         pm_con,
#         "
#         UPDATE episode_chain_seq
#         SET id = LAST_INSERT_ID(id + 1)
#         "
#       )
#       
#       chain_id <- dbGetQuery(
#         pm_con,
#         "SELECT LAST_INSERT_ID() AS chain_id"
#       )$chain_id[[1]]
#       
#       position <- 1L
#       
#       dbExecute(
#         pm_con,
#         "
#         INSERT INTO episode_chain (chain_id, label, next_position)
#         VALUES (?, ?, 2)
#         ",
#         params = list(chain_id, pm_label)
#       )
#     } else {
#       chain_id <- chain_row$chain_id[[1]]
#       position <- chain_row$next_position[[1]]
#       
#       dbExecute(
#         pm_con,
#         "
#         UPDATE episode_chain
#         SET next_position = next_position + 1
#         WHERE chain_id = ?
#         ",
#         params = list(chain_id)
#       )
#     }
#     
#     dbExecute(
#       pm_con,
#       "
#       INSERT INTO episode_chain_item
#         (chain_id, position, episode_entry_id, clockfactory_job)
#       VALUES (?, ?, ?, ?)
#       ",
#       params = list(
#         chain_id,
#         position,
#         pm_episode_entry_id,
#         pm_clockfactory_job
#       )
#     )
#     
#     tibble(
#       chain_id = chain_id,
#       position = position
#     )
#   })
# }

lookup_replay <- function(pm_chains,
                          pm_cur_chain,
                          pm_replay_target_slot,
                          pm_bc_type,
                          pm_nipperstudio,
                          pm_replay_week_start) {
  # prune current chain
  replay_candidates <- pm_chains |> filter(episode_chain == pm_cur_chain)
  flog.info("Lookup replay for %s, %s candidates", pm_cur_chain, nrow(replay_candidates), name = log_slug)
  
  replay_source <- if (nrow(replay_candidates) == 0) {
    "NOT FOUND"
  } else if (str_detect(pm_bc_type, "live")) {
    shortlist <- replay_candidates |> filter(ec_slot < pm_replay_week_start)
    
    if (nrow(shortlist) == 0) "NOT FOUND" else shortlist$episode_entry_id[1]
    
  } else if (pm_nipperstudio != "N") {
    shortlist_nips.1 <- replay_candidates |> filter(ec_slot <= pm_replay_target_slot - days(182L))
    shortlist_nips.2 <- replay_candidates |> filter(ec_slot < pm_replay_target_slot)
    shortlist <- if (nrow(shortlist_nips.1) > 0) shortlist_nips.1 else shortlist_nips.2
    
    if (nrow(shortlist) == 0) "NOT FOUND" else shortlist$episode_entry_id[1]
    
  } else {
    shortlist <- replay_candidates |> filter(ec_slot < pm_replay_target_slot)
    
    if (nrow(shortlist) == 0) "NOT FOUND" else shortlist$episode_entry_id[1]
  }
}

log_tibble <- function(x, label = deparse(substitute(x)), n = 20, width = 160) {
  x_tbl <- as_tibble(x)
  txt <- capture.output(print(x_tbl, n = n, width = width))
  flog.error("%s:\n%s", label, paste(txt, collapse = "\n"), name = log_slug)
}

# parse_rebuild_options <- function(args = commandArgs(trailingOnly = TRUE)) {
#   rebuild_options <- list(
#     make_option(
#       c("-d", "--date"),
#       type = "character",
#       default = NULL,
#       help = "'rebuild from'-date",
#       metavar = "character"
#     )
#   )
#   
#   opt_parser <- OptionParser(option_list = rebuild_options)
#   opts <- parse_args(opt_parser, args = args)
#   
#   if (is.null(opts$date)) {
#     stop(
#       "missing argument(s). Enter 'rebuild from'-date; use '-d' or --date'.",
#       call. = FALSE
#     )
#   }
#   
#   opts$date
# }

# For this site return the broadcast week boundary at or before this slot
# - uses R-convention for DAYS_OF_WEEK, not SQL-convention
bc_week_start <- function(pm_slot_start, pm_site_id) {
  h1 <- bc_week_start_hour(pm_site_id)
  candidate <- floor_date(pm_slot_start, "day") - 
               days((wday(pm_slot_start, week_start = R_DAYS_OF_WEEK$MONDAY) - R_DAYS_OF_WEEK$THURSDAY) %% 7L) + 
               hours(h1)
  if_else(candidate <= pm_slot_start, candidate, candidate - days(7L))
}

# get_rebuild_date <- function(pm_start) {
#   if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
#     rstudioapi::showPrompt(title = "Rebuild start date",
#                            message = "Rebuild from where? (yyyy-mm-dd HH:MM:SS)",
#                            default = as.character(pm_start))
#   } else {
#     readline("Rebuild from where? (yyyy-mm-dd HH:MM:SS) ")
#   }
# }

# make sure the clockprofile on GD is closed and circular: no gaps/overlaps, and stop_ts of last row = start_ts of first row
check_circular_sequence <- function(x) {
  days <- c(ma = 0, di = 1, wo = 2, do = 3, vr = 4, za = 5, zo = 6)
  week_minutes <- 7 * 24 * 60
  
  x |>
    mutate(
      row = row_number(),
      day = str_sub(slot_key, 1, 2),
      hour = as.integer(str_sub(slot_key, 3, 4)),
      minute_of_week = unname(days[day]) * 24 * 60 + hour * 60,
      
      next_slot_key = lead(slot_key, default = first(slot_key)),
      actual_next_minute = lead(minute_of_week, default = first(minute_of_week)),
      
      expected_next_minute = (minute_of_week + slot_minutes) %% week_minutes,
      
      problem = case_when(
        is.na(days[day]) ~ "invalid weekday abbreviation",
        is.na(hour) | hour < 0 | hour > 23 ~ "invalid hour",
        is.na(slot_minutes) | slot_minutes <= 0 ~ "invalid duration",
        actual_next_minute != expected_next_minute ~
          "gap, overlap, wrong slot_minutes, wrong slot_key, or wrong row order",
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(problem)) |>
    select(
      row,
      slot_key,
      slot_minutes,
      next_slot_key,
      expected_next_minute,
      actual_next_minute,
      problem
    )
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) y else x
}

parse_args <- function(args = commandArgs(trailingOnly = TRUE)) {
  out <- list()
  
  if (length(args) == 0) {
    return(out)
  }
  
  if (length(args) %% 2 != 0) {
    stop("Arguments should be supplied as `--key value`-pairs.", call. = FALSE)
  }
  
  for (i in seq(1, length(args), by = 2)) {
    key <- sub("^--", "", args[[i]])
    value <- args[[i + 1]]
    out[[key]] <- value
  }
  
  out
}

# is_rstudio_interactive <- function() {
#   interactive() && requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()
# }
# 
# prompt_value <- function(title, message, default = "") {
#   default <- if (is.null(default) || length(default) == 0 || is.na(default)) "" else as.character(default)
#   
#   if (is_rstudio_interactive()) {
#     return(
#       rstudioapi::showPrompt(
#         title = title,
#         message = message,
#         default = default
#       )
#     )
#   }
#   
#   value <- readline(
#     prompt = paste0(message, if (nzchar(default)) paste0(" [", default, "] ") else " ")
#   )
#   
#   if (nzchar(value)) value else default
# }

# get_site_id <- function(site_arg = NULL) {
#   
#   site_choice <- if (!is.null(site_arg) && nzchar(site_arg)) {
#     site_arg
#   } else if (interactive()) {
#     prompt_value(title = "Select site", message = "1: Concertzender, 2: World of Jazz")
#   } else {
#     stop("Missing required argument: --site", call. = FALSE)
#   }
#   
#   if (is.null(site)) {
#     stop("Invalid site choice. Use 1 or 2.", call. = FALSE)
#   }
#   
#   site
# }
# 
# get_interactive_start_date <- function(start_of_new_week) {
#   prompt_value(
#     title = "Rebuild start date",
#     message = "Rebuild from where? (yyyy-mm-dd HH:MM:SS)",
#     default = start_of_new_week
#   ) |>
#     as.POSIXct(tz = "Europe/Amsterdam")
# }

fetch_broadcasts <- function(pm_con) {
  dbGetQuery(pm_con, "select * from broadcasts where status == 'published'")
}

fetch_lacie <- function(pm_chain, pm_lacie_chains) {
  flog.info("fetching LaCie for chain = %s", pm_chain, name = log_slug)
  
  lacie <- pm_lacie_chains |> group_by(chain) |> 
    mutate(cur_rnk = row_number(),
           nxt_pos = max(pos) + cur_rnk) |> ungroup() |> 
    filter(cur_rnk == 1 & chain == pm_chain) 
  
  bot_lacie <- lacie |> select(ep_id, pos = nxt_pos)
  lacie_chains_upd <- pm_lacie_chains |> rows_update(by = "ep_id", y = bot_lacie) |> arrange(chain, pos)
  
  list(lacie_chains_upd, lacie)
}

separate_dt <- function(fn) {
  
  sep <- "[ _.-]"
  day <- "(?:ma|di|wo|do|vr|za|zo)"
  hour <- "(?:[01]?\\d|2[0-3])"
  
  hour_pat <- paste0(
    "^(?:",
    
    # Case 1: ordinal weekday, e.g. za2_18u
    day, "[1-5]", sep,
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    "|",
    
    # Case 2: weekday directly before hour, e.g. za17
    "(?!", day, "[1-5]", sep, ")",
    day,
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    "|",
    
    # Case 3: plain hour, e.g. 2100 or 0000
    "(", hour, ")(?:00|u)?(?=", sep, "|$)",
    
    ")"
  )
  
  tibble(raw = fn) |>
    mutate(
      ymd_chr = str_sub(raw, 1, 8),
      after_date = str_sub(raw, 10),
      m = str_match(after_date, hour_pat),
      hour = coalesce(m[, 2], m[, 3], m[, 4]),
      
      datetime_candidate = if_else(!is.na(hour),
                                   str_c(ymd_chr, " ", str_pad(hour, 2, pad = "0"), ":00:00"),
                                   NA_character_),
      
      datetime = ymd_hms(datetime_candidate, quiet = TRUE),
      
      datetime_chr = if_else(!is.na(datetime),
                             format(datetime, "%Y-%m-%d %H:%M:%S"),
                             NA_character_)
    ) |>
    pull(datetime_chr) 
}

# shrink & format playout ----
rgb <- function(r, g, b) {
  list(red = r / 255, green = g / 255, blue = b / 255)
}

get_sheet_id <- function(ss, sheet) {
  gs4_get(ss)$sheets |> filter(name == sheet) |> pull(id)
}

append_week <- function(ss, sheet, new_rows) {
  
  sheet_append(ss = ss, data = new_rows, sheet = sheet)
  
  current_data <- read_sheet(ss = ss, sheet = sheet) |>
    mutate(.sheet_row = row_number() + 1L,
           .run_number = parse_integer(str_remove(uitz_wk, "-"))) |>
    filter(!is.na(.run_number))
  
  runs <- current_data |> distinct(.run_number) |> arrange(.run_number) |> pull(.run_number)
  
  # keep latest 3
  if (length(runs) <= 3) {
    return(invisible(current_data |> select(-.sheet_row, -.run_number)))
  }
  
  oldest_runs <- runs[seq_len(length(runs) - 3)]
  rows_to_delete <- current_data |> filter(.run_number %in% oldest_runs) |> arrange(.sheet_row)
  actual_rows <- as.integer(rows_to_delete$.sheet_row)
  
  if (any(diff(actual_rows) != 1L)) {
    stop("Rows to delete are not contiguous. Refusing to delete.")
  }

  delete_range <- glue("{min(rows_to_delete$.sheet_row)}:{max(rows_to_delete$.sheet_row)}")
  range_delete(ss = ss, sheet = sheet, range = delete_range, shift = "up")
  
  after_cleanup_raw <- read_sheet(ss, sheet = sheet)
  sheet_resize(ss = ss, sheet = sheet, nrow = nrow(after_cleanup_raw) + 1L, exact = TRUE)
  
  # # re-read because row numbers may have shifted
  # after_cleanup <- read_sheet(ss, sheet = sheet) |>
  #   mutate(
  #     .sheet_row = row_number() + 1L,
  #     .run_number = parse_integer(str_remove(uitz_wk, "-"))) |>
  #   filter(!is.na(.run_number))
  # 
  # newest_run <-after_cleanup |>
  #   summarise(.run_number = max(.run_number, na.rm = TRUE)) |>
  #   pull(.run_number)
  # 
  # new_week_rows <- after_cleanup |>
  #   filter(.run_number == newest_run) |>
  #   arrange(.sheet_row)
  # 
  # actual_new_rows <- as.integer(new_week_rows$.sheet_row)
  # 
  # if (length(actual_new_rows) > 1L && any(diff(actual_new_rows) != 1L)) {
  #   stop("New week rows are not contiguous. Refusing to format.", call. = FALSE)
  # }
  # 
  # start_row_index <- min(actual_new_rows) - 1L
  # end_row_index   <- max(actual_new_rows)
  # sheet_id <- sheet_properties(ss) |> filter(name == sheet) |> pull(id)
  # checkbox_col_index <- match("gereed", names(after_cleanup |> select(-.sheet_row, -.run_number))) - 1L
  # 
  # req <- request_generate(
  #   endpoint = "sheets.spreadsheets.batchUpdate",
  #   params = list(
  #     spreadsheetId = as_sheets_id(ss),
  #     requests = list(
  #       list(
  #         repeatCell = list(
  #           range = list(
  #             sheetId = sheet_id,
  #             startRowIndex = start_row_index,
  #             endRowIndex = end_row_index
  #           ),
  #           cell = list(
  #             userEnteredFormat = list(
  #               textFormat = list(
  #                 fontFamily = "Roboto"
  #               )
  #             )
  #           ),
  #           fields = "userEnteredFormat.textFormat.fontFamily"
  #         )
  #       ),
  #       list(
  #         repeatCell = list(
  #           range = list(
  #             sheetId = sheet_id,
  #             startRowIndex = start_row_index,
  #             endRowIndex = end_row_index,
  #             startColumnIndex = checkbox_col_index,
  #             endColumnIndex = checkbox_col_index + 1L
  #           ),
  #           cell = list(
  #             dataValidation = list(
  #               condition = list(
  #                 type = "BOOLEAN"
  #               ),
  #               strict = TRUE
  #             )
  #           ),
  #           fields = "dataValidation"
  #         )
  #       )
  #     )
  #   )
  # )
  # 
  # resp <- request_make(req)
  # gargle::response_process(resp)
  
  invisible(rows_to_delete)
}
