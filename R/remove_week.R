# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# remove all rows from week
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

begin_ts <- force_tz(ymd_hms("2026-04-23 13:00:00", quiet = T), tzone = "Europe/Amsterdam")
end_ts  <- begin_ts + days(7L) - minutes(1L)

sql_result <- dbExecute(con, "DROP TEMPORARY TABLE IF EXISTS to_delete")

# prep episode id's ----
sql_stmt <- glue_sql("
  CREATE TEMPORARY TABLE to_delete (
    id char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci PRIMARY KEY
  ) ENGINE = MEMORY
  AS
  select distinct e.id
  from entries e join entries b on b.parent_id = e.id
                               and b.type = 'broadcast'
                               and b.deleted_at is null
  where e.type = 'episode'
    and e.deleted_at is null
    and b.dates->>'$.start' between {fmt_ts(begin_ts)} and {fmt_ts(end_ts)};", .con = con)
n_to_delete <- dbExecute(con, sql_stmt)

if (n_to_delete == 0) stop("no rows to delete")

persist_now <- with_tz(now(), "UTC")
flog.info(str_glue("Removing {fmt_ts(begin_ts)}, generic 'deleted_at' = {logfmt_ts(persist_now)}"), name = "clof")

# start transaction
dbBegin(con)

tryCatch({
  # remove taxonomables ----
  # no delete flag in this table
  sql_result <- dbExecute(con, "delete FROM taxonomables where taxonomable_id in (select id from to_delete)")
  
  # hide episodes ----
  sql_stmt <- glue_sql("
     update entries set deleted_at = {fmt_ts(persist_now)}
     where deleted_at is null
       and type = 'episode'
       and id in (select id from to_delete);", .con = con)
  sql_result <- dbExecute(con, sql_stmt)
  
  # hide broadcasts ----
  sql_stmt <- glue_sql("
     update entries set deleted_at = {fmt_ts(persist_now)}
     where deleted_at is null
       and type = 'broadcast'
       and parent_id in (select id from to_delete)", .con = con)
  sql_result <- dbExecute(con, sql_stmt)
  
  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  stop(e)
})

sql_result <- dbExecute(con, "DROP TEMPORARY TABLE to_delete")
