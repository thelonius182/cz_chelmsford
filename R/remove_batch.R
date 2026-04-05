# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# remove all rows inserted in 'this_run' ClockFactory-run
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Use either one; glue_sql will substitute correctly using single quotes
# this_run <- job_id
this_run <- "9d7a5e3e-6271-4ba5-b94a-d4dbcd22fb2d"

sql_result <- dbExecute(con, "DROP TEMPORARY TABLE IF EXISTS to_delete")

sql_stmt <- glue_sql("
  CREATE TEMPORARY TABLE to_delete (
    id char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci PRIMARY KEY
  ) ENGINE = MEMORY
  AS
  SELECT id
  FROM entries
  WHERE attributes->>'$.job_id' = {this_run}", .con = con)
sql_result <- dbExecute(con, sql_stmt)

if (sql_result == 0) stop("no rows to delete")

dbBegin(con)

tryCatch({
  sql_result <- dbExecute(con, "
    DELETE txb
    FROM taxonomables txb
    JOIN to_delete d ON d.id = txb.taxonomable_id
  ")
  
  sql_result <- dbExecute(con, "
    DELETE txy
    FROM taxonomies txy
    JOIN to_delete d ON d.id = txy.id
  ")
  
  sql_result <- dbExecute(con, "
    DELETE e
    FROM entries e
    JOIN to_delete d ON d.id = e.id
  ")
  
  sql_result <- dbExecute(con, "
    DELETE e
    FROM entries e
    JOIN to_delete d ON d.id = e.parent_id
  ")
  
  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  stop(e)
})

sql_result <- dbExecute(con, "DROP TEMPORARY TABLE to_delete")
