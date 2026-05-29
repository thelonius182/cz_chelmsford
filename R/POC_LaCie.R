pacman::p_load(DBI, RSQLite, fs, dplyr, stringr, readr)


has_valid_date_prefix <- function(file_names) {
  date_part <- str_sub(file_names, 1, 8)
  parsed <- ymd(date_part, quiet = TRUE)
  
  str_detect(file_names, "^\\d{8}\\D") &
    !is.na(parsed) &
    format(parsed, "%Y%m%d") == date_part
}

# get_bc_date <- function(file_names) {
#   date_part <- str_sub(file_names, 1, 8)
#   ymd(date_part, quiet = TRUE)
# }

get_chain <- function(qfn, root_flr) {
  path_dir(path_rel(qfn, root_flr))
}

sync_playlist_stack <- function(con_sqlite, lacie_root, audio_extensions) {
  
  if (!dir_exists(lacie_root)) {
    stop("Remote folder is not reachable: ", lacie_root)
  }
  
  current_scan <- dir_info(
    path = lacie_root,
    recurse = TRUE,
    type = "file",
    fail = FALSE
  ) |>
    mutate(
      qfn = make_relative_path(path, lacie_root),
      extension = str_to_lower(path_ext(path))
    ) |>
    filter(extension %in% audio_extensions) |>
    transmute(
      qfn,
      full_path = as.character(path),
      file_name = path_file(path)
    ) |>
    arrange(qfn)
  
  dbWithTransaction(con_sqlite, {
    
    dbWriteTable(con_sqlite, "current_scan", current_scan, temporary = TRUE, overwrite = TRUE)
    
    # Remove rows for files that no longer exist.
    dbExecute(con_sqlite, "DELETE FROM lacie_stack WHERE qfn NOT IN (SELECT qfn FROM current_scan)"
    )
    
    # Append new files to the bottom of the stack.
    dbExecute(con_sqlite, "INSERT INTO lacie_stack (position,
                                                    qfn)
                           SELECT
                             (
                               SELECT COALESCE(MAX(position), 0)
                               FROM lacie_stack
                             ) + ROW_NUMBER() OVER (ORDER BY qfn),
                             c.qfn
                           FROM current_scan c
                           WHERE c.qfn NOT IN (SELECT qfn FROM lacie_stack)
                           ORDER BY c.relative_path")
    
    dbExecute(con_sqlite, "DROP TABLE current_scan")
  })
  
  invisible(scan)
}

compact_stack_positions <- function(con_sqlite) {
  
  dbWithTransaction(con_sqlite, {
    
    dbExecute(con_sqlite, "CREATE TEMPORARY TABLE compacted_stack AS
                       SELECT
                         ROW_NUMBER() OVER (ORDER BY position) AS position,
                         qfn,
                         full_path,
                         file_name
                       FROM lacie_stack
                       ORDER BY position")
    
    dbExecute(con_sqlite, "DELETE FROM lacie_stack")
    
    dbExecute(con_sqlite, "INSERT INTO lacie_stack (position,
                                             qfn,
                                             full_path,
                                             file_name)
                    SELECT position,
                           qfn,
                           full_path,
                           file_name
                    FROM compacted_stack")
    
    dbExecute(con_sqlite, "DROP TABLE compacted_stack")
  })
}

stack_needs_compaction <- function(con_sqlite) {
  stats <- dbGetQuery(con_sqlite, "SELECT COUNT(*) AS n,
                                   COALESCE(MIN(position), 0) AS min_position
                            FROM lacie_stack")
  
  stats$n > 0 && stats$min_position > stats$n
}

# - - - - - - - - - - - - - - - - 
#     ---- >> MAIN << ----
# - - - - - - - - - - - - - - - - 


validated_scan_list <- scan_list |> 
  filter(has_valid_date_prefix(file_name)) |> 
  mutate(chain = LACIE_CHAINS[get_chain(full_path, lacie_root)]) |> 
  group_by(chain) |> 
  mutate(pos = row_number()) |> 
  ungroup() |> 
  select(-relative_path)

lacie_root <- "/mnt/muw/WoJ-hh"
audio_extensions <- c("wav", "aif", "aiff", "m4a")
LACIE_CHAINS <- list(
  "Door de Mazen van het Net" = "MAZEN",
  "Duke Ellington" = "DUKE",
  "Groove and Grease" = "GROOVE"
)
LACIE_CHAINS$`Door de Mazen van het Net`

con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
on.exit(dbDisconnect(con_sqlite), add = TRUE)

dbExecute(con_sqlite, "create table if not exists lacie_stack (id    binary(16) primary key,   
                                                               chain text not null,
                                                               pos   integer not null,
                                                               qfn   text not null)"
)

"SELECT position, qfn, full_path
FROM lacie_stack
ORDER BY position
LIMIT 1;
"

"UPDATE lacie_stack
SET position = (
  SELECT COALESCE(MAX(position), 0) + 1
  FROM lacie_stack
)
WHERE qfn = ?;"


if (stack_needs_compaction(con_sqlite)) compact_stack_positions(con_sqlite)

# iho <- read_lines("/mnt/muw/WoJ-hh/inhoudsopgave.txt") |> as_tibble()
# dum <- read_lines("/mnt/muw/WoJ-hh/dummy-content.txt")
# 
# duke <- iho |> filter(str_detect(value, "duke"))
# 
# flr <- "/mnt/muw/WoJ-hh/Duke Ellington/"
# for (fn in duke$value) {
#   qfn <- paste0(flr, fn)
#   file_create(path = qfn)
#   write_lines(x = dum, file = qfn)
# }
# 
# iho.1 <- iho |> filter(str_detect(value, "groove"))
# flr <- "/mnt/muw/WoJ-hh/Groove and Grease/"
# for (fn in iho.1$value) {
#   qfn <- paste0(flr, fn)
#   file_create(path = qfn)
#   write_lines(x = dum, file = qfn)
# }
# 
# iho.1 <- iho |> filter(str_detect(value, "mazen"))
# flr <- "/mnt/muw/WoJ-hh/Door de Mazen van het Net/"
# for (fn in iho.1$value) {
#   qfn <- paste0(flr, fn)
#   file_create(path = qfn)
#   write_lines(x = dum, file = qfn)
# }

