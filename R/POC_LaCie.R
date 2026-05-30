pacman::p_load(DBI, RSQLite, fs, dplyr, stringr, readr, lubridate, purrr, uuid)

has_valid_date_prefix <- function(fn) {
  date_part <- str_sub(fn, 1, 8)
  parsed <- ymd(date_part, quiet = TRUE)
  
  str_detect(fn, "^\\d{8}\\D") &
    !is.na(parsed) &
    format(parsed, "%Y%m%d") == date_part
}

get_chain <- function(pm_qfn, pm_root) {
  path_dir(path_rel(pm_qfn, pm_root))  
}

scan_fs <- function(root) {
  dir_ls(root, recurse = TRUE, type = "file") |>
    tibble(qfn = _) |>
    mutate(fn = path_file(qfn)) |> 
    filter(str_to_lower(path_ext(fn)) %in% c("wav", "aif", "aiff", "m4a") & 
             has_valid_date_prefix(fn)) |> 
    mutate(lacie_dir = path_dir(path_rel(qfn, root)),
           lid = UUIDgenerate(n = n(), use.time = FALSE)) |> 
    inner_join(lacie_chains, by = join_by(lacie_dir)) |> 
    group_by(chain) |>
    mutate(pos = row_number()) |>
    ungroup() |>
    arrange(chain, pos) |> 
    select(lid, chain, pos, fn)
}

sync_playlist_stack <- function(con_sqlite, lacie_root, audio_extensions) {
  
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
                           ORDER BY c.qfn")
    
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
lacie_root <- "/mnt/muw/WoJ-hh"

if (!dir_exists(lacie_root)) {
  stop("Remote folder is not reachable: ", lacie_root)
}

lacie_chains <- tibble(
  lacie_dir = c("Door de Mazen van het Net", "Duke Ellington", "Groove and Grease"),
  chain     = c("LC_MAZEN",                  "LC_DUKE",        "LC_GROOVE")
)

fs_lacies <- scan_fs(lacie_root)
con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
on.exit(dbDisconnect(con_sqlite), add = TRUE)


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

