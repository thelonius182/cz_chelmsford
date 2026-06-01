# pacman::p_load(DBI, RSQLite, fs, dplyr, stringr, readr, lubridate, purrr, uuid)

has_valid_date_prefix <- function(fn) {
  
  date_part <- str_sub(fn, 1, 8)
  parsed <- ymd(date_part, quiet = TRUE)
  
  str_detect(fn, "^\\d{8}\\D") &
    !is.na(parsed) &
    format(parsed, "%Y%m%d") == date_part
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
    select(chain, pos, fn, lid)
}

sync_db <- function(con, fs) {
  
  db <- dbGetQuery(con, "select * from lacie_stack;")
  
  dbWithTransaction(con, {
    
    dbw <- dbWriteTable(con, name = "fs_lacies", value = fs, temporary = TRUE, overwrite = TRUE)

    # Remove rows of deleted files
    dbe <- dbExecute(con, "delete from lacie_stack
                           where not exists (select 'soep-met-ballen' from fs_lacies fs
                                             where fs.chain = lacie_stack.chain
                                               and fs.fn    = lacie_stack.fn);")    
    
    # Register new files at the bottom of the stack.
    any_ins <- fs |> 
      rename_with(~ paste0("fs_", .x)) |> 
      left_join(db, by = join_by(fs_chain == chain, fs_fn == fn)) |> 
      mutate(pos = coalesce(pos, 0L)) |> 
      arrange(fs_chain, pos, fs_pos) |> 
      group_by(fs_chain) |> 
      mutate(cur_rnk = row_number(),
             bot_pos = max(pos) + cur_rnk) |> 
      ungroup() |> 
      filter(is.na(lid))
    
    if (nrow(any_ins) > 0) {
      
      db_ins <- any_ins |> select(lid = fs_lid, 
                                  chain = fs_chain,
                                  fn = fs_fn,
                                  pos = bot_pos)
      
      dbAppendTable(con, "lacie_stack", db_ins)    
    }
    
    dbExecute(con, "drop table fs_lacies")
  })
  
  # compact_db(con)
}

# - - - - - - - - - - - - - - - - 
#     ---- >> MAIN << ----
# - - - - - - - - - - - - - - - - 
lacie_root <- "/mnt/muw/WoJ-hh"

if (!dir_exists(lacie_root)) {
  stop("Remote folder is not reachable: ", lacie_root)
}

lacie_chains <- tibble(
  lacie_dir = c("Door de Mazen van het Net", 
                "Tussen Swing & Bop", 
                "Deep Jazz", 
                "Vocal Jazz", 
                "Three of a Kind", 
                "Jazz Piano", 
                "House of Hard Bop", 
                "Duke Ellington", 
                "Groove and Grease"),
  chain     = c("L_MAZEN",
                "L_SWING",
                "L_DEEPJ",
                "L_VOCAL",
                "L_3KIND",
                "L_PIANO",
                "L_HABOP",
                "L_DUKEE",
                "L_GROOV")
)
