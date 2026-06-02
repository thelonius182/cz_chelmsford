# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# tools for handling files intially stored on external archive drives manufacured by the 
# LaCie company. Now found on CZ-NAS (Synology)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 
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
           ep_id = UUIDgenerate(n = n(), use.time = FALSE)) |> 
    inner_join(lacie_chains, by = join_by(lacie_dir)) |> 
    group_by(chain) |>
    mutate(pos = row_number()) |>
    ungroup() |>
    arrange(chain, pos) |> 
    select(chain, pos, fn, ep_id)
}

sync_db <- function(con_sqlite, fs) {
  
  db <- dbGetQuery(con_sqlite, "select * from lacie_stack;")
  
  dbWithTransaction(con_sqlite, {
    
    dbw <- dbWriteTable(con_sqlite, name = "fs_lacies", value = fs, temporary = TRUE, overwrite = TRUE)

    # Remove rows of deleted files
    dbe <- dbExecute(con_sqlite, "delete from lacie_stack
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
      filter(is.na(ep_id))
    
    if (nrow(any_ins) > 0) {
      
      db_ins <- any_ins |> select(ep_id = fs_lid, 
                                  chain = fs_chain,
                                  fn = fs_fn,
                                  pos = bot_pos)
      
      dbAppendTable(con_sqlite, "lacie_stack", db_ins)    
    }
    
    dbExecute(con_sqlite, "drop table fs_lacies")
  })
  
  # compact_db(con_sqlite)
}

lacie_chains <- tibble(
  lacie_dir = c("Door de Mazen van het Net", 
                "Tussen Swing en Bop", 
                "Deep Jazz", 
                "Vocal Jazz", 
                "Three of a Kind", 
                "Jazz Piano", 
                "House of Hard Bop", 
                "Duke Ellington", 
                "Groove & Grease"),
  chain     = c("MAZEN",
                "SWIBOP",
                "DEEPJZZ",
                "VOCALJZ",
                "TOAK",
                "JZPIANO",
                "HOUSEBOP",
                "DUKE",
                "GROOV")
)

lacie_episodes <- function(con_mysql) {
  sql_stmt <- "select min(b.dates->>'$.start') as bc_start, e.id as episode_id, p.title_nl
               from entries p 
                        join entries e on e.parent_id = p.id
                                      and e.type = 'episode' 
                                      and e.deleted_at is null 
                        join entries b on b.parent_id = e.id
                                      and b.type = 'broadcast' 
                                      and b.deleted_at is null 
                                      and b.site_id = 1
               where p.type = 'program' 
                 and p.deleted_at is null 
                 and p.title_nl regexp 'duke|deep|groove|house|jazz piano|mazen|three|tussen swing|vocal jazz'
               group by e.id, p.title_nl
               order by 3, 1;"
  dbGetQuery(con_mysql, sql_stmt) |> left_join(lacie_chains, by = join_by(title_nl == lacie_dir))
}
