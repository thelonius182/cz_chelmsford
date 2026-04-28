
# downloads GD ----
# . trigger GD-auth
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")

# . get episode chain labels from GD
path_chains_cz <- "/home/lon/R_projects/cz_chelmsford/resources/chains_cz.xlsx"
drive_download(file = cz_get_url("chains_cz"), overwrite = T, path = path_chains_cz)
df_raw_nipper_fresh <- cz_extract_sheet(path_chains_cz, sheet_name = "nipper_fresh")
df_nipper_fresh <- df_raw_nipper_fresh |> mutate(wp_title = str_to_lower(wp_title))
df_raw_chains <- cz_extract_sheet(path_chains_cz, sheet_name = "wp_chains")
df_clean_chains <- df_raw_chains |> filter(episode_chain != "#NONE#")

# rerun query on Nipper first
wp_slot_alloc_raw <- read_tsv(file = "resources/clockfactory_slot_allocation.tsv", col_types = "cccc")

wp_slot_alloc <- wp_slot_alloc_raw |> 
  mutate(wp_post_id = as.integer(ID),
         wp_post_date = force_tz(ymd_hms(post_date), tzone = "Europe/Amsterdam"),
         wp_title = str_to_lower(wp_title)) |> 
  select(wp_title, wp_slot, wp_post_date, wp_post_id) |> arrange(wp_post_id)

# split chain tibbles in conditional (a) and unconditional ones (b), to simplify joining with slots
df_chains_a <- df_clean_chains |> filter(!is.na(wp_slot))
df_joined_slots_a <- wp_slot_alloc |> inner_join(df_chains_a, by = join_by(wp_title, wp_slot)) |> 
  filter(wp_post_date < start_ts) |> select(-wp_slot)
df_chains_b <- df_clean_chains |> filter(is.na(wp_slot))
df_joined_slots_b <- wp_slot_alloc |> inner_join(df_chains_b, by = join_by(wp_title)) |> 
  filter(wp_post_date < start_ts) |> select(-wp_slot.x, -wp_slot.y)

df_joined_slots_backfill <- df_joined_slots_a |> bind_rows(df_joined_slots_b) |> arrange(episode_chain, wp_post_date)

sql_stmt <- "select legacy_id,
       id as episode_id,
       parent_id as pgm_id,
       title_nl
from entries
where type = 'episode'
  and legacy_id >= 500000
order by 1"
df_linked_episodes <- dbGetQuery(con, sql_stmt)

df_backfill_a <- df_joined_slots_backfill |> left_join(df_linked_episodes, by = join_by(wp_post_id == legacy_id)) |> 
  filter(!is.na(episode_id)) |> arrange(episode_chain, wp_post_date) |> select(episode_id, wp_post_date, episode_chain)

STEP <- list(
  FACTORY = 10L,
  EDITOR  = 20L,
  DESK    = 30L,
  PLAYOUT = 99L
)

step_lookup <- tibble(
  step = c(10L, 20L, 30L, 99L),
  step_name = c("FACTORY", "EDITOR", "DESK", "PLAYOUT")
)

# jobs <- tibble(id = 1:5,
#                completed = c(STEP$FACTORY, STEP$EDITOR, STEP$DESK, STEP$PLAYOUT, STEP$EDITOR))
# 
# planned_jobs <- jobs |> filter(completed < STEP$PLAYOUT) |> 
#   left_join(step_lookup, by = join_by(completed == step))


sql_stmt <- "select * from episode_chain_item"
db_existing_chain_items <- dbGetQuery(con, sql_stmt)
df_backfill_b <- df_backfill_a |> anti_join(db_existing_chain_items, by = join_by(episode_id == episode_entry_id))

job_id <- UUIDgenerate(use.time = FALSE) 

for (rn in seq_len(nrow(df_backfill_b))) {
  result <- append_chain_item(pm_con = con,
                              pm_label = df_backfill_b$episode_chain[rn],
                              pm_episode_entry_id = df_backfill_b$episode_id[rn],
                              pm_clockfactory_job = job_id)
}  

