# insert program ----
ins_result <- 
  cpnm_pgm_ins(pm_title_NL = "Schatgraven",
               pm_title_EN = "Treasure Hunting",
               pm_descr_NL = "Een duik in de archieven om luisteraars te verrassen met verborgen meesterwerken en vergeten opnames.",
               pm_descr_EN = "A dive into the archives to surprise listeners with hidden masterpieces and forgotten recordings.",
               pm_cpnm_db = con_cpnm)

# fix missing editor(s) ----
for (cur_editor in ds_missing$redacteurs) {
  cpnm_edi_ins(pm_name_NL = cur_editor, pm_cpnm_db = con_cpnm)
}

# WJ: append missing colofons ----
sql_stmt <- "with tlg as (select name->>'$.nl' as editor_lg 
			 from taxonomies
             where legacy_type = 'programma_maker'),
tcf as (select name->>'$.nl' as editor_cf, id as cf_id
        from taxonomies
        where type = 'colofon')
select * from tlg left join tcf on tcf.editor_cf = tlg.editor_lg
order by 1
;"
cf_editors <- dbGetQuery(con_cpnm, sql_stmt)
missing_cfs <- tbl_w_ids_3 |> left_join(cf_editors, by = join_by(redacteurs == editor_lg)) |> 
  select(redacteurs, editor_cf) |> distinct() |> filter(is.na(editor_cf))

for (cur_editor in missing_cfs$redacteurs) {
  ins_cpnm_editor(pm_name_NL = cur_editor, pm_cpnm_db = con_cpnm)
}

for (cur_editor in woj_schedule_w_ids_missing$redacteurs) {
  ins_cpnm_editor(pm_name_NL = cur_editor, pm_cpnm_db = con_cpnm)
}

# fix missing EN in WJ ----
fix_these_pgms <- woj_schedule_w_ids.5 |> 
  select(pgm_id, titel_NL, titel_EN) |> distinct() |> arrange(titel_NL)

for (rn in seq_len(nrow(fix_these_pgms))) {
  tit_NL <- fix_these_pgms$titel_NL[rn]
  tit_EN <- fix_these_pgms$titel_EN[rn]
  slu_NL <- as_slug(tit_NL)
  slu_EN <- as_slug(tit_EN)
  cur_id <- fix_these_pgms$pgm_id[rn]
  qry <- glue_sql("update entries 
                   set title = JSON_OBJECT('nl', {tit_NL},
                                           'en', {tit_EN}),
                       slug = JSON_OBJECT('nl', {slu_NL},
                                          'en', {slu_EN}),
                       user_id = 5,
                       updated_at = NOW()
                   where id = {cur_id};", 
                  .con = con_cpnm)
  sql_result <- dbExecute(conn = con_cpnm, statement = qry)
}

fix_these_editors <- woj_schedule_w_ids.5 |> select(ty_editor_id, redacteurs) |> distinct() |> arrange(redacteurs)

for (rn in seq_len(nrow(fix_these_editors))) {
  edi_NL <- fix_these_editors$redacteurs[rn]
  slu_NL <- as_slug(edi_NL)
  cur_id <- fix_these_editors$ty_editor_id[rn]
  qry <- glue_sql("update taxonomies 
                   set name = JSON_OBJECT('nl', {edi_NL},
                                           'en', {edi_NL}),
                       slug = JSON_OBJECT('nl', {slu_NL},
                                          'en', {slu_NL}),
                       user_id = 5,
                       updated_at = NOW()
                   where id = {cur_id};", 
                  .con = con_cpnm)
  sql_result <- dbExecute(conn = con_cpnm, statement = qry)
}

# fix missing EN in CZ ----
fix_these_pgms <- tbl_w_ids_3 |> 
  select(pgm_id, titel_NL, titel_EN) |> distinct() |> arrange(titel_NL)

for (rn in seq_len(nrow(fix_these_pgms))) {
  tit_NL <- fix_these_pgms$titel_NL[rn]
  tit_EN <- fix_these_pgms$titel_EN[rn]
  slu_NL <- as_slug(tit_NL)
  slu_EN <- as_slug(tit_EN)
  cur_id <- fix_these_pgms$pgm_id[rn]
  qry <- glue_sql("update entries 
                   set title = JSON_OBJECT('nl', {tit_NL},
                                           'en', {tit_EN}),
                       slug = JSON_OBJECT('nl', {slu_NL},
                                          'en', {slu_EN}),
                       user_id = 5,
                       updated_at = NOW()
                   where id = {cur_id};", 
                  .con = con_cpnm)
  sql_result <- dbExecute(conn = con_cpnm, statement = qry)
}

fix_these_editors <- tbl_w_ids_3 |> select(ty_editor_id, redacteurs) |> distinct() |> arrange(redacteurs)

for (rn in seq_len(nrow(fix_these_editors))) {
  edi_NL <- fix_these_editors$redacteurs[rn]
  slu_NL <- as_slug(edi_NL)
  cur_id <- fix_these_editors$ty_editor_id[rn]
  qry <- glue_sql("update taxonomies 
                   set name = JSON_OBJECT('nl', {edi_NL},
                                           'en', {edi_NL}),
                       slug = JSON_OBJECT('nl', {slu_NL},
                                          'en', {slu_NL}),
                       user_id = 5,
                       updated_at = NOW()
                   where id = {cur_id};", 
                  .con = con_cpnm)
  sql_result <- dbExecute(conn = con_cpnm, statement = qry)
}

# CZ+WJ: fix EN-names in programs/slugs <2026-04-05>
df_wpgidsinfo_lc <- df_raw_wpgidsinfo |> mutate(titel_nl_lc = str_to_lower(`titel-NL`))
fix_these <- program_titles |> left_join(df_wpgidsinfo_lc, by = join_by(titel_nl_lc)) |> 
  filter(!is.na(`key-modelrooster`)) |> 
  select(pgm_id, titel_NL = `titel-NL`, titel_EN = `titel-EN`) |> distinct() |> arrange(titel_NL)

for (rn in seq_len(nrow(fix_these))) {
  tit_NL <- fix_these$titel_NL[rn]
  tit_EN <- fix_these$titel_EN[rn]
  slu_NL <- as_slug(tit_NL)
  slu_EN <- as_slug(tit_EN)
  cur_id <- fix_these$pgm_id[rn]
  qry <- glue_sql("update entries 
                   set title = JSON_OBJECT('nl', {tit_NL},
                                           'en', {tit_EN}),
                       slug = JSON_OBJECT('nl', {slu_NL},
                                          'en', {slu_EN}),
                       user_id = 5,
                       updated_at = NOW()
                   where id = {cur_id};", 
                  .con = con_cpnm)
  sql_result <- dbExecute(conn = con_cpnm, statement = qry)
}

# add missing episodes to clock ----
qry <- "with broadcasts as (
    select cast(b.dates->>'$.start' as datetime) as bc_start,
           cast(b.dates->>'$.end'   as datetime) as bc_stop,
           e.title_nl,
           e.id as episode_entry_id
    from entries b join entries e on e.id = b.parent_id
                                 and e.deleted_at is null
                                 and e.type = 'episode'
    where cast(b.dates->>'$.start' as datetime) between '2026-05-07 12:50:00' and '2026-05-14 12:50:00'
      and b.deleted_at is null
      and b.type = 'broadcast'
      and b.site_id = 1
    ),
    ordered as (
        select bc_start,
               bc_stop,
               lead(bc_start) over (order by bc_start) as next_bc_start,
               title_nl,
               episode_entry_id
        from broadcasts
    )
    select bc_start,
           bc_stop,
           next_bc_start,
           title_nl,
           episode_entry_id
    from ordered
    where next_bc_start is not null
      and bc_stop = next_bc_start
    order by bc_start;"
cz_week <- dbGetQuery(con_cpnm, qry) |> mutate(bc_start = force_tz(bc_start, tzone = tz_am)) |> select(slot = bc_start,
                                                                                                  episode_entry_id)
cz_replays <- df_new_episodes_replay |> select(slot) |> inner_join(cz_week, by = join_by(slot))
df_clock_cz.3 <- df_clock_cz.2 |> rows_update(cz_replays, by = "slot")

n <- nrow(df_new_episodes_replay)
slot <- df_new_episodes_replay$slot
episode_entry_id <- character(n)

for (rn in seq_len(n)) {
  episode_entry_id[rn] <- lookup_replay(
    pm_chains = df_chains,
    pm_cur_chain = df_new_episodes_replay$episode_chain[rn],
    pm_replay_slot = df_new_episodes_replay$slot[rn],
    pm_offset = df_new_episodes_replay$replay_offset[rn]
  )
}

sav_tbl <- tibble(
  slot = slot,
  episode_entry_id = episode_entry_id
)

tib_episode_catlg_keys <- episode_chains |> inner_join(df_clockcatalogue, by = join_by(episode_chain)) |> 
  select(ep_id = episode_entry_id, ep_catkey = catalg_key) 
sql_sts <- dbAppendTable(conn = con_cpnm, name = "episode_catlg_keys", value = tib_episode_catlg_keys)

con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
dbDisconnect(con_sqlite)

dbExecute(con_sqlite, "drop table lacie_stack;")
dbExecute(con_sqlite, "create table lacie_stack (ep_id        text    primary key,
                                                 chain        text    not null,
                                                 fn           text    not null,
                                                 title        text    not null,
                                                 bc_start_chr text    not null,
                                                 pos          integer not null,
                                                 unique (chain, fn));"
)

fs_lacies <- scan_fs(lacie_root)
sdb <- sync_db(con_cpnm = con_sqlite, fs = fs_lacies)
db_lacies <- dbGetQuery(con_sqlite, "select * from lacie_stack order by chain, pos;")

dbAppendTable(conn = con_sqlite, name = "lacie_stack", value = fs_lacies)
db_lacies <- dbGetQuery(con_sqlite, "select * from lacie_stack order by chain, pos;")

top_lacies <- db_lacies |> 
  group_by(chain) |> 
  mutate(cur_rnk = row_number(),
         nxt_pos = max(pos) + cur_rnk) |> 
  ungroup() |> 
  filter(cur_rnk == 1) 

bot_lacies <- top_lacies |> select(ep_id, pos = nxt_pos)
db_lacies_upd <- db_lacies |> rows_update(by = "ep_id", y = bot_lacies) |> arrange(chain, pos)

# init LaCie-chains ----
iho <- read_lines("/mnt/muw/WoJ-hh/inhoudsopgave2.txt") |> tibble(fn = _)
dum <- read_lines("/mnt/muw/WoJ-hh/dummy-content.txt")

write_dummies <- function(flr, iho) {
  for (fn in iho$fn) {
    qfn <- paste0(flr, fn)
    file_create(path = qfn)
    write_lines(x = dum, file = qfn)
  }
}

iho.1 <- iho |> filter(str_detect(fn, "duke"))
flr <- "/mnt/muw/WoJ-hh/Duke Ellington/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "groove"))
flr <- "/mnt/muw/WoJ-hh/Groove & Grease/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "house"))
flr <- "/mnt/muw/WoJ-hh/House of Hard Bop/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "piano"))
flr <- "/mnt/muw/WoJ-hh/Jazz Piano/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "mazen"))
flr <- "/mnt/muw/WoJ-hh/Mazen/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "three"))
flr <- "/mnt/muw/WoJ-hh/Three of a Kind/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "swing"))
flr <- "/mnt/muw/WoJ-hh/Tussen Swing en Bop/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "vocal"))
flr <- "/mnt/muw/WoJ-hh/Vocal Jazz/"
write_dummies(flr, iho.1)

iho.1 <- iho |> filter(str_detect(fn, "Deep"))
flr <- "/mnt/muw/WoJ-hh/Deep Jazz/"
write_dummies(flr, iho.1)

# all current titles WoJ ----
tryCatch(
  {
    # . trigger GD-auth
    options(gargle_oauth_cache = ".secrets-salsa")
    gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
    df_clockcatalogue_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
    df_clockprofile_raw <- read_sheet(ss = config$gws_clock_profiles, sheet = "WORLD_OF_JAZZ")
  },
  error = function(e1) {
    flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
    break
  })

# . tidy catalogue ----
df_clockcatalogue <- df_clockcatalogue_raw |> rename(catalg_key = `key-modelrooster`,
                                                     titel_NL = `titel-NL`,
                                                     titel_EN = `titel-EN`,
                                                     productie = `productie-1-taak`,
                                                     redacteurs = `productie-1-mdw`,
                                                     genre_1 = `genre-1-NL`,
                                                     genre_2 = `genre-2-NL`,
                                                     intro_NL = `std.samenvatting-NL`,
                                                     intro_EN = `std.samenvatting-EN`,
                                                     afbeelding = feat_img_ids,
                                                     episode_chain = `episode-chain`) 

# . tidy profile ----
df_clockprofile <- df_clockprofile_raw |>
  rename(slot_key = slot) |> 
  mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key) |> 
  filter(!is.na(slot_minutes))

mk_weekly <- df_clockprofile |> filter(!is.na(wekelijks)) |> select(1:6) |> 
  rename(catalg_key = wekelijks, prod_type = te)
mk_biweekly<- df_clockprofile |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
  rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
mk_week.1 <- df_clockprofile |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
  mutate(block = 1L)
mk_week.2 <- df_clockprofile |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
  mutate(block = 2L)
mk_week.3 <- df_clockprofile |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
  mutate(block = 3L)
mk_week.4 <- df_clockprofile |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
  mutate(block = 4L)
mk_week.5 <- df_clockprofile |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
  mutate(block = 5L)

# . repair 'Thema' ----
# repair 'Thema' on CZ only, assume it's in slots 'wo20' and 'wo21' of weeks 2 and 4
# if (site_id == 1) {
#   mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
#   mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
# }

# . repair 'Rec/Play' ----
# ... on WJ only, assume it's in slots 'do20' and 'do21' of weeks 2 and 4
#                            and slots 'wo18' and 'wo19' of weeks 1 and 3
# if (site_id == 2) {
mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
mk_week.1 <- mk_week.1 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
mk_week.3 <- mk_week.3 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
# }

df_clock_cz_weekly <- mk_weekly

df_clock_cz_biweekly <- mk_biweekly

mk_5_weeks <- mk_week.1 |> 
  bind_rows(mk_week.2) |>
  bind_rows(mk_week.3) |>
  bind_rows(mk_week.4) |>
  bind_rows(mk_week.5)

df_clock_cz_5_weeks <- mk_5_weeks

woj_titles <- df_clock_cz_weekly |> 
  bind_rows(df_clock_cz_biweekly) |> 
  bind_rows(df_clock_cz_5_weeks) |> 
  left_join(df_clockcatalogue, by = join_by(catalg_key)) |> 
  mutate(src = if_else(prod_type == "h", "H", src),
         prod_type = if_else(prod_type == "h", "u", prod_type)) |> 
  mutate(is_replay = if_else(is.na(src), FALSE, TRUE)) |> 
  select(-src, -mac, -`dummy-1`, -slug) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
  select(slot_key,
         block,
         catalg_key,
         prod_type,
         `redactie-NL`,
         titel_NL,
         uitzendtype,
         is_replay) |> distinct() |> arrange(prod_type, titel_NL)

# all current titles CZ ----
tryCatch(
  {
    # . trigger GD-auth
    options(gargle_oauth_cache = ".secrets-salsa")
    gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
    df_clockcatalogue_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
    df_clockprofile_raw <- read_sheet(ss = config$gws_clock_profiles, sheet = "CONCERTZENDER")
  },
  error = function(e1) {
    flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
    break
  })

# . tidy catalogue ----
df_clockcatalogue <- df_clockcatalogue_raw |> rename(catalg_key = `key-modelrooster`,
                                                     titel_NL = `titel-NL`,
                                                     titel_EN = `titel-EN`,
                                                     productie = `productie-1-taak`,
                                                     redacteurs = `productie-1-mdw`,
                                                     genre_1 = `genre-1-NL`,
                                                     genre_2 = `genre-2-NL`,
                                                     intro_NL = `std.samenvatting-NL`,
                                                     intro_EN = `std.samenvatting-EN`,
                                                     afbeelding = feat_img_ids,
                                                     episode_chain = `episode-chain`) 

# . tidy profile ----
df_clockprofile <- df_clockprofile_raw |>
  rename(slot_key = slot) |> 
  mutate(slot_minutes = as.integer(min), .keep = "unused", .after = slot_key) |> 
  filter(!is.na(slot_minutes))

mk_weekly <- df_clockprofile |> filter(!is.na(wekelijks)) |> select(1:6) |> 
  rename(catalg_key = wekelijks, prod_type = te)
mk_biweekly<- df_clockprofile |> filter(!is.na(`twee-wekelijks`)) |> select(1:3, 9:11) |> 
  rename(catalg_key = `twee-wekelijks`) |> pivot_longer(cols = c(A, B), names_to = "cycle", values_to = "prod_type")
mk_week.1 <- df_clockprofile |> filter(!is.na(`week 1`)) |> select(1:4, catalg_key = `week 1`, prod_type = t1) |> 
  mutate(block = 1L)
mk_week.2 <- df_clockprofile |> filter(!is.na(`week 2`)) |> select(1:4, catalg_key = `week 2`, prod_type = t2) |> 
  mutate(block = 2L)
mk_week.3 <- df_clockprofile |> filter(!is.na(`week 3`)) |> select(1:4, catalg_key = `week 3`, prod_type = t3) |> 
  mutate(block = 3L)
mk_week.4 <- df_clockprofile |> filter(!is.na(`week 4`)) |> select(1:4, catalg_key = `week 4`, prod_type = t4) |> 
  mutate(block = 4L)
mk_week.5 <- df_clockprofile |> filter(!is.na(`week 5`)) |> select(1:4, catalg_key = `week 5`, prod_type = t5) |> 
  mutate(block = 5L)

# . repair 'Thema' ----
# repair 'Thema' on CZ only, assume it's in slots 'wo20' and 'wo21' of weeks 2 and 4
# if (site_id == 1) {
mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "wo20", 120L, slot_minutes)) |> filter(slot_key != "wo21")
# }

# . repair 'Rec/Play' ----
# ... on WJ only, assume it's in slots 'do20' and 'do21' of weeks 2 and 4
#                            and slots 'wo18' and 'wo19' of weeks 1 and 3
# if (site_id == 2) {
# mk_week.2 <- mk_week.2 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
# mk_week.4 <- mk_week.4 |> mutate(slot_minutes = if_else(slot_key == "do20", 120L, slot_minutes)) |> filter(slot_key != "do21")
# mk_week.1 <- mk_week.1 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
# mk_week.3 <- mk_week.3 |> mutate(slot_minutes = if_else(slot_key == "wo18", 120L, slot_minutes)) |> filter(slot_key != "wo19")
# }

df_clock_cz_weekly <- mk_weekly

df_clock_cz_biweekly <- mk_biweekly

mk_5_weeks <- mk_week.1 |> 
  bind_rows(mk_week.2) |>
  bind_rows(mk_week.3) |>
  bind_rows(mk_week.4) |>
  bind_rows(mk_week.5)

df_clock_cz_5_weeks <- mk_5_weeks

cz_titles <- df_clock_cz_weekly |> 
  bind_rows(df_clock_cz_biweekly) |> 
  bind_rows(df_clock_cz_5_weeks) |> 
  left_join(df_clockcatalogue, by = join_by(catalg_key)) |> 
  mutate(src = if_else(prod_type == "h", "H", src),
         prod_type = if_else(prod_type == "h", "u", prod_type)) |> 
  mutate(is_replay = if_else(is.na(src), FALSE, TRUE)) |> 
  select(-src, -mac, -`dummy-1`, -slug) |> 
  pivot_longer(cols = c(genre_1, genre_2), names_to = NULL, values_to = "genre", values_drop_na = TRUE) |> 
  filter(!is_replay) |> 
  select(slot_key,
         block,
         catalg_key,
         prod_type,
         # `redactie-NL`,
         # titel_NL,
         uitzendtype) |> distinct() |> arrange(catalg_key, prod_type) |> 
  filter(prod_type != str_sub(uitzendtype, 1, 1))
