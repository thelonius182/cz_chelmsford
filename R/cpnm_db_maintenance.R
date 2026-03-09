# insert program ----
ins_result <- 
  ins_cpnm_pgm(pm_title_NL = "Live Forms",
               pm_title_EN = "Live Forms",
               pm_descr_NL = "Geniet van live electronische en electro-akoestische muziek, vanuit de redactie van de Concertzender, of elders",
               pm_descr_EN = "Enjoy live electronic and electro-acoustic music, from the office of the Concertzender, or elsewhere",
               pm_cpnm_db = con)

# insert editor ----
ins_cpnm_editor(pm_name_NL = 'Hans Mantel en Rembrandt Frerichs',
                pm_cpnm_db = con)

# CZ: append missing colofons ----
for (cur_editor in tbl_w_ids_2_missing$redacteurs) {
  ins_cpnm_editor(pm_name_NL = cur_editor, pm_cpnm_db = con)
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
cf_editors <- dbGetQuery(con, sql_stmt)
missing_cfs <- tbl_w_ids_3 |> left_join(cf_editors, by = join_by(redacteurs == editor_lg)) |> 
  select(redacteurs, editor_cf) |> distinct() |> filter(is.na(editor_cf))

for (cur_editor in missing_cfs$redacteurs) {
  ins_cpnm_editor(pm_name_NL = cur_editor, pm_cpnm_db = con)
}

for (cur_editor in woj_schedule_w_ids_missing$redacteurs) {
  ins_cpnm_editor(pm_name_NL = cur_editor, pm_cpnm_db = con)
}

# fix missing EN-entries ----
fix_these_pgms <- woj_schedule_w_ids.5 |> select(pgm_id, titel_NL, titel_EN) |> distinct() |> arrange(titel_NL)

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
                  .con = con)
  sql_result <- dbExecute(conn = con, statement = qry)
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
                  .con = con)
  sql_result <- dbExecute(conn = con, statement = qry)
}

# fix program titles ----
qry <- 
"with ds1 as (
  select lower(p.title->>'$.nl') as title_NL, 
         p.title, 
         p.id as pgm_id,
         b.dates
  from entries p join entries e on e.parent_id = p.id
	  		     join entries b on b.parent_id = e.id
  where p.type = 'program' 
), ds2 as (
  select title_NL, title, pgm_id, count(*) as n_bcs
  from ds1
  group by title_NL, title, pgm_id
), ds3 as (
  select ds2.*,
  ROW_NUMBER() OVER (PARTITION BY title_NL
	  			     ORDER BY n_bcs desc) AS rn
  from ds2
)
select title_nl, title, pgm_id from ds3
where rn = 1
order by 1
;
"
db_titles <- dbGetQuery(conn = con, statement = qry)
gd_titles <- uniques_titles |> mutate(gd_title_lc = str_to_lower(titel_NL))
combi_titles <- gd_titles |> left_join(db_titles, by = join_by(gd_title_lc == title_nl))
combi_titles_missing <- combi_titles |> filter(is.na(title))
