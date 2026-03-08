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
for (rn in seq_len(nrow(tbl_w_ids_3))) {
  cur_title_NL <- tbl_w_ids_3$titel_NL[rn]
  cur_title_EN <- tbl_w_ids_3$titel_EN[rn]
  cur_descr_NL <- tbl_w_ids_3$intro_NL[rn]
  cur_descr_EN <- tbl_w_ids_3$intro_EN[rn]
  print(cur_descr_NL)
}
