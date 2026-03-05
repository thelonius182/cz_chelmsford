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

sql_stmt <- "with tlg as (select name->>'$.nl' as editor_lg 
			 from taxonomies
             where type = 'creator' 
               and legacy_type = 'programma_maker'),
tcf as (select name->>'$.nl' as editor_cf, id as cf_id
        from taxonomies
        where type = 'colofon')
select * from tlg left join tcf on tcf.editor_cf = tlg.editor_lg
order by 1
;"
cf_editors <- dbGetQuery(con, sql_stmt)
missing_cfs <- tbl_w_ids_3 |> left_join(cf_editors, by = join_by(redacteurs == editor_lg)) |> 
  select(redacteurs, editor_cf) |> distinct() |> filter(is.na(editor_cf))
