wp_slot_alloc_raw <- read_tsv(file = "resources/clockfactory_slot_allocation.tsv", col_types = "cccc")

wp_slot_alloc <- wp_slot_alloc_raw |> 
  mutate(wp_post_id = as.integer(ID),
         wp_post_date = force_tz(ymd_hms(post_date), tzone = "Europe/Amsterdam"),
         wp_title = str_to_lower(wp_title),
         wp_slot_rev = paste0(str_extract(wp_slot, ".{4}$"), "-", str_extract(wp_slot, "^\\d"))) |> 
  select(wp_title, wp_slot_rev, wp_post_date, wp_post_id) |> arrange(wp_post_id)

wp_slot_uq <- wp_slot_alloc |> group_by(wp_title, wp_slot_rev) |> mutate(n = n()) |> ungroup() |> 
  select(wp_title, wp_slot_rev, n) |> distinct() |> arrange(wp_title, wp_slot_rev)
write_delim(wp_slot_uq, file = "resources/wp_slot_uq.tsv", delim = "\t")
dbCreateTable(
  con,
  "legacy_editors",
  fields = c(
    wp_post_id = "INT",
    wp_colofon = "VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
  )
)

dbAppendTable(con, "legacy_editors", wp_editors)

daylist <- c("ma", "di", "wo", "do", "vr", "za", "zo")
df_chain_parms <- df_clock_cz.12 |> select(bc_ts, slot, pgm_id, episode_chain, redacteurs) |> distinct() |> 
  mutate(pm_weekday = match(str_extract(slot, "^[a-z]{2}"), daylist) - 1,
         pm_time = paste0(str_extract(slot, "\\d{2}$"), ":00:00.000000"),
         pm_week = case_when(day(bc_ts) > 28 ~ 5,
                             day(bc_ts) > 21 ~ 4,
                             day(bc_ts) > 14 ~ 3,
                             day(bc_ts) > 7 ~ 2,
                             TRUE ~ 1))
df_all_chains <- tibble()

for (rn in seq_len(nrow(df_chain_parms))) {
  cur_chain <- get_chain(pm_parent_id = df_chain_parms$pgm_id[rn],
                         pm_weekday = df_chain_parms$pm_weekday[rn],
                         pm_time = df_chain_parms$pm_time[rn],
                         pm_chain = df_chain_parms$episode_chain[rn],
                         pm_colofon = df_chain_parms$redacteurs[rn],
                         pm_con = con)
  df_all_chains <- df_all_chains |> bind_rows(cur_chain)  
}  

pm_parent_id <- "4d29e726-16e0-11f1-94a8-3631520831df"
pm_weekday <- 4L
pm_time <- '07:00:00.000000'
pm_chain <- 'OCHTEDIT-E'
pm_colofon <- "CZ"

get_chain <- function(pm_parent_id, pm_weekday, pm_time, pm_chain, pm_colofon, pm_con) {
  
  pm_colofon_expr <- if (str_ends(pm_chain, "MAIN")) {
    SQL("is not null")
  } pm_colofon == "CZ")  else SQL(paste0("= '", pm_colofon, "'"))
  
  sql_stmt <- glue_sql("
       with episodes as (
       select p.title->>'$.nl' as title_nl,
              e.id as episode_id,
              e.legacy_id,
              lg.wp_colofon,
              min(b.dates->>'$.start') as episode_ts
       from entries e join entries p on p.id = e.parent_id
                                    and p.deleted_at is null
       			   join entries b on b.parent_id = e.id
                                    and b.deleted_at is null
       			   left join legacy_editors lg on lg.wp_post_id = e.legacy_id
       where e.parent_id = {pm_parent_id}
         and e.deleted_at is null
         and e.site_id = 1
         and e.legacy_id > 500000
       group by p.title->>'$.nl',
                e.id,
                e.legacy_id,
                lg.wp_colofon
       ), 
       chains as (
       select title_nl,
              episode_id,
              episode_ts,
              case when day(episode_ts) > 28 then 5
                   when day(episode_ts) > 21 then 4
                   when day(episode_ts) > 14 then 3
                   when day(episode_ts) >  7 then 2
                   else 1
              end as cz_week,
              weekday(episode_ts) as wkd,
              wp_colofon,
              {pm_chain} as episode_chain
       from episodes
       where weekday(episode_ts) = {pm_weekday} 
         and time(episode_ts) = {pm_time}
         and wp_colofon {pm_colofon_expr}
       )
       select title_nl,
              episode_id, 
              episode_ts, 
        --       time(episode_ts) as epi_ts,
              wp_colofon,
              episode_chain,
              lpad(row_number() over (partition by episode_chain order by episode_ts), 4, '0') as chain_pos
       from chains
       order by 3 desc
       ;
       ", .con = pm_con)
  dbGetQuery(conn = pm_con, sql_stmt)
}

cz_titles <- wp_slot_uq |> select(wp_title) |> distinct()
