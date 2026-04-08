wp_editors_raw <- read_tsv(file = "resources/clockfactory_redacteur.tsv", col_types = "cccc")
wp_editors <- wp_editors_raw |> mutate(wp_post_id = as.integer(ID),
                                       wp_post_date = force_tz(ymd_hms(post_date), tzone = "Europe/Amsterdam")) |> 
  rename(wp_title = post_title, wp_colofon = colofon) |> 
  select(wp_post_id, wp_colofon) |> arrange(wp_post_id)

dbCreateTable(
  con,
  "legacy_editors",
  fields = c(
    wp_post_id = "INT",
    wp_colofon = "VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
  )
)

dbAppendTable(con, "legacy_editors", wp_editors)
