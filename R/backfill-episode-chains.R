wp_editors_raw <- read_tsv(file = "resources/clockfactory_redacteur.tsv", col_types = "cccc")
wp_editors <- wp_editors_raw |> mutate(wp_post_id = as.integer(ID),
                                       wp_post_date = force_tz(ymd_hms(post_date), tzone = "Europe/Amsterdam")) |> 
  rename(wp_title = post_title, wp_produced_by = produced_by) |> 
  select(wp_post_id, wp_produced_by) |> arrange(wp_post_id)

