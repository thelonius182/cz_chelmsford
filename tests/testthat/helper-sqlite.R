library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)
library(tibble)

make_seeded_test_con <- function(pm_tibble, pm_table_name) {
  tt_con <- dbConnect(SQLite(), ":memory:")
  
  copy_to(dest = tt_con,
          df = pm_tibble,
          name = pm_table_name,
          temporary = FALSE,
          overwrite = TRUE)
  
  tt_con
}
