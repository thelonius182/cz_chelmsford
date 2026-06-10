# - - - - - - - - - - - - - - - - - - - - -
# Create a Playlist Week Schema
# - - - - - - - - - - - - - - - - - - - - -

# init & cfg ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite
               # openxlsx, readxl, 
)

config <- read_yaml("config.yaml")
TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)
DFT_IMG <- list(CZ = "d5c53946-16e5-11f1-94a8-3631520831df", WJ = "d7d25b88-16e5-11f1-94a8-3631520831df")
BUILD_TYPE <- list(EXTEND = 1L, REVISE  = 2L)
PROD_TYPE <- list(LACIE = "a", SEMI_LIVE = "s")
R_DAYS_OF_WEEK <- list(MONDAY = 1L, THURSDAY = 4L) # R and SQL have different conventions
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
log_slug <- "clof"
apf <- flog.appender(appender.file(config$log_appender_file), log_slug)

# load GD-sheets ----
tryCatch(
  {
    # . trigger GD-auth
    options(gargle_oauth_cache = ".secrets-salsa")
    gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
    gws_plws_raw <- read_sheet(ss = config$gws_playlistweeks, sheet = names(SITE)[[SITE$WORLD_OF_JAZZ]])
  },
  error = function(e1) {
    flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
    break
  })
