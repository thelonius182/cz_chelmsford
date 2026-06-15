# - - - - - - - - - - - - - - - - - - - - -
# Create a Weekly Playout
# - - - - - - - - - - - - - - - - - - - - -

# init & cfg ----
pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, DBI, digest, 
               purrr, httr, jsonlite, yaml, ssh, googlesheets4, glue, uuid, RMariaDB, RSQLite)

config <- read_yaml("config.yaml")
fmt_ts <- stamp("1958-12-25 13:00:00", quiet = T, orders = "ymd HMS")
log_slug <- "clof"
flap <- flog.appender(appender.file(config$log_appender_file), log_slug)
TZ_AM <- "Europe/Amsterdam"
SITE <- list(CONCERTZENDER = 1L, WORLD_OF_JAZZ = 2L)

rgb <- function(r, g, b) {
  list(red = r / 255, green = g / 255, blue = b / 255)
}

get_sheet_id <- function(ss, sheet) {
  gs4_get(ss)$sheets |> filter(name == sheet) |> pull(id)
}

format_pl_sheet <- function(ss, sheet, section_fill = rgb(217, 234, 211), header_fill = rgb(255, 229, 153)) {
  
  sheet_id <- get_sheet_id(ss = ss, sheet = sheet)
  
  data <- read_sheet(ss = ss, sheet = sheet)
  
  n_rows <- nrow(data) + 1
  n_cols <- ncol(data)
  
  checkbox_index <- match(gereed, names(data)) - 1L
  
  section_rows <-data |> 
    mutate(.sheet_row = row_number() + 1) |>
    filter(!is.na(uitzending),
           uitzending != "",
           is.na(slot) | slot == "",
           is.na(herhvan) | herhvan == ""
    ) |> pull(.sheet_row)
  
  requests <- list(
    list(
      updateSheetProperties = list(
        properties = list(
          sheetId = sheet_id,
          gridProperties = list(frozenRowCount = 1)
        ),
        fields = "gridProperties.frozenRowCount"
      )
    ),
    
    list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 0,
          endRowIndex = 1,
          startColumnIndex = 0,
          endColumnIndex = n_cols
        ),
        cell = list(
          userEnteredFormat = list(
            backgroundColor = header_fill,
            horizontalAlignment = "CENTER",
            textFormat = list(bold = TRUE),
            borders = list(
              bottom = list(
                style = "SOLID",
                width = 1,
                color = rgb(160, 160, 160)
              )
            )
          )
        ),
        fields = "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat,borders)"
      )
    )
  )
  
  if (!is.na(checkbox_index)) {
    requests <- c(
      requests,
      list(
        list(
          setDataValidation = list(
            range = list(
              sheetId = sheet_id,
              startRowIndex = 1,
              endRowIndex = n_rows,
              startColumnIndex = checkbox_index,
              endColumnIndex = checkbox_index + 1L
            ),
            rule = list(
              condition = list(type = "BOOLEAN"),
              strict = TRUE,
              showCustomUi = TRUE
            )
          )
        )
      )
    )
  }
  
  section_requests <- map(section_rows, 
                          \(r) {
                            list(
                              repeatCell = list(
                                range = list(
                                  sheetId = sheet_id,
                                  startRowIndex = r - 1L,
                                  endRowIndex = r,
                                  startColumnIndex = 0,
                                  endColumnIndex = min(n_cols, 7)
                                ),
                                cell = list(
                                  userEnteredFormat = list(
                                    backgroundColor = section_fill,
                                    textFormat = list(
                                      foregroundColor = rgb(255, 0, 0)
                                    )
                                  )
                                ),
                                fields = "userEnteredFormat(backgroundColor,textFormat)"
                              )
                            )
                          })
  
  column_widths <- c(45, 125, 80, 250, 115, 70, 95, 260)
  
  width_requests <- imap(column_widths[seq_len(min(length(column_widths), n_cols))],
                         \(width, i) {
                           list(
                             updateDimensionProperties = list(
                               range = list(
                                 sheetId = sheet_id,
                                 dimension = "COLUMNS",
                                 startIndex = i - 1L,
                                 endIndex = i
                               ),
                               properties = list(pixelSize = width),
                               fields = "pixelSize"
                             )
                           )
                         })
  
  requests <- c(requests, section_requests, width_requests)
  
  req <- request_generate(endpoint = "sheets.spreadsheets.batchUpdate",
                          params = list(spreadsheetId = as_sheets_id(ss), requests = requests))
  
  request_make(req)
  
  invisible(data)
}

# Append a week ----
# - also: keep latest 3 and preserve format
append_week <- function(ss, sheet, new_rows) {
  
  sheet_append(ss = ss, data = new_rows, sheet = sheet)
  
  current_data <- read_sheet(ss = ss, sheet = sheet) |>
    mutate(.sheet_row = row_number() + 1,
           .run_number = parse_integer(str_extract(cz_week, "^[0-9]+"))) |>
    filter(!is.na(.run_number))
  
  runs <- current_data |> distinct(.run_number) |> arrange(.run_number) |> pull(.run_number)
  
  if (length(runs) <= 3) {
    format_pl_sheet(ss = ss, sheet = sheet)
    return(invisible(current_data |> select(-.sheet_row, -.run_number)))
  }
  
  oldest_runs <- runs[seq_len(length(runs) - 3)]
  
  rows_to_delete <-current_data |> filter(.run_number %in% oldest_runs) |> arrange(.sheet_row)
  
  expected_rows <- seq(min(rows_to_delete$.sheet_row), max(rows_to_delete$.sheet_row))
  
  if (!identical(rows_to_delete$.sheet_row, expected_rows)) {
    stop("Rows to delete are not contiguous. Refusing to delete.")
  }
  
  delete_range <- glue("{min(rows_to_delete$.sheet_row)}:{max(rows_to_delete$.sheet_row)}")
  
  range_delete(ss = ss, sheet = sheet, range = delete_range, shift = "up")
  
  format_pl_sheet(ss = ss, sheet = sheet)
  
  invisible(rows_to_delete)
}

# > Main Control Loop ----
# read this as DO {...} WHILE(FALSE)
repeat {
  
  # load GD-sheet ----
  tryCatch(
    {
      # . trigger GD-auth
      options(gargle_oauth_cache = ".secrets-salsa")
      gs4_auth(email = "cz.teamservice@concertzender.nl", scopes = "spreadsheets")
      gws_plws_raw <- read_sheet(ss = config$gws_playlistweeks, sheet = "WORLD_OF_JAZZ")
      gws_clock_catalg_raw <- read_sheet(ss = config$gws_clock_catalogue, sheet = "data")
    },
    error = function(e1) {
      flog.error("Load error GD-sheet(s): %s", conditionMessage(e1), name = log_slug)
      break
    })
  
  where_to_continue <- max(gws_plws_raw$uitzending, na.rm = T)
  where_to_continue <- ymd("2026-06-18")
  hour(where_to_continue) <- 12
  minute(where_to_continue) <- 55
  where_to_stop <- where_to_continue + days(7L)
  source("R/custom_functions.R", encoding = "UTF-8")
  source("R/cpnm_db_setup.R", encoding = "UTF-8")

  # get week from cpnm-db ----  
  db_week <- dbGetQuery(con_cpnm, statement = read_file("SQL/clockweek.sql"), 
                        params = list(where_to_continue, where_to_stop, SITE$WORLD_OF_JAZZ))
  
  # . check LaCie drive ----
  lacie_root <- config$lacie_root
  
  if (!dir_exists(lacie_root)) {
    flog.error(str_glue("{lacie_root} is not available; quiting this job."), name = log_slug)
    break
  }
  
  # . get LaCie chains ----
  source("R/LaCie_tools.R", encoding = "UTF-8")
  ep_lacies <- lacie_episodes(con_mysql = con_cpnm)
  con_sqlite <- dbConnect(SQLite(), "resources/lacie.sqlite")
  fs_lacies <- scan_fs(lacie_root) |> 
    mutate(bc_start_chr = separate_dt(fn)) |> 
    inner_join(ep_lacies, by = join_by(chain, bc_start_chr)) |> 
    # some broadcasts have 2 files: .wav and .aiff; exclude .wav
    group_by(episode_id) |> 
    mutate(rn = row_number()) |> 
    ungroup() |> 
    filter(rn == 1)
  sdb <- sync_db(con_sqlite = con_sqlite, fs = fs_lacies)
  db_lacies <- dbGetQuery(con_sqlite, "select * from lacie_stack order by chain, pos;")
  
  gws_week_lac <- db_week |> left_join(db_lacies, by = join_by(ep_id))
  
  # . get catalogue ----
  catalg <- gws_clock_catalg_raw |> select(catalg_key = `key-modelrooster`, title = `titel-NL`, 
                                           server = `cz-playout-mac`, uitzendtype, chain = `episode-chain`)
  catalg_woj <- catalg |> filter(str_detect(uitzendtype, "_woj") & chain != "#NONE#")
  
  # . tibble for GWS
  gws_week <- gws_week_lac |> 
    left_join(catalg, by = join_by(playlist == catalg_key)) |> 
    filter(is.na(uitzendtype) | uitzendtype != "non-stop") |> 
    mutate(min_bc_start = if_else(min_bc_start == bc_start, NA_character_, min_bc_start),
           source = case_when(!is.na(fn) ~ "LaCie", 
                              ep_title %in% catalg_woj$title ~ "WoJ-pc",
                              TRUE ~ "Uitzendmac"),
           slot = to_slot_key(ymd_hms(bc_start, quiet = T, tz = TZ_AM))) |> 
    select(uitzending = bc_start, 
           slot,
           programma = ep_title, 
           herhaling_van = min_bc_start, 
           source) |> distinct() |> arrange(source, programma)
  
  append_week(ss = config$gws_playlistweeks, sheet = "WORLD_OF_JAZZ")
  
  # Exit from MCL
  break
}

# Cleanup ----
dbDisconnect(con_cpnm)
close_tunnel(tunnel)
dbDisconnect(con_sqlite)
