# init ----
pacman::p_load(tidyr, dplyr, stringr, readr, fs, yaml, purrr)

config <- read_yaml("config.yaml")

replace_color_label <- function(path, rgb) {
  bytes <- read_file_raw(path)
  txt <- rawToChar(bytes)
  
  pattern <- "^ColorLabel=\\d{1,3},\\d{1,3},\\d{1,3}$"
  replacement <- paste0("ColorLabel=", rgb)
  
  txt_new <- str_replace(txt, regex(pattern, multiline = TRUE), replacement)

  write_file(charToRaw(txt_new), path)
  
  invisible(TRUE)
}

base_rgb <- "30,30,30" 
band_rgb <- "100,100,100" 

files <- dir_ls("F1", type = "file") |>
  sort()

files |>
  iwalk(\(path, i) {
    rgb <- if (i %% 2 == 0) base_rgb else band_rgb
    replace_color_label(path, rgb)
  }
  )
