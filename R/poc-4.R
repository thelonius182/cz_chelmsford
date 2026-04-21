
extract_keywords <- function(x,
                             stop_words = c(
                               "the", "of", "and", "a", "an",
                               "de", "het", "een", "van", "op",
                               "y", "la", "el", "le", "les",
                               "in", "on", "to", "for", "with"
                             ),
                             generic_words = c(
                               "radio", "serie", "series", "program",
                               "programma", "episode", "show", "eigenzinnige",
                               "sound", "strike", "lied", "acoustic"
                             ),
                             n = 2) {
  
  map_chr(x, function(one_title) {
    words_original <- one_title |>
      str_replace_all("&", " and ") |>
      str_split("\\W+") |>
      pluck(1)
    
    words_ascii <- words_original |>
      str_to_lower() |>
      stringi::stri_trans_general("Latin-ASCII")
    
    is_cap <- str_detect(words_original, "^[A-Z]")
    
    keep <- words_ascii != "" &
      !words_ascii %in% stop_words &
      !words_ascii %in% generic_words &
      str_detect(words_ascii, "[a-z0-9]")
    
    words_kept <- words_ascii[keep]
    is_cap_kept <- is_cap[keep]
    
    if (length(words_kept) == 0) {
      return("")
    }
    
    if (sum(is_cap_kept) >= 2) {
      proper_words <- words_kept[is_cap_kept]
      surname <- tail(proper_words, 1)
      others <- words_kept[!is_cap_kept]
      result <- c(surname, head(others, n - 1))
      return(str_c(unique(result), collapse = " "))
    }
    
    scores <- nchar(words_kept)
    result <- words_kept[order(scores, decreasing = TRUE)] |>
      unique() |>
      head(n)
    
    str_c(result, collapse = " ")
  })
}

slug8 <- function(x,
                  stop_words = c(
                    "the", "of", "and", "a", "an",
                    "de", "het", "een", "van", "op",
                    "y", "la", "el", "concertzender"
                  )) {
  
  map_chr(x, function(one_title) {
    
    x_ascii <- one_title |>
      str_to_lower() |>
      str_replace_all("&", " and ") |>
      stringi::stri_trans_general("Latin-ASCII")
    
    words <- x_ascii |>
      str_split("\\s+") |>
      pluck(1) |>
      discard(~ .x == "" || .x %in% stop_words)
    
    base_string <- words |>
      str_c(collapse = " ") |>
      str_replace_all("[^a-z0-9]+", "") 
    
    if (base_string == "") {
      base_string <- x_ascii |>
        str_replace_all("[^a-z0-9]+", "")
    }
    
    str_sub(str_c(base_string, "        "), 1, 8) |> str_to_upper()
    # chk  <- digest(one_title, algo = "xxhash32") 
      # str_sub(1, 4)
    
    # str_c(stem, "-", chk) |> str_to_upper()
  })
}

cz_slugs <- cz_titles |> mutate(slug = slug8(extract_keywords(wp_title)))

STEP <- list(
  FACTORY = 10L,
  EDITOR  = 20L,
  DESK    = 30L,
  PLAYOUT = 99L
)

step_lookup <- tibble(
  step = c(10L, 20L, 30L, 99L),
  step_name = c("FACTORY", "EDITOR", "DESK", "PLAYOUT")
)

jobs <- tibble(id = 1:5,
               latest_completed_step = c(STEP$FACTORY, STEP$EDITOR, STEP$DESK, STEP$PLAYOUT, STEP$EDITOR))

planned_jobs <- jobs |> filter(latest_completed_step < STEP$PLAYOUT) |> 
  left_join(step_lookup, by = join_by(latest_completed_step == step))
