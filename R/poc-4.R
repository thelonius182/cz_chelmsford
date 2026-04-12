append_chain_item <- function(con, label, state, clockfactory_job) {
  dbWithTransaction(con, {
    dbExecute(
      con,
      "
      INSERT INTO episode_chain (label, next_position)
      VALUES (?, 1)
      ON DUPLICATE KEY UPDATE
        chain_id = LAST_INSERT_ID(chain_id)
      ",
      params = list(label)
    )
    
    chain_id <- dbGetQuery(
      con,
      "SELECT LAST_INSERT_ID() AS chain_id"
    )$chain_id[[1]]
    
    dbExecute(
      con,
      "
      UPDATE episode_chain
      SET next_position = LAST_INSERT_ID(next_position + 1)
      WHERE chain_id = ?
      ",
      params = list(chain_id)
    )
    
    position <- dbGetQuery(
      con,
      "SELECT LAST_INSERT_ID() - 1 AS position"
    )$position[[1]]
    
    dbExecute(
      con,
      "
      INSERT INTO episode_chain_item (chain_id, position, state, clockfactory_job)
      VALUES (?, ?, ?, ?)
      ",
      params = list(chain_id, position, state, clockfactory_job)
    )
    
    tibble(chain_id = chain_id, position = position)
  })
}

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
