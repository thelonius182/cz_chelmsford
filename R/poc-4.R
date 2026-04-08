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
