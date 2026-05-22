test_that("fetch_broadcasts returns only published broadcasts", {
  tib = tibble(id = c(1, 2, 3),
               status = c("published", "draft", "published"),
               broadcast_date = as.Date(c("2024-01-01", "2024-01-02", "2024-01-03"))
  )
  tab <- "broadcasts"
  sqlite_con <- make_seeded_test_con(pm_tibble = tib, pm_table_name = tab)
  withr::defer(dbDisconnect(sqlite_con), teardown_env())
  
  result <- fetch_broadcasts(sqlite_con)
  
  expect_equal(result$id, c(1, 3))
})
