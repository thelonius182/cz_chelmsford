test_that("date argument is parsed using short option", {
  opts <- parse_rebuild_options(c("-d", "2026-05-01"))
  
  expect_equal(opts$date, "2026-05-01")
})

test_that("date argument is parsed using long option", {
  opts <- parse_rebuild_options(c("--date", "2026-05-01"))
  
  expect_equal(opts$date, "2026-05-01")
})

test_that("missing date gives an error", {
  expect_error(
    parse_rebuild_options(character()),
    "missing argument"
  )
})
