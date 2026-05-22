test_that("parse_args returns empty list when no args are supplied", {
  expect_equal(
    parse_args(character()),
    list()
  )
})

test_that("parse_args parses --key value pairs", {
  srvjob_args <- parse_args(c("--site_id", "1"))
  
  expect_equal(srvjob_args$site, "1")
  expect_equal(
    srvjob_args,
    list(site_id = "1")
  )
})

test_that("parse_args errors when uneven number of args is supplied", {
  expect_error(
    parse_args(c("--input", "file.csv", "--output")),
    "Arguments should be supplied as --key value pairs.",
    fixed = TRUE
  )
})

