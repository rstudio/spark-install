context("spark_default_version")

test_that("spark_default_version retrieves expected defaults", {
  skip_on_cran()

  defaultVersion <- spark_available_versions()

  expect_equals(defaultVersion$spark, "2.1.1")
  expect_equals(defaultVersion$hadoop, "2.7")
})
