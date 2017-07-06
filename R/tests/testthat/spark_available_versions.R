context("spark_available_versions")

test_that("spark_available_versions is not empty", {
  skip_on_cran()

  allVersions <- spark_available_versions()

  expect_equals(class(allVersions), "data.frame")
  expect_equals(allVersions[allVersions$spark == "2.1.1",], 4)
})
