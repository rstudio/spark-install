context("spark_installed_versions")

test_that("spark_installed_versions can detect if Spark is installed", {
  installed <- spark_installed_versions()
  expect_equal(class(installed), "data.frame")
})
