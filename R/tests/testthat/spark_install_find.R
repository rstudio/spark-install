context("spark_install_find")

test_that("spark_install_find failure on invalid version", {
  skip_on_cran()

  testthat::expect_error(
    spark_install_find(version = "9.9.9")
  )
})
