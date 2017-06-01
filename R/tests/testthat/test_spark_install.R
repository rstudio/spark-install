context("spark_install")

test_that("spark_install can install and uninstall", {
  skip_on_cran()

  spark_install(version = "1.6.2", hadoop_version = "2.6")
  spark_uninstall(version = "1.6.2", hadoop_version = "2.6")
})
