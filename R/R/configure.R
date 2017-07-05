#' Configures a local installation of Spark
#'
#' Configures a local Spark installation.
#'
#' @param spark_home The path to a Spark installation.
#' @param spark_environment Optional environment to be enhanced with
#'   environment variables required to launch `spark-submit` through
#'   functions similar to `system`. When this parameter is specified,
#'   environment variables are not set.
#'
#' @export
spark_configure <- function(spark_home, spark_environment = NULL)
{
  if (.Platform$OS.type == "windows")
    prepare_windows_environment(spark_home, spark_environment)
}
