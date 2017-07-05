#' Configures a local installation of Spark
#'
#' Configures a local Spark installation.
#'
#' @param spark_home The path to a Spark installation.
#'
#' @export
spark_configure <- function(spark_home)
{
  if (.Platform$OS.type == "windows")
    prepare_windows_environment(spark_home)
}
