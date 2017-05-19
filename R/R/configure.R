#' @rdname spark_install
#' @export
spark_configure <- function(spark_home)
{
  if (.Platform$OS.type == "windows")
    prepare_windows_environment(spark_home)
}
