#' @rdname spark_install
#' @export
spark_install_dir <- function() {
  getOption("spark.install.dir", rappdirs::app_dir("spark", "rstudio")$cache())
}
