starts_with <- function(lhs, rhs) {
  if (nchar(lhs) < nchar(rhs))
    return(FALSE)
  identical(substring(lhs, 1, nchar(rhs)), rhs)
}

aliased_path <- function(path) {
  home <- path.expand("~/")
  if (starts_with(path, home))
    path <- file.path("~", substring(path, nchar(home) + 1))
  path
}

stopf <- function(fmt, ..., call. = TRUE, domain = NULL) {
  stop(simpleError(
    sprintf(fmt, ...),
    if (call.) sys.call(sys.parent())
  ))
}
