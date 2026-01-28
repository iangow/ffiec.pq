#' Create FFIEC item Parquet files
#'
#' Writes FFIEC item reference tables shipped with the package as Parquet files
#' into the FFIEC Parquet output directory resolved in the same way as
#' \code{\link{ffiec_list_pqs}}.
#'
#' This is intended to make FFIEC item metadata available to non-R workflows
#' (e.g., Python, DuckDB, Polars) using the same directory layout as the main
#' FFIEC Parquet datasets.
#'
#' @param out_dir Optional output directory. If \code{NULL}, the directory is
#'   resolved using \code{schema} and environment variables, as in
#'   \code{\link{ffiec_list_pqs}}.
#' @param schema Schema name passed to \code{resolve_out_dir()}.
#' @param overwrite Logical; whether to overwrite existing files.
#'
#' @return A tibble with one row per file written and columns \code{base_name},
#' \code{full_name}, \code{schedule}, and \code{written}.
#'
#' @examples
#' \dontrun{
#' ffiec_create_item_pqs()
#' }
#'
#' @export
ffiec_create_item_pqs <- function(out_dir = NULL,
                                  schema = "ffiec",
                                  overwrite = FALSE) {
  out_dir <- resolve_out_dir(out_dir, schema)
  if (is.null(out_dir)) {
    stop("Provide `out_dir` or set DATA_DIR.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  files <- c(
    "ffiec_items.parquet",
    "ffiec_item_details.parquet"
  )

  dest_paths <- file.path(out_dir, files)

  if (!overwrite) {
    existing <- file.exists(dest_paths)
    if (any(existing)) {
      stop(
        "The following files already exist. Use overwrite = TRUE to replace them:\n",
        paste(basename(dest_paths[existing]), collapse = ", "),
        call. = FALSE
      )
    }
  }

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write Parquet files.", call. = FALSE)
  }

  ffiec_items        <- get("ffiec_items",
                            envir = asNamespace("ffiec.pq"))
  ffiec_item_details <- get("ffiec_item_details",
                            envir = asNamespace("ffiec.pq"))

  arrow::write_parquet(ffiec_items, dest_paths[[1]])
  arrow::write_parquet(ffiec_item_details, dest_paths[[2]])

  tibble::tibble(
    base_name = files,
    full_name = dest_paths,
    schedule  = "items",
    written   = TRUE
  )
}
