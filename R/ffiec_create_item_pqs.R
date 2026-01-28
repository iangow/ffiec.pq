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
#' @param include_long Logical; whether to also write an unnested ("long")
#'   version of \code{ffiec_item_schedules} with one row per date.
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
                                  overwrite = FALSE,
                                  include_long = TRUE) {
  out_dir <- resolve_out_dir(out_dir, schema)
  if (is.null(out_dir)) {
    stop("Provide `out_dir` or set DATA_DIR.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  # Names and objects to write
  files <- c(
    ffiec_items = "ffiec_items.parquet",
    ffiec_item_details = "ffiec_item_details.parquet",
    ffiec_item_schedules = "ffiec_item_schedules.parquet"
  )

  dest_paths <- file.path(out_dir, unname(files))

  # Respect overwrite = FALSE
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

  # Write Parquet (require arrow at runtime)
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write Parquet files.", call. = FALSE)
  }

  # Write the three core tables
  arrow::write_parquet(ffiec_items, dest_paths[[1]])
  arrow::write_parquet(ffiec_item_details, dest_paths[[2]])
  arrow::write_parquet(ffiec_item_schedules, dest_paths[[3]])

  written <- rep(TRUE, length(dest_paths))
  out_files <- unname(files)

  # Optionally write a long/unnested version (more Python-friendly)
  if (isTRUE(include_long)) {
    long_name <- "ffiec_item_schedules_long.parquet"
    long_path <- file.path(out_dir, long_name)

    if (!overwrite && file.exists(long_path)) {
      stop(
        "The following file already exists. Use overwrite = TRUE to replace it:\n",
        long_name,
        call. = FALSE
      )
    }

    # Avoid importing tidyr—use base to unnest list-column
    # ffiec_item_schedules: columns item, schedule, dates (list of Date)
    long_df <- ffiec_item_schedules[
      rep(seq_len(nrow(ffiec_item_schedules)), lengths(ffiec_item_schedules$dates)),
      c("item", "schedule"),
      drop = FALSE
    ]
    long_df$date <- as.Date(unlist(ffiec_item_schedules$dates), origin = "1970-01-01")

    arrow::write_parquet(long_df, long_path)

    out_files <- c(out_files, long_name)
    dest_paths <- c(dest_paths, long_path)
    written <- c(written, TRUE)
  }

  tibble::tibble(
    base_name = basename(dest_paths),
    full_name = dest_paths,
    schedule  = "items",
    written   = written
  )
}
