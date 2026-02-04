#' Create FFIEC item Parquet files
#'
#' Writes FFIEC item reference tables as Parquet files into the FFIEC
#' Parquet output directory, resolved using the same rules as
#' [ffiec_list_pqs()].
#'
#' @param data_dir Optional parent directory for Parquet output. If provided
#'   and \code{schema} is not \code{NULL}, files are written under
#'   \code{file.path(data_dir, schema)}. If \code{NULL}, the environment
#'   variable \code{DATA_DIR} is used.
#' @param schema Schema name used to resolve the Parquet directory
#'   (default \code{"ffiec"}). Set to \code{NULL} to use the resolved
#'   directory directly without appending a schema subdirectory.
#' @param overwrite Logical; whether to overwrite existing files.
#'
#' @return A tibble with one row per file written and columns
#'   \code{base_name}, \code{full_name}, \code{schedule}, and \code{written}.
#'
#' @keywords internal
#' @noRd
ffiec_create_item_pqs <- function(data_dir = NULL,
                                  schema = "ffiec", overwrite = FALSE) {
  out_dir <- resolve_out_dir(data_dir = data_dir, schema = schema)
  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set DATA_DIR.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write Parquet files.", call. = FALSE)
  }

  # Files to create
  files <- c(
    ffiec_items        = "ffiec_items.parquet",
    ffiec_item_details = "ffiec_item_details.parquet"
  )

  dest_paths <- file.path(out_dir, unname(files))
  exists     <- file.exists(dest_paths)

  # Load data lazily (only if needed)
  get_data <- function(name) {
    get(name, envir = asNamespace("ffiec.pq"))
  }

  written <- logical(length(dest_paths))

  # ---- ffiec_items ----
  if (!exists[[1]] || overwrite) {
    arrow::write_parquet(
      get_data("ffiec_items"),
      dest_paths[[1]]
    )
    written[[1]] <- TRUE
  } else {
    message(
      "File already exists: ", basename(dest_paths[[1]]), "\n",
      "Re-run `ffiec_create_item_pqs()` with `overwrite = TRUE` to replace it."
    )
    written[[1]] <- FALSE
  }

  # ---- ffiec_item_details ----
  if (!exists[[2]] || overwrite) {
    arrow::write_parquet(
      get_data("ffiec_item_details"),
      dest_paths[[2]]
    )
    written[[2]] <- TRUE
  } else {
    message(
      "File already exists: ", basename(dest_paths[[2]]), "\n",
      "Re-run `ffiec_create_item_pqs()` with `overwrite = TRUE` to replace it."
    )
    written[[2]] <- FALSE
  }

  tibble::tibble(
    base_name = basename(dest_paths),
    full_name = dest_paths,
    schedule  = "items",
    written   = written
  )
}
