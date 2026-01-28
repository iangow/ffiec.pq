#' Create FFIEC item-schedule coverage Parquet file
#'
#' Scans FFIEC schedule Parquet files in the resolved output directory and
#' creates a Parquet file describing which items (columns) appear in which
#' schedules and on which reporting dates. Dates are inferred from file names.
#'
#' Includes optional filtering by schedule, a progress indicator, and a local
#' cache (RDS) of Parquet schemas to speed up repeated runs.
#'
#' @param out_dir Optional output directory. If \code{NULL}, the directory is
#'   resolved using \code{schema} and environment variables.
#' @param schema Schema name passed to \code{resolve_out_dir()}.
#' @param schedules Optional character vector of schedules to include
#'   (e.g., \code{c("rc", "rcn")}). If \code{NULL}, all schedules are included
#'   except \code{"por"} and \code{"items"}.
#' @param overwrite Logical; whether to overwrite an existing output file.
#' @param file_name Output parquet file name.
#' @param progress Logical; whether to show a progress bar while reading schemas.
#' @param cache Logical; whether to cache file schemas in an RDS file for
#'   faster repeated runs.
#' @param cache_file Optional cache file path. If \code{NULL}, defaults to
#'   \code{file.path(out_dir, ".ffiec_item_schedules_schema_cache.rds")}.
#'
#' @return A tibble with one row describing the file written.
#'
#' @export
ffiec_create_item_schedules_pq <- function(out_dir = NULL,
                                           schema = "ffiec",
                                           schedules = NULL,
                                           overwrite = FALSE,
                                           file_name = "ffiec_item_schedules.parquet",
                                           progress = TRUE,
                                           cache = TRUE,
                                           cache_file = NULL) {
  out_dir <- resolve_out_dir(out_dir, schema)
  if (is.null(out_dir)) {
    stop("Provide `out_dir` or set DATA_DIR.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write Parquet files.", call. = FALSE)
  }

  out_path <- file.path(out_dir, file_name)
  if (!overwrite && file.exists(out_path)) {
    stop(
      "Output file already exists. Use overwrite = TRUE to replace it:\n",
      basename(out_path),
      call. = FALSE
    )
  }

  # --- choose cache location ---
  if (is.null(cache_file)) {
    cache_file <- file.path(out_dir, ".ffiec_item_schedules_schema_cache.rds")
  }

  # --- list files using the package’s existing logic ---
  pqs <- ffiec_list_pqs(out_dir = out_dir, schema = schema)

  pqs <- pqs |>
    dplyr::filter(!.data$schedule %in% c("items", "por")) |>
    dplyr::filter(stringr::str_detect(.data$base_name, "_\\d{8}\\.parquet$"))

  if (!is.null(schedules)) {
    schedules <- unique(as.character(schedules))
    pqs <- dplyr::filter(pqs, .data$schedule %in% schedules)
  }

  if (nrow(pqs) == 0) {
    stop("No Parquet files matched the requested schedule(s).", call. = FALSE)
  }

  # --- schema cache: path -> list(mtime, size, cols) ---
  schema_cache <- list()
  if (isTRUE(cache) && file.exists(cache_file)) {
    schema_cache <- tryCatch(readRDS(cache_file), error = function(e) list())
    if (!is.list(schema_cache)) schema_cache <- list()
  }

  # Helper: get schema names with caching
  pq_cols_cached <- function(path) {
    info <- file.info(path)
    mtime <- as.numeric(info$mtime)
    size  <- as.numeric(info$size)

    key <- path
    hit <- schema_cache[[key]]

    if (isTRUE(cache) &&
        is.list(hit) &&
        isTRUE(all.equal(hit$mtime, mtime)) &&
        isTRUE(all.equal(hit$size, size)) &&
        !is.null(hit$cols)) {
      return(hit$cols)
    }

    cols <- arrow::read_parquet(path, as_data_frame = FALSE)$schema$names

    if (isTRUE(cache)) {
      schema_cache[[key]] <<- list(mtime = mtime, size = size, cols = cols)
    }

    cols
  }

  # --- read schemas with progress ---
  n <- nrow(pqs)
  if (isTRUE(progress)) {
    pb <- utils::txtProgressBar(min = 0, max = n, style = 3)
    on.exit(try(close(pb), silent = TRUE), add = TRUE)

    cols_list <- vector("list", n)
    for (i in seq_len(n)) {
      cols_list[[i]] <- pq_cols_cached(pqs$full_name[[i]])
      utils::setTxtProgressBar(pb, i)
    }
  } else {
    cols_list <- purrr::map(pqs$full_name, pq_cols_cached)
  }

  # --- persist cache (best-effort) ---
  if (isTRUE(cache)) {
    try(saveRDS(schema_cache, cache_file), silent = TRUE)
  }

  # --- build mapping ---
  ffiec_item_schedules <-
    pqs |>
    dplyr::mutate(
      date = as.Date(stringr::str_extract(.data$base_name, "\\d{8}"), format = "%Y%m%d"),
      cols = cols_list
    ) |>
    dplyr::select(.data$schedule, .data$date, .data$cols) |>
    tidyr::unnest(.data$cols, names_repair = "minimal") |>
    dplyr::rename(item = .data$cols) |>
    dplyr::filter(.data$item != "IDRSSD") |>
    dplyr::group_by(.data$item, .data$schedule) |>
    dplyr::summarise(
      dates = list(sort(unique(.data$date))),
      .groups = "drop"
    ) |>
    dplyr::arrange(.data$item, .data$schedule)

  arrow::write_parquet(ffiec_item_schedules, out_path)

  tibble::tibble(
    base_name = basename(out_path),
    full_name = out_path,
    schedule  = "items",
    written   = TRUE
  )
}
