#' List FFIEC bulk zip files
#'
#' Scans a directory for FFIEC "Call Bulk All Schedules" zip files and returns
#' a tibble with the zipfile paths and the report date parsed from the filename.
#'
#' Directory resolution:
#' - If `raw_dir` is supplied, it is used as-is.
#' - Otherwise, this function uses `RAW_DATA_DIR` and `schema` to form
#'   `file.path(RAW_DATA_DIR, schema)`.
#'
#' @param raw_dir Directory containing FFIEC bulk zip files. If `NULL`, uses
#'   `RAW_DATA_DIR` and `schema`.
#' @param schema Subdirectory name under `RAW_DATA_DIR`. Defaults to `"ffiec"`.
#'
#' @return A tibble with columns:
#' \describe{
#'   \item{zipfile}{Full path to the bulk zip file.}
#'   \item{date}{Report date as a \code{Date}.}
#' }
#' @export
ffiec_list_zips <- function(raw_dir = NULL, schema = "ffiec") {
  raw_dir <- resolve_raw_dir(raw_dir, schema)

  if (is.null(raw_dir) || !nzchar(raw_dir)) {
    stop("Provide `raw_dir` or set RAW_DATA_DIR.", call. = FALSE)
  }

  files <- list.files(
    raw_dir,
    pattern = "^FFIEC CDR Call Bulk All Schedules.*\\.zip$",
    full.names = TRUE
  )

  if (length(files) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0)),
      date_raw = character(0)
    ))
  }

  mmddyyyy <- stringr::str_extract(basename(files), "\\d{8}")
  bad <- is.na(mmddyyyy)

  if (any(bad)) {
    stop(
      "Could not parse an 8-digit MMDDYYYY date from these filenames:\n",
      paste0("  - ", basename(files[bad]), collapse = "\n"),
      "\nExpected filenames like: 'FFIEC CDR Call Bulk All Schedules 12312022.zip'",
      call. = FALSE
    )
  }

  date <- as.Date(mmddyyyy, format = "%m%d%Y")
  bad_date <- is.na(date)

  if (any(bad_date)) {
    stop(
      "Parsed date token(s) but as.Date() returned NA for:\n",
      paste0("  - ", basename(files[bad_date]), collapse = "\n"),
      "\nExtracted tokens:\n",
      paste0("  - ", mmddyyyy[bad_date], collapse = "\n"),
      call. = FALSE
    )
  }

  tibble::tibble(
    zipfile = normalizePath(files, mustWork = FALSE),
    date = date
  ) |>
    dplyr::arrange(.data$date, .data$zipfile)
}

#' Get a manifest of files inside an FFIEC call report bulk zip
#'
#' Lists the contents of a bulk zip file and parses schedule identifiers, report
#' date, and multi-part file indicators from the internal filenames.
#'
#' @param zip_file Path to a bulk FFIEC zip file.
#'
#' @return A tibble with columns \code{file}, \code{schedule}, \code{date},
#'   \code{date_raw}, \code{part}, and \code{n_parts}.
#' @keywords internal
#' @noRd
get_cr_files <- function(zip_file) {
  files <- unzip(zip_file, list = TRUE)$Name

  tibble::tibble(file = files) |>
    dplyr::filter(.data$file != "Readme.txt") |>
    dplyr::mutate(
      schedule = stringr::str_extract(basename(.data$file), "(?<=Schedule )[^ ]+"),
      date_raw_mmddyyyy = stringr::str_extract(basename(.data$file), "\\d{8}"),
      date = lubridate::mdy(.data$date_raw_mmddyyyy),
      date_raw = format(.data$date, "%Y%m%d"),
      part    = stringr::str_extract(basename(.data$file), "(?<=\\()\\d+(?= of )") |> as.integer(),
      n_parts = stringr::str_extract(basename(.data$file), "(?<=of )\\d+(?=\\))") |> as.integer()
    ) |>
    dplyr::select(-.data$date_raw_mmddyyyy)
}

#' Determine POR kind from an internal filename
#'
#' @param inner_file Internal file path within the zip.
#'
#' @return A scalar character string giving the kind.
#' @keywords internal
#' @noRd
por_kind <- function(inner_file) {
  bn <- basename(inner_file)
  if (grepl("Call Bulk POR", bn, fixed = TRUE)) return("por")
  "por"
}

#' Scan FFIEC Parquet files into DuckDB
#'
#' Create a lazy \code{tbl} backed by DuckDB by scanning one or more FFIEC
#' Parquet files. Files are selected either by schedule name (using a
#' glob pattern) or by an explicit Parquet filename. No data is read
#' eagerly; evaluation occurs only when the result is collected.
#'
#' This function is intended for use with Parquet files produced by
#' [ffiec_process()]. It validates that the requested files exist on
#' disk before constructing the DuckDB query.
#'
#' @param conn A valid DuckDB connection.
#' @param schedule Optional character scalar giving the FFIEC schedule
#'   name (e.g. \code{"rc"}, \code{"rci"}, \code{"rcb"}. All matching Parquet files
#'   of the form \code{\{prefix\}\{schedule\}_YYYYMMDD.parquet} will be scanned.
#' @param pq_file Optional character scalar giving a specific Parquet
#'   filename or glob pattern relative to `pq_dir`.
#' @param pq_dir Directory containing FFIEC Parquet files. If `NULL`,
#'   defaults to the resolved output directory for `schema`.
#' @param schema Schema name used to resolve default directories
#'   (default \code{"ffiec"}).
#' @param prefix Optional filename prefix used when the Parquet files
#'   were created (default \code{""}).
#' @param union_by_name Logical; whether to union Parquet files by column
#'   name when scanning multiple files (passed to DuckDB's
#'   \code{read_parquet()}). Default is \code{TRUE}.
#'
#' @details
#' Exactly one of `schedule` or `pq_file` must be supplied.
#'
#' When `schedule` is used, files are located using a glob pattern:
#' \code{\{prefix\}\{schedule\}_*.parquet}. When \code{pq_file} is used, the value is
#' treated as a filename or glob pattern relative to \code{pq_dir}.
#'
#' A fast filesystem check is performed using \code{Sys.glob()}. If no files
#' match, the function errors before issuing any DuckDB query.
#'
#' @return A lazy `tbl` backed by DuckDB.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Scan all RC schedule files
#' rc <- ffiec_scan_pqs(con, schedule = "rc")
#'
#' # Scan a single Parquet file
#' dor <- ffiec_scan_pqs(con, pq_file = "dor_20231231.parquet")
#'
#' # Work lazily
#' rc |> dplyr::count(date)
#' }
#'
#' @export
ffiec_scan_pqs <- function(conn,
                           schedule = NULL,
                           pq_file = NULL,
                           pq_dir = NULL,
                           schema = "ffiec",
                           prefix = "",
                           union_by_name = TRUE) {

  pq_dir <- resolve_out_dir(pq_dir, schema)
  if (is.null(pq_dir)) {
    stop("Provide `pq_dir` or set DATA_DIR.", call. = FALSE)
  }
  stopifnot(DBI::dbIsValid(conn), nzchar(pq_dir))

  # exactly one of schedule / pq_file
  n_specified <- sum(!is.null(schedule), !is.null(pq_file))
  if (n_specified != 1L) {
    stop("Provide exactly one of `schedule` or `pq_file`.", call. = FALSE)
  }

  if (!is.null(schedule)) {
    glob <- file.path(pq_dir, sprintf("%s%s_*.parquet", prefix, schedule))
    matches <- Sys.glob(glob)

    if (length(matches) == 0L) {
      stop(
        sprintf(
          "No parquet files found for schedule '%s' using pattern:\n  %s",
          schedule, glob
        ),
        call. = FALSE
      )
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s, union_by_name=%s)",
      DBI::dbQuoteString(conn, glob),
      if (isTRUE(union_by_name)) "true" else "false"
    )

  } else {
    glob <- file.path(pq_dir, pq_file)
    matches <- Sys.glob(glob)

    if (length(matches) == 0L) {
      stop(
        sprintf(
          "Parquet file not found:\n  %s",
          glob
        ),
        call. = FALSE
      )
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s)",
      DBI::dbQuoteString(conn, glob)
    )
  }

  dplyr::tbl(conn, dbplyr::sql(sql))
}
