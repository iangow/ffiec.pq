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
#'   \item{date_raw}{Report date in YYYYMMDD form.}
#' }
#' @export
list_ffiec_zips <- function(raw_dir = NULL, schema = "ffiec") {
  raw_dir <- resolve_raw_dir(raw_dir, schema)

  if (is.null(raw_dir) || !nzchar(raw_dir)) {
    stop("Provide `raw_dir` or set RAW_DATA_DIR.", call. = FALSE)
  }

  files <- list.files(
    raw_dir,
    pattern = "^FFIEC CDR Call Bulk All Schedules",
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
    dplyr::mutate(date_raw = format(.data$date, "%Y%m%d")) |>
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

#' Determine DOR/POR kind from an internal filename
#'
#' @param inner_file Internal file path within the zip.
#'
#' @return A scalar character string giving the kind.
#' @keywords internal
#' @noRd
dor_kind <- function(inner_file) {
  bn <- basename(inner_file)
  if (grepl("Call Bulk POR", bn, fixed = TRUE)) return("por")
  "dor"
}
