#' List FFIEC bulk zip files in a schema directory
#'
#' Scans a directory for FFIEC "Call Bulk All Schedules" zip files and returns
#' a tibble with the zipfile paths and the report date parsed from the filename.
#'
#' Directory resolution:
#' - If \code{raw_dir} is provided, it is used directly.
#' - Otherwise, \code{RAW_DATA_DIR} and \code{schema} are used.
#'
#' @param raw_dir Directory containing FFIEC bulk zip files. If \code{NULL},
#'   uses \code{RAW_DATA_DIR} and \code{schema}.
#' @param schema Subdirectory name under \code{RAW_DATA_DIR}.
#'   Defaults to \code{"ffiec"}.
#'
#' @return A tibble with columns \code{zipfile}, \code{date}, and \code{date_raw}.
#' @export
list_ffiec_zips <- function(raw_dir = NULL, schema = "ffiec") {
  raw_dir <- resolve_raw_dir(raw_dir, schema)

  if (is.null(raw_dir)) {
    stop(
      "Provide `raw_dir` or set RAW_DATA_DIR (optionally with `schema`).",
      call. = FALSE
    )
  }

  files <- list.files(
    raw_dir,
    pattern = "^FFIEC CDR Call Bulk All Schedules",
    full.names = TRUE
  )

  if (length(files) == 0) {
    stop(
      "No FFIEC bulk zip files found in:\n  ",
      raw_dir,
      call. = FALSE
    )
  }

  mmddyyyy <- stringr::str_extract(basename(files), "\\d{8}")

  if (anyNA(mmddyyyy)) {
    stop(
      "Could not parse MMDDYYYY date from filenames:\n",
      paste0("  - ", basename(files[is.na(mmddyyyy)]), collapse = "\n"),
      call. = FALSE
    )
  }

  date <- as.Date(mmddyyyy, format = "%m%d%Y")

  if (anyNA(date)) {
    stop(
      "Parsed date tokens but as.Date() failed for:\n",
      paste0("  - ", basename(files[is.na(date)]), collapse = "\n"),
      call. = FALSE
    )
  }

  tibble::tibble(
    zipfile = normalizePath(files, mustWork = FALSE),
    date    = date
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
