#' List FFIEC bulk zip files
#'
#' Scans a directory for FFIEC Call Report bulk zip files and returns
#' a tibble with zipfile paths and report dates parsed from the filenames.
#'
#' This function is typically used to discover bulk zip files prior to
#' calling [ffiec_process()].
#'
#' @param raw_data_dir Optional parent directory containing FFIEC bulk
#'   zip files. If provided and \code{schema} is not \code{NULL}, files are
#'   expected under \code{file.path(raw_data_dir, schema)}. If \code{NULL},
#'   the environment variable \code{RAW_DATA_DIR} is used.
#' @param schema Schema name used to resolve the input directory
#'   (default \code{"ffiec"}). Set to \code{NULL} to use the resolved
#'   directory directly without appending a schema subdirectory.
#' @param type Character scalar indicating the bulk file type to list.
#'   Supported values are:
#'   \describe{
#'     \item{\code{"tsv"}}{Tab-delimited Call Report bulk files
#'       (filenames of the form
#'       \code{"FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip"}).}
#'     \item{\code{"xbrl"}}{XBRL Call Report bulk files
#'       (filenames of the form
#'       \code{"FFIEC CDR Call Bulk XBRL MMDDYYYY.zip"}).}
#'   }
#'
#' @details
#' Filenames are expected to contain an 8-digit \code{MMDDYYYY} date token,
#' which is parsed and returned as a \code{Date}. An error is raised if any
#' matching file does not conform to this naming convention.
#'
#' @return A tibble with columns:
#' \describe{
#'   \item{zipfile}{Full path to the bulk zip file.}
#'   \item{date}{Report date as a \code{Date}.}
#' }
#'
#' @examples
#' \dontrun{
#' # List TSV bulk zip files using RAW_DATA_DIR/ffiec
#' ffiec_list_zips()
#'
#' # List XBRL bulk zip files
#' ffiec_list_zips(type = "xbrl")
#'
#' # List bulk zip files from an explicit directory (no schema subdirectory)
#' ffiec_list_zips(raw_data_dir = "/data/raw/ffiec", schema = NULL)
#' }
#'
#' @export
ffiec_list_zips <- function(raw_data_dir = NULL,
                            schema = "ffiec",
                            type = "tsv") {

  in_dir <- resolve_in_dir(raw_data_dir = raw_data_dir, schema = schema)

  if (is.null(in_dir) || !nzchar(in_dir)) {
    stop("Provide `raw_data_dir` or set `RAW_DATA_DIR`.", call. = FALSE)
  }

  in_dir <- normalizePath(in_dir, mustWork = FALSE)

  pattern <- switch(
    type,
    tsv  = "^FFIEC CDR Call Bulk All Schedules \\d{8}\\.zip$",
    xbrl = "^FFIEC CDR Call Bulk XBRL \\d{8}\\.zip$",
    stop("Unknown type: ", type, call. = FALSE)
  )

  files <- list.files(
    in_dir,
    pattern = pattern,
    full.names = TRUE
  )

  if (length(files) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  # Extract 8-digit token; keep only those that look like MMDDYYYY
  mmddyyyy <- stringr::str_extract(basename(files), "\\d{8}")
  keep_tok <- !is.na(mmddyyyy)

  if (!any(keep_tok)) {
    # No matching bulk zips found; return empty tibble rather than error
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  files2 <- files[keep_tok]
  tok2   <- mmddyyyy[keep_tok]

  date <- as.Date(tok2, format = "%m%d%Y")
  keep_date <- !is.na(date)

  # Drop files with unparseable date tokens (e.g., 13322025)
  files3 <- files2[keep_date]
  date3  <- date[keep_date]

  if (length(files3) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  tibble::tibble(
    zipfile = normalizePath(files3, mustWork = FALSE),
    date = date3
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
#' Parquet files. Files may be selected either by FFIEC schedule name
#' or by an explicit Parquet filename or glob pattern. No data is read
#' eagerly; evaluation occurs only when the result is collected.
#'
#' This function is intended for use with Parquet files produced by
#' [ffiec_process()]. It validates that the requested files exist on
#' disk before constructing the DuckDB query.
#'
#' @param conn A valid DuckDB connection.
#' @param schedule Optional character scalar giving the FFIEC schedule
#'   name (e.g. \code{"rc"}, \code{"rci"}, \code{"rcb"}). All matching Parquet
#'   files of the form \code{\{prefix\}\{schedule\}_YYYYMMDD.parquet} are scanned.
#' @param pq_file Optional character scalar giving a Parquet filename or
#'   glob pattern. May be a base name or relative path within the resolved
#'   Parquet directory, or a fully qualified path when \code{data_dir} is
#'   not supplied.
#' @param data_dir Optional parent directory for Parquet files. If \code{NULL},
#'   the environment variable \code{DATA_DIR} is used.
#' @param schema Schema name used to resolve the Parquet directory
#'   (default \code{"ffiec"}). Set to \code{NULL} to treat \code{data_dir}
#'   (or \code{DATA_DIR}) as the final Parquet directory.
#' @param prefix Optional filename prefix used when the Parquet files
#'   were created (default \code{""}).
#' @param union_by_name Logical; whether to union Parquet files by column
#'   name when scanning multiple files (passed to DuckDB's
#'   \code{read_parquet()}). Default is \code{TRUE}.
#' @param keep_filename Logical; whether to keep the original Parquet file name (passed to DuckDB's
#'   \code{read_parquet()}). Default is \code{FALSE}.
#'
#' @details
#' Exactly one of \code{schedule} or \code{pq_file} must be supplied.
#'
#' \strong{Directory resolution}
#'
#' By default, Parquet files are read from a schema subdirectory:
#' \code{file.path(data_dir, schema)}. If \code{data_dir} is \code{NULL},
#' \code{DATA_DIR} is used as the parent directory.
#'
#' If \code{schema = NULL}, no schema subdirectory is appended; in this case,
#' \code{data_dir} (or \code{DATA_DIR}) is treated as the final directory
#' containing Parquet files.
#'
#' When \code{schedule} is used, files are selected with the glob pattern
#' \code{\{prefix\}\{schedule\}_*.parquet} in the resolved directory.
#'
#' When \code{pq_file} is used, the value is treated as a filename or glob
#' pattern relative to the resolved directory. If neither \code{data_dir} nor
#' \code{DATA_DIR} is available, \code{pq_file} may instead be a fully qualified
#' path to an existing Parquet file.
#'
#' A fast filesystem check is performed using \code{Sys.glob()}. If no
#' matching files are found, the function errors before issuing any DuckDB query.
#'
#' @return A lazy \code{tbl} backed by DuckDB.
#' @export
ffiec_scan_pqs <- function(conn,
                           schedule = NULL,
                           pq_file = NULL,
                           data_dir = NULL,
                           schema = "ffiec",
                           prefix = "",
                           union_by_name = TRUE,
                           keep_filename = FALSE) {

  stopifnot(DBI::dbIsValid(conn))

  # exactly one of schedule / pq_file
  n_specified <- sum(!is.null(schedule), !is.null(pq_file))
  if (n_specified != 1L) {
    stop("Provide exactly one of `schedule` or `pq_file`.", call. = FALSE)
  }

  pq_path <- resolve_out_dir(data_dir = data_dir, schema = schema)
  if (!is.null(pq_path)) {
    pq_path <- normalizePath(pq_path, mustWork = FALSE)
  }

  if (!is.null(schedule)) {
    # schedule mode requires a directory
    if (is.null(pq_path) || !nzchar(pq_path)) {
      stop("Provide `data_dir` or set `DATA_DIR`.", call. = FALSE)
    }

    glob <- file.path(pq_path, sprintf("%s%s_*.parquet", prefix, schedule))
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

    if (union_by_name) {
      union_by_name
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s,
                                 union_by_name=%s,
                                 filename=%s)",
      DBI::dbQuoteString(conn, glob),
      if (isTRUE(union_by_name)) "true" else "false",
      if (isTRUE(keep_filename)) "true" else "false"
    )

  } else {
    # pq_file mode: allow absolute path/glob if pq_path is NULL
    if (is.null(pq_path) || !nzchar(pq_path)) {
      glob <- pq_file
    } else {
      glob <- file.path(pq_path, pq_file)
    }

    matches <- Sys.glob(glob)
    if (length(matches) == 0L) {
      stop(sprintf("Parquet file not found:\n  %s", glob), call. = FALSE)
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s)",
      DBI::dbQuoteString(conn, glob)
    )
  }

  dplyr::tbl(conn, dbplyr::sql(sql))
}

#' Check primary-key and non-NULL constraints in FFIEC Parquet files
#'
#' Scans FFIEC Parquet files into DuckDB and verifies that a specified
#' set of columns jointly satisfies two integrity constraints:
#'
#' \itemize{
#'   \item No missing values (non-NULL constraint)
#'   \item Uniqueness across rows (primary-key constraint)
#' }
#'
#' The function operates lazily via DuckDB and only materializes rows
#' involved in violations. It is intended as a lightweight validation
#' tool for Parquet files produced by [ffiec_process()].
#'
#' @param conn A valid DuckDB connection.
#' @param schedule Optional character scalar giving the FFIEC schedule
#'   to check (e.g. \code{"rc"}, \code{"rci"}). Passed to
#'   [ffiec_scan_pqs()].
#' @param cols Character vector of column names that together define
#'   the primary key and must also be non-missing.
#' @param data_dir Optional parent directory containing FFIEC Parquet
#'   files. If \code{NULL}, the directory is resolved using the
#'   \code{DATA_DIR} environment variable.
#' @param schema Schema name used to resolve the Parquet directory
#'   (default \code{"ffiec"}). Set to \code{NULL} to treat \code{data_dir}
#'   as the final directory.
#' @param prefix Optional filename prefix used when the Parquet files
#'   were created (default \code{""}).
#'
#' @return A tibble with one row per checked schedule and columns:
#' \describe{
#'   \item{schedule}{FFIEC schedule identifier.}
#'   \item{ok}{Logical; \code{TRUE} if no violations were detected.}
#'   \item{null_violations}{A tibble listing columns and counts of
#'     missing values, or empty if none were found.}
#'   \item{pk_violations}{A tibble of duplicated key combinations,
#'     or empty if the key is unique.}
#' }
#'
#' @examples
#' \dontrun{
#' library(duckdb)
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Check IDRSSD-date uniqueness for the RC schedule
#' ffiec_check_pq_keys(
#'   conn = con,
#'   schedule = "rc",
#'   cols = c("IDRSSD", "date")
#' )
#'
#' # Check all schedules
#' schedules <- ffiec_list_pqs() |> dplyr::distinct(schedule) |> dplyr::pull()
#' results <- purrr::map_dfr(
#'   schedules,
#'   \(s) ffiec_check_pq_keys(con, s, c("IDRSSD", "date"))
#' )
#' }
#'
#' @export
ffiec_check_pq_keys <- function(conn,
                                schedule = NULL,
                                cols,
                                data_dir = NULL,
                                schema = "ffiec",
                                prefix = "") {

  df <- ffiec_scan_pqs(conn = conn,
                       schedule = schedule,
                       data_dir = data_dir,
                       schema = schema,
                       prefix = prefix,
                       union_by_name = TRUE)

  res <- check_pk_and_non_null(df, cols)

  tibble::tibble(
    schedule = schedule,
    ok   = res$ok,
    null_violations = list(res$null_violations),
    pk_violations   = list(res$pk_violations)
  )
}

#' @keywords internal
#' @noRd
check_pk_and_non_null <- function(df, cols) {
  stopifnot(is.character(cols))

  # ---- non-NULL check ----
  nulls <- df |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(cols),
        ~ sum(is.na(.x), na.rm = TRUE),
        .names = "{.col}"
      )
    ) |>
    tidyr::pivot_longer(
      dplyr::everything(),
      names_to = "column",
      values_to = "n_na"
    ) |>
    dplyr::filter(.data$n_na > 0L) |>
    dplyr::collect()

  # ---- primary key uniqueness check ----
  dupes <- df |>
    dplyr::count(dplyr::across(dplyr::all_of(cols))) |>
    dplyr::filter(.data$n > 1L) |>
    dplyr::collect()

  list(
    ok = nrow(nulls) == 0L && nrow(dupes) == 0L,
    null_violations = nulls,
    pk_violations   = dupes
  )
}
